package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	configPath := flag.String("config", "./config.yaml", "path to config file")
	listenAddr := flag.String("listen-address", ":2112", "address for the metrics HTTP server to listen on")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	appCtx, appCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer appCancel()

	a := newApp(appCtx, appCancel, logger, *configPath)

	// ---- Metrics server ----
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	// /healthz — процесс жив. Не зависит от состояния конфига/БД.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// /readyz — хотя бы один reload применил структурно валидный конфиг.
	// Не зависит от доступности отдельных БД: partial outage переживается
	// штатно и не должен выталкивать здоровый Pod из Service — недоступность
	// БД видна через свои метрики (app_db_connection_errors_total, app_query_up).
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !a.isReady() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("config not yet applied"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	metricsSrv := &http.Server{
		Addr:              *listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20, // 1 MiB
	}

	// listenErrCh получает значение если ListenAndServe упал не из-за
	// штатного Shutdown() — для exporter'а это фатально, его единственная
	// работа отдавать /metrics.
	listenErrCh := make(chan error, 1)
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			listenErrCh <- err
		}
	}()

	// bgWG отслеживает ВСЕ фоновые горутины (не только watcher, как было
	// раньше) — иначе stopAllWorkers/closeAllPools могли начаться пока
	// poolHealthChecker ещё в середине dial/reconcile или metricsUpdater
	// ещё итерируется по пулам, которые вот-вот закроют. Обе горутины уже
	// корректно возвращаются по ctx.Done() — здесь только дожидаемся этого
	// перед тем как продолжить teardown.
	var bgWG sync.WaitGroup

	bgWG.Add(1)
	go func() {
		defer bgWG.Done()
		a.pm.metricsUpdater(5 * time.Second)
	}()

	// Первый reload уже публикует директории для watcher через a.watchDirsCh
	// и выставляет a.reconnectInterval из реального конфига. poolHealthChecker
	// стартует ПОСЛЕ этого, не до — иначе его тикер создавался бы с
	// захардкоженным дефолтом 5 минут из newApp(), и настоящее значение
	// db_reconnect_interval применилось бы только на первом естественном
	// тике старого тикера, каким бы коротким ни был интервал в конфиге.
	a.reload()

	bgWG.Add(1)
	go func() {
		defer bgWG.Done()
		a.poolHealthChecker()
	}()

	bgWG.Add(1)
	go func() {
		defer bgWG.Done()
		watchConfig(a.ctx, a.logger, a.configPath, a.reload, a.watchDirsCh)
	}()

	exitCode := 0

	select {
	case <-appCtx.Done():
		logger.Info("shutdown signal received")
	case err := <-listenErrCh:
		logger.Error("metrics server failed to listen, shutting down", "error", err)
		appCancel()
		exitCode = 1
	}

	bgWG.Wait()

	logger.Info("shutting down workers")
	a.stopAllWorkers()

	logger.Info("closing pools")
	a.closeAllPools()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("metrics server shutdown error", "error", err)
	}

	logger.Info("application stopped gracefully")
	os.Exit(exitCode)
}
