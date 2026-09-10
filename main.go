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

	a := newApp(appCtx, logger, *configPath)

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

	// bgWG дожидается всех фоновых горутин перед teardown — иначе
	// stopAllWorkers/closeAllPools могли бы начаться пока poolHealthChecker
	// ещё в середине dial/reconcile.
	var bgWG sync.WaitGroup

	bgWG.Add(1)
	go func() {
		defer bgWG.Done()
		a.pm.metricsUpdater(5 * time.Second)
	}()

	// watchConfig запускается и дожидается установки watch ДО первого
	// reload — иначе reload() (может занимать секунды на несколько БД)
	// оставлял бы окно, в которое ConfigMap мог обновиться незамеченным:
	// fsnotify не воспроизводит события задним числом, и exporter молча
	// остался бы на старой версии конфига до следующего изменения.
	watcherReady := make(chan error, 1)
	bgWG.Add(1)
	go func() {
		defer bgWG.Done()
		watchConfig(a.ctx, a.logger, a.configPath, a.reload, a.watchDirsCh, watcherReady)
	}()
	if err := <-watcherReady; err != nil {
		logger.Error("failed to set up config watcher — hot-reload will not work, "+
			"continuing with a one-time config load", "error", err)
	}

	// poolHealthChecker стартует после первого reload — иначе его тикер
	// создавался бы с дефолтом 5 минут из newApp(), а не реальным
	// db_reconnect_interval из конфига.
	a.reload()

	bgWG.Add(1)
	go func() {
		defer bgWG.Done()
		a.poolHealthChecker()
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
