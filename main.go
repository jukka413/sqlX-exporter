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

	// /healthz — процесс жив и обрабатывает HTTP-запросы. Не связан с
	// состоянием конфига или БД — если процесс совсем завис, kubelet должен
	// узнать об этом именно отсюда.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// /readyz — хотя бы один reload успешно применил структурно валидный
	// конфиг. Намеренно не зависит от доступности отдельных БД — partial
	// outage одной БД штатно переживается (см. failedPools/poolHealthChecker)
	// и не должен выталкивать здоровый Pod из Service; недоступность БД
	// видна через свои собственные метрики (app_db_connection_errors_total,
	// app_query_up), а не через readiness.
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

	// listenErrCh получает ровно одно значение если ListenAndServe упал НЕ
	// из-за штатного Shutdown(). Раньше такая ошибка (например порт уже
	// занят) только логировалась — процесс продолжал жить, Pod оставался
	// Running, а /metrics был недоступен вообще без единого внешнего сигнала
	// о проблеме. Для exporter'а это фатальная ситуация: его единственная
	// работа — отдавать /metrics.
	listenErrCh := make(chan error, 1)
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			listenErrCh <- err
		}
	}()

	// ---- Pool metrics updater ----
	go a.pm.metricsUpdater(5 * time.Second)

	// ---- Pool health checker — переподключение к упавшим БД ----
	go a.poolHealthChecker()

	// First load — уже публикует директории для watcher через a.watchDirsCh
	// (см. app.reload). Отдельный сбор директорий здесь больше не нужен —
	// это единственный механизм и для первого запуска, и для всех
	// последующих hot-reload.
	a.reload()

	// FSNotify watcher. watcherDone закрывается когда watchConfig
	// действительно вернулся — то есть больше НИКОГДА не вызовет reload().
	// main дожидается этого перед stopAllWorkers/closeAllPools (см. ниже,
	// P1.21): без этого shutdown мог начать разбирать пулы/воркеры пока
	// watcher ещё выполняет (или вот-вот начнёт) reload(), обращающийся к
	// уже частично снесённому состоянию.
	var watcherWG sync.WaitGroup
	watcherWG.Add(1)
	go func() {
		defer watcherWG.Done()
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

	// Дожидаемся watcher'а ДО остановки воркеров и закрытия пулов — гарантия
	// что ни один reload() не выполняется и не может начаться параллельно
	// с teardown.
	watcherWG.Wait()

	logger.Info("shutting down workers")
	a.stopAllWorkers()

	logger.Info("closing pools")
	a.closeAllPools()

	// Graceful shutdown metrics server
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("metrics server shutdown error", "error", err)
	}

	logger.Info("application stopped gracefully")
	os.Exit(exitCode)
}
