package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	configPath := flag.String("config", "./config.yaml", "path to config file")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	appCtx, appCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer appCancel()

	a := &app{
		logger:     logger,
		ctx:        appCtx,
		cancel:     appCancel,
		configPath: *configPath,
		workers:    make(map[string]*worker),
		pools:      make(map[string]*dbPool),
	}

	// ---- Metrics server ----
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	metricsSrv := &http.Server{
		Addr:              ":2112",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server failed", "error", err)
		}
	}()

	// ---- Pool metrics updater ----
	go a.poolMetricsUpdater(5 * time.Second)

	// First load
	a.reload()

	// FSNotify watcher
	go watchConfig(a.ctx, a.logger, a.configPath, a.reload)

	<-a.ctx.Done()

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
}
