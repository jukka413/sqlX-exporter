package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	appCtx, appCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer appCancel()

	a := &app{
		logger:     logger,
		ctx:        appCtx,
		cancel:     appCancel,
		configPath: "./config.yaml",
		workers:    make(map[string]*worker),
		pools:      make(map[string]*dbPool),
	}

	// ---- Metrics server ----
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		srv := &http.Server{
			Addr:              ":2112",
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		}
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
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

	a.logger.Info("shutting down workers")
	a.stopAllWorkers()

	a.logger.Info("closing pools")
	a.closeAllPools()

	a.logger.Info("application stopped gracefully")
}
