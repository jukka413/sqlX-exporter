package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
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
		logger:            logger,
		ctx:               appCtx,
		cancel:            appCancel,
		configPath:        *configPath,
		workers:           make(map[string]*worker),
		pools:             make(map[string]*dbPool),
		failedPools:       make(map[string]DBConfig),
		queriesCfg:        make(map[string]QueryConfig),
		reconnectInterval: 5 * time.Minute, // дефолт до первого reload
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

	// ---- Pool health checker — переподключение к упавшим БД ----
	go a.poolHealthChecker()

	// First load
	a.reload()

	// Собираем директории инклюд-файлов для watcher
	// После reload список инклюдов может измениться — watcher перезапустится
	// через горутину если список изменился (упрощённо: следим за всеми директориями
	// которые были в конфиге на старте; при добавлении нового include нужен рестарт).
	includeDirs := collectIncludeDirs(a.configPath)

	// FSNotify watcher
	go watchConfig(a.ctx, a.logger, a.configPath, a.reload, includeDirs...)

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

// collectIncludeDirs читает конфиг и возвращает директории всех инклюд-файлов.
// Используется при старте чтобы настроить watcher на все нужные директории.
func collectIncludeDirs(configPath string) []string {
	cfg, err := loadConfig(configPath)
	if err != nil || len(cfg.Includes) == 0 {
		return nil
	}
	baseDir := filepath.Dir(configPath)
	seen := map[string]struct{}{}
	var dirs []string
	for _, inc := range cfg.Includes {
		dir := filepath.Dir(filepath.Join(baseDir, inc))
		if _, ok := seen[dir]; !ok {
			seen[dir] = struct{}{}
			dirs = append(dirs, dir)
		}
	}
	return dirs
}
