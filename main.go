package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Databases map[string]struct {
		URL      string `yaml:"url"`
		MaxConns int    `yaml:"max_conns"`
		MinConns int    `yaml:"min_conns"`
	} `yaml:"databases"`

	Queries map[string]struct {
		DB       string `yaml:"db"`
		SQL      string `yaml:"sql"`
		Timeout  string `yaml:"timeout"`
		Interval string `yaml:"interval"`
	} `yaml:"queries"`
}

func main() {

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// ---------- Graceful shutdown ----------
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ---------- Load config ----------
	var cfg Config
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		logger.Error("failed to read config.yaml", "error", err)
		os.Exit(1)
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		logger.Error("failed to parse YAML", "error", err)
		os.Exit(1)
	}

	// ---------- Create pools ----------
	pools := make(map[string]*pgxpool.Pool)

	for name, dbCfg := range cfg.Databases {

		poolCfg, err := pgxpool.ParseConfig(dbCfg.URL)
		if err != nil {
			logger.Error("failed to parse db config", "db", name, "error", err)
			//os.Exit(1)
			continue
		}

		poolCfg.MaxConns = int32(dbCfg.MaxConns)
		poolCfg.MinConns = int32(dbCfg.MinConns)

		pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
		if err != nil {
			logger.Error("failed to create pool", "db", name, "error", err)
			continue
		}

		// Проверка подключения
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = pool.Ping(pingCtx)
		cancel()

		if err != nil {
			logger.Error("database ping failed", "db", name, "error", err)
			pool.Close()
			continue
		}

		pools[name] = pool
		logger.Info("connected to database", "db", name)
	}

	if len(pools) == 0 {
		logger.Error("no databases available, shutting down")
		os.Exit(1)
	}

	// ---------- Start query workers ----------
	var wg sync.WaitGroup

	for name, queryCfg := range cfg.Queries {
		wg.Add(1)
		go startQueryWorker(ctx, &wg, logger, name, queryCfg, pools)
	}

	// ---------- Wait shutdown signal ----------
	<-ctx.Done()
	logger.Info("shutdown signal received")

	// Ждём завершения всех воркеров
	wg.Wait()

	// Закрываем все пулы
	for name, pool := range pools {
		logger.Info("closing pool", "db", name)
		pool.Close()
	}

	logger.Info("application stopped gracefully")
}

func startQueryWorker(
	ctx context.Context,
	wg *sync.WaitGroup,
	logger *slog.Logger,
	name string,
	queryCfg struct {
		DB       string `yaml:"db"`
		SQL      string `yaml:"sql"`
		Timeout  string `yaml:"timeout"`
		Interval string `yaml:"interval"`
	},
	pools map[string]*pgxpool.Pool,
) {
	defer wg.Done()

	pool, ok := pools[queryCfg.DB]
	if !ok {
		logger.Error("database not found for query", "query", name, "db", queryCfg.DB)
		return
	}

	interval, err := time.ParseDuration(queryCfg.Interval)
	if err != nil {
		logger.Error("invalid interval", "query", name, "error", err)
		return
	}

	timeout, err := time.ParseDuration(queryCfg.Timeout)
	if err != nil {
		logger.Error("invalid timeout", "query", name, "error", err)
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	logger.Info("started query worker", "query", name)

	for {
		select {

		case <-ctx.Done():
			logger.Info("stopping query worker", "query", name)
			return

		case <-ticker.C:
			executeQuery(ctx, logger, name, queryCfg.SQL, timeout, pool)
		}
	}
}

func executeQuery(
	parentCtx context.Context,
	logger *slog.Logger,
	name string,
	sql string,
	timeout time.Duration,
	pool *pgxpool.Pool,
) {

	queryCtx, cancel := context.WithTimeout(parentCtx, timeout)
	defer cancel()

	start := time.Now()

	var result any
	err := pool.QueryRow(queryCtx, sql).Scan(&result)

	duration := time.Since(start)

	if err != nil {
		logger.Error("query failed",
			"query", name,
			"error", err,
			"duration", duration.String(),
		)
		return
	}

	logger.Info("query success",
		"query", name,
		"result", result,
		"duration", duration.String(),
	)
}
