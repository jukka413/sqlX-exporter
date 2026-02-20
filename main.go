package main

import (
	"context"
	"errors"
	//"io/ioutil"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Database struct {
		URL             string `yaml:"url"`
		MaxConns        int    `yaml:"max_conns"`
		MinConns        int    `yaml:"min_conns"`
		MaxConnLifetime string `yaml:"max_conn_lifetime"`
		MaxConnIdleTime string `yaml:"max_conn_idle_time"`
		HealthCheck     string `yaml:"health_check_period"`
	} `yaml:"database"`
	Query struct {
		SQL     string `yaml:"sql"`
		Timeout string `yaml:"timeout"`
	} `yaml:"query"`
}

func main() {
	// ---------- Logger ----------
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// ---------- Config ----------
	//databaseURL := os.Getenv("DATABASE_URL")
	//if databaseURL == "" {
	//	logger.Error("DATABASE_URL is not set")
	//	os.Exit(1)
	//}

	// ---------- Graceful shutdown ----------
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var cfgFile Config
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		logger.Error("failed to read config.yaml", "error", err)
		os.Exit(1)
	}

	if err := yaml.Unmarshal(data, &cfgFile); err != nil {
		logger.Error("failed to parse YAML", "error", err)
		os.Exit(1)
	}

	// Pool config
	poolCfg, err := pgxpool.ParseConfig(cfgFile.Database.URL)
	if err != nil {
		logger.Error("failed to parse database config", "error", err)
		os.Exit(1)
	}

	poolCfg.MaxConns = int32(cfgFile.Database.MaxConns)
	poolCfg.MinConns = int32(cfgFile.Database.MinConns)

	if d, err := time.ParseDuration(cfgFile.Database.MaxConnLifetime); err == nil {
		poolCfg.MaxConnLifetime = d
	}
	if d, err := time.ParseDuration(cfgFile.Database.MaxConnIdleTime); err == nil {
		poolCfg.MaxConnIdleTime = d
	}
	if d, err := time.ParseDuration(cfgFile.Database.HealthCheck); err == nil {
		poolCfg.HealthCheckPeriod = d
	}

	// Создание пула
	dbpool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		logger.Error("failed to create connection pool", "error", err)
		os.Exit(1)
	}
	defer dbpool.Close()

	// Таймаут для запроса
	queryTimeout, err := time.ParseDuration(cfgFile.Query.Timeout)
	if err != nil {
		logger.Error("invalid query timeout", "error", err)
		os.Exit(1)
	}

	// ---------- Pool config ----------
	//cfg, err := pgxpool.ParseConfig(databaseURL)
	//if err != nil {
	//	logger.Error("failed to parse database config", "error", err)
	//	os.Exit(1)
	//}
	//
	//// Пул соединений (адекватные значения)
	//cfg.MaxConns = 10
	//cfg.MinConns = 2
	//cfg.MaxConnLifetime = time.Hour
	//cfg.MaxConnIdleTime = 30 * time.Minute
	//cfg.HealthCheckPeriod = time.Minute

	// ---------- Connect ----------
	//dbpool, err := pgxpool.NewWithConfig(ctx, cfg)
	//if err != nil {
	//	logger.Error("failed to create connection pool", "error", err)
	//	os.Exit(1)
	//}
	//defer dbpool.Close()

	// Проверим соединение
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := dbpool.Ping(pingCtx); err != nil {
		logger.Error("database ping failed", "error", err)
		os.Exit(1)
	}

	logger.Info("database connected")

	// ---------- Query ----------
	queryCtx, cancelQuery := context.WithTimeout(ctx, queryTimeout)
	defer cancelQuery()

	var name string
	err = dbpool.QueryRow(queryCtx,
		cfgFile.Query.SQL,
	).Scan(&name)

	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			logger.Error("query timeout")
		} else {
			logger.Error("query failed", "error", err)
		}
		os.Exit(1)
	}

	logger.Info("query successful", "name", name)

	// Ожидание сигнала завершения
	<-ctx.Done()
	logger.Info("shutting down gracefully")
}
