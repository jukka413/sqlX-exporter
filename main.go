package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	// ---------- Logger ----------
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// ---------- Config ----------
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		logger.Error("DATABASE_URL is not set")
		os.Exit(1)
	}

	// ---------- Graceful shutdown ----------
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ---------- Pool config ----------
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		logger.Error("failed to parse database config", "error", err)
		os.Exit(1)
	}

	// Пул соединений (адекватные значения)
	cfg.MaxConns = 10
	cfg.MinConns = 2
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = 30 * time.Minute
	cfg.HealthCheckPeriod = time.Minute

	// ---------- Connect ----------
	dbpool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		logger.Error("failed to create connection pool", "error", err)
		os.Exit(1)
	}
	defer dbpool.Close()

	// Проверим соединение
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := dbpool.Ping(pingCtx); err != nil {
		logger.Error("database ping failed", "error", err)
		os.Exit(1)
	}

	logger.Info("database connected")

	// ---------- Query ----------
	queryCtx, cancelQuery := context.WithTimeout(ctx, 5*time.Second)
	defer cancelQuery()

	var name string
	err = dbpool.QueryRow(queryCtx,
		`SELECT 11`,
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
