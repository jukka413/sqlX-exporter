package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func startQueryWorker(
	ctx context.Context,
	wg *sync.WaitGroup,
	logger *slog.Logger,
	name string,
	queryCfg QueryConfig,
	pool *pgxpool.Pool,
) {
	defer wg.Done()

	timeout, err := time.ParseDuration(queryCfg.Timeout)
	if err != nil {
		logger.Error("invalid timeout, worker stopped", "query", name, "error", err)
		return
	}

	// --- Scheduled mode (if schedule is provided) ---
	if queryCfg.Schedule != nil {
		loc, entries, err := parseSchedule(queryCfg.Schedule)
		if err != nil {
			logger.Error("invalid schedule, worker stopped", "query", name, "error", err)
			return
		}

		logger.Info("started scheduled query worker", "query", name)

		for {
			nr := nextRun(time.Now(), loc, entries)
			wait := time.Until(nr)

			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				logger.Info("stopping query worker", "query", name)
				return
			case <-timer.C:
				runOnce(ctx, logger, name, queryCfg, pool, timeout)
			}
		}
	}

	// --- Interval mode (existing behaviour) ---
	interval, err := time.ParseDuration(queryCfg.Interval)
	if err != nil {
		logger.Error("invalid interval, worker stopped", "query", name, "error", err)
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	logger.Info("started interval query worker", "query", name)

	for {
		select {
		case <-ctx.Done():
			logger.Info("stopping query worker", "query", name)
			return
		case <-ticker.C:
			runOnce(ctx, logger, name, queryCfg, pool, timeout)
		}
	}
}

func runOnce(
	ctx context.Context,
	logger *slog.Logger,
	name string,
	queryCfg QueryConfig,
	pool *pgxpool.Pool,
	timeout time.Duration,
) {
	start := time.Now()

	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	var result any
	err := pool.QueryRow(queryCtx, queryCfg.SQL).Scan(&result)
	cancel()

	duration := time.Since(start).Seconds()
	queryDuration.WithLabelValues(name, queryCfg.DB).Observe(duration)

	if err != nil {
		queryErrors.WithLabelValues(name, queryCfg.DB).Inc()
		logger.Error("query failed", "query", name, "error", err)
		return
	}

	value, ok := toFloat64(result)
	if !ok {
		logger.Error("query result is not numeric", "query", name)
		return
	}

	metric := getOrCreateQueryMetric(name)
	metric.WithLabelValues(queryCfg.DB).Set(value)

	logger.Info("query success",
		"query", name,
		"value", value,
		"duration", duration,
	)
}

func toFloat64(v any) (float64, bool) {
	switch t := v.(type) {
	case int:
		return float64(t), true
	case int32:
		return float64(t), true
	case int64:
		return float64(t), true
	case float32:
		return float64(t), true
	case float64:
		return t, true
	default:
		return 0, false
	}
}
