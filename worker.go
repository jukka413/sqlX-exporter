package main

import (
	"context"
	"database/sql"
	"log/slog"
	"strconv"
	"sync"
	"time"
)

func startQueryWorker(
	ctx context.Context,
	wg *sync.WaitGroup,
	logger *slog.Logger,
	name string,
	queryCfg QueryConfig,
	db *sql.DB,
) {
	defer wg.Done()

	timeout, err := time.ParseDuration(queryCfg.Timeout)
	if err != nil {
		logger.Error("invalid timeout, worker stopped", "query", name, "error", err)
		return
	}

	// --- Scheduled mode ---
	if queryCfg.Schedule != nil {
		loc, entries, err := parseSchedule(queryCfg.Schedule)
		if err != nil {
			logger.Error("invalid schedule, worker stopped", "query", name, "error", err)
			return
		}

		logger.Info("started scheduled query worker", "query", name)

		runner := newSingleRunner(ctx, logger, name)
		defer runner.cancelCurrent()

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
				runner.run(queryCfg, db, timeout)
			}
		}
	}

	// --- Interval mode ---
	interval, err := time.ParseDuration(queryCfg.Interval)
	if err != nil {
		logger.Error("invalid interval, worker stopped", "query", name, "error", err)
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	logger.Info("started interval query worker", "query", name)

	runner := newSingleRunner(ctx, logger, name)
	defer runner.cancelCurrent()

	for {
		select {
		case <-ctx.Done():
			logger.Info("stopping query worker", "query", name)
			return
		case <-ticker.C:
			runner.run(queryCfg, db, timeout)
		}
	}
}

// singleRunner гарантирует, что в каждый момент времени выполняется не более одного запроса.
// При вызове run() предыдущий запущенный запрос немедленно отменяется через context,
// после чего запускается новый.
type singleRunner struct {
	parentCtx     context.Context
	logger        *slog.Logger
	name          string
	cancelCurrent context.CancelFunc
	currentWg     sync.WaitGroup
}

func newSingleRunner(parentCtx context.Context, logger *slog.Logger, name string) *singleRunner {
	return &singleRunner{
		parentCtx:     parentCtx,
		logger:        logger,
		name:          name,
		cancelCurrent: func() {}, // no-op пока не было первого запуска
	}
}

func (r *singleRunner) run(queryCfg QueryConfig, db *sql.DB, timeout time.Duration) {
	// Отменяем предыдущий запрос и ждём его завершения.
	// context.WithCancel гарантирует, что db.QueryRowContext вернёт управление
	// как только контекст будет отменён — даже если сервер БД ещё думает.
	r.cancelCurrent()
	r.currentWg.Wait()

	runCtx, cancel := context.WithCancel(r.parentCtx)
	r.cancelCurrent = cancel

	r.currentWg.Add(1)
	go func() {
		defer r.currentWg.Done()
		if runCtx.Err() != nil {
			// parentCtx уже отменён (приложение останавливается)
			return
		}
		runOnce(runCtx, r.logger, r.name, queryCfg, db, timeout)
	}()
}

// -----

func runOnce(
	ctx context.Context,
	logger *slog.Logger,
	name string,
	queryCfg QueryConfig,
	db *sql.DB,
	timeout time.Duration,
) {
	start := time.Now()

	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	var result any
	err := db.QueryRowContext(queryCtx, queryCfg.SQL).Scan(&result)
	cancel()

	duration := time.Since(start).Seconds()
	queryDuration.WithLabelValues(name, queryCfg.DB).Observe(duration)

	if err != nil {
		// Отличаем намеренную отмену (новый тик пришёл раньше) от реальной ошибки БД.
		// context.Canceled — это не ошибка, просто пришёл следующий тик.
		if ctx.Err() == context.Canceled {
			logger.Info("query cancelled by next tick", "query", name, "elapsed", duration)
			return
		}
		queryErrors.WithLabelValues(name, queryCfg.DB).Inc()
		logger.Error("query failed", "query", name, "error", err)
		return
	}

	value, ok := toFloat64(result)
	if !ok {
		logger.Error("query result is not numeric", "query", name)
		return
	}

	metric := getOrCreateQueryMetric(name, queryCfg.Labels)
	metric.With(buildLabelValues(queryCfg.DB, queryCfg.Labels)).Set(value)

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
	case []byte:
		f, err := strconv.ParseFloat(string(t), 64)
		if err != nil {
			return 0, false
		}
		return f, true
	case string:
		f, err := strconv.ParseFloat(t, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}
