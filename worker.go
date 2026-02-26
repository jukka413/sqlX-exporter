package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
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
	defer cancel()

	var err error
	if queryCfg.ValueColumn != "" {
		err = runMultiRow(queryCtx, logger, name, queryCfg, db)
	} else {
		err = runSingleValue(queryCtx, logger, name, queryCfg, db)
	}

	duration := time.Since(start).Seconds()
	queryDuration.WithLabelValues(name, queryCfg.DB).Observe(duration)

	if err != nil {
		if ctx.Err() == context.Canceled {
			logger.Info("query cancelled by next tick", "query", name, "elapsed", duration)
			return
		}
		queryErrors.WithLabelValues(name, queryCfg.DB).Inc()
		logger.Error("query failed", "query", name, "error", err)
		return
	}

	logger.Info("query success", "query", name, "duration", duration)
}

// runSingleValue — старое поведение: одна строка, один числовой столбец.
func runSingleValue(
	ctx context.Context,
	logger *slog.Logger,
	name string,
	queryCfg QueryConfig,
	db *sql.DB,
) error {
	var result any
	if err := db.QueryRowContext(ctx, queryCfg.SQL).Scan(&result); err != nil {
		return err
	}

	value, ok := toFloat64(result)
	if !ok {
		logger.Error("query result is not numeric", "query", name)
		return nil
	}

	metric := getOrCreateQueryMetric(name, queryCfg.Labels, nil)
	metric.With(buildLabelValues(queryCfg.DB, queryCfg.Labels, nil)).Set(value)

	logger.Info("query value", "query", name, "value", value)
	return nil
}

// runMultiRow — новое поведение: несколько строк, value_column задаёт значение метрики,
// остальные столбцы становятся лейблами. Каждая строка — отдельная метрика с уникальным
// набором лейбл-значений.
func runMultiRow(
	ctx context.Context,
	logger *slog.Logger,
	name string,
	queryCfg QueryConfig,
	db *sql.DB,
) error {
	rows, err := db.QueryContext(ctx, queryCfg.SQL)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			logger.Warn("failed to close rows", "query", name, "error", cerr)
		}
	}()

	// Получаем имена столбцов из результата запроса
	colNames, err := rows.Columns()
	if err != nil {
		return err
	}

	// Проверяем что value_column присутствует в результате
	valueColIdx := -1
	for i, col := range colNames {
		if strings.EqualFold(col, queryCfg.ValueColumn) {
			valueColIdx = i
			break
		}
	}
	if valueColIdx == -1 {
		return fmt.Errorf("value_column %q not found in query result columns: %v",
			queryCfg.ValueColumn, colNames)
	}

	// Имена столбцов-лейблов = все столбцы кроме value_column
	colLabelNames := make([]string, 0, len(colNames)-1)
	for i, col := range colNames {
		if i != valueColIdx {
			colLabelNames = append(colLabelNames, col)
		}
	}

	// Создаём/получаем метрику с финальным набором лейблов.
	// Это нужно сделать до итерации по строкам, чтобы зафиксировать лейблы один раз.
	metric := getOrCreateQueryMetric(name, queryCfg.Labels, colLabelNames)

	// Буфер для сканирования строки — все столбцы как any
	scanBuf := make([]any, len(colNames))
	scanPtrs := make([]any, len(colNames))
	for i := range scanBuf {
		scanPtrs[i] = &scanBuf[i]
	}

	rowCount := 0
	for rows.Next() {
		if err := rows.Scan(scanPtrs...); err != nil {
			return err
		}

		// Извлекаем значение метрики
		value, ok := toFloat64(scanBuf[valueColIdx])
		if !ok {
			logger.Warn("skipping row: value column is not numeric",
				"query", name,
				"value_column", queryCfg.ValueColumn,
				"raw", scanBuf[valueColIdx],
			)
			continue
		}

		// Собираем значения столбцов-лейблов в map
		colLabelValues := make(map[string]string, len(colLabelNames))
		for i, col := range colNames {
			if i != valueColIdx {
				colLabelValues[col] = anyToString(scanBuf[i])
			}
		}

		metric.With(buildLabelValues(queryCfg.DB, queryCfg.Labels, colLabelValues)).Set(value)
		rowCount++
	}

	if err := rows.Err(); err != nil {
		return err
	}

	if rowCount == 0 {
		logger.Warn("query returned 0 rows", "query", name)
	}

	return nil
}

// anyToString конвертирует значение столбца в строку для использования как лейбл Prometheus.
func anyToString(v any) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case []byte:
		return string(t)
	case string:
		return t
	default:
		return fmt.Sprintf("%v", t)
	}
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
