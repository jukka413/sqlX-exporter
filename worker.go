package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func startQueryWorker(
	ctx context.Context,
	wg *sync.WaitGroup,
	logger *slog.Logger,
	name string,
	queryCfg QueryConfig,
	db *sql.DB,
	prevLabels *[]prometheus.Labels,
) {
	defer wg.Done()

	timeout, err := time.ParseDuration(queryCfg.Timeout)
	if err != nil {
		logger.Error("invalid timeout, worker stopped", "query", name, "error", err)
		return
	}

	runner := newSingleRunner(ctx, logger, name, prevLabels)
	defer runner.cancelCurrent()

	// --- Scheduled mode ---
	if queryCfg.Schedule != nil {
		loc, entries, err := parseSchedule(queryCfg.Schedule)
		if err != nil {
			logger.Error("invalid schedule, worker stopped", "query", name, "error", err)
			return
		}

		logger.Info("started scheduled query worker", "query", name)

		for {
			nr := nextRun(time.Now(), loc, entries)
			timer := time.NewTimer(time.Until(nr))
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

// =========================================================================
// singleRunner
// =========================================================================

// singleRunner гарантирует что в каждый момент времени выполняется не более одного запроса.
// При вызове run() предыдущий запрос отменяется через context, новый запускается в горутине.
//
// Также хранит prevLabels — набор prometheus.Labels последнего успешного multi-row запуска.
// При следующем запуске мы сравниваем его с текущим результатом и удаляем исчезнувшие строки.
type singleRunner struct {
	parentCtx     context.Context
	logger        *slog.Logger
	name          string
	cancelCurrent context.CancelFunc
	currentWg     sync.WaitGroup

	// prevLabels — указатель на срез лейблов последнего успешного multi-row запуска.
	// Указатель (а не значение) позволяет передавать состояние между воркерами при hot-reload:
	// старый воркер и новый смотрят на одну и ту же память.
	// Доступ безопасен: cancelCurrent+Wait гарантируют что старая горутина завершилась
	// до того как новая начнёт читать/писать prevLabels.
	prevLabels *[]prometheus.Labels
}

func newSingleRunner(parentCtx context.Context, logger *slog.Logger, name string, prevLabels *[]prometheus.Labels) *singleRunner {
	return &singleRunner{
		parentCtx:     parentCtx,
		logger:        logger,
		name:          name,
		cancelCurrent: func() {},
		prevLabels:    prevLabels,
	}
}

func (r *singleRunner) run(queryCfg QueryConfig, db *sql.DB, timeout time.Duration) {
	r.cancelCurrent()
	r.currentWg.Wait()

	runCtx, cancel := context.WithCancel(r.parentCtx)
	r.cancelCurrent = cancel

	r.currentWg.Add(1)
	go func() {
		defer r.currentWg.Done()
		if runCtx.Err() != nil {
			return
		}
		updated := runOnce(runCtx, r.logger, r.name, queryCfg, db, timeout, *r.prevLabels)
		*r.prevLabels = updated
	}()
}

// =========================================================================
// runOnce
// =========================================================================

// runOnce выполняет один запуск запроса и возвращает обновлённый срез prevLabels
// (для multi-row) или nil (для single-value).
// prevLabels используется для reconciliation — удаления метрик исчезнувших строк.
func runOnce(
	ctx context.Context,
	logger *slog.Logger,
	name string,
	queryCfg QueryConfig,
	db *sql.DB,
	timeout time.Duration,
	prevLabels []prometheus.Labels,
) (nextPrevLabels []prometheus.Labels) {
	start := time.Now()

	// metricName — имя метрики в Prometheus.
	// Для запросов клонированных под несколько БД MetricName содержит
	// оригинальное имя без суффикса __db, чтобы метрики были чистыми.
	// name (ключ воркера) может содержать суффикс для уникальности.
	metricName := queryCfg.MetricName
	if metricName == "" {
		metricName = name // фолбэк для обратной совместимости
	}

	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var runErr error
	if queryCfg.ValueColumn != "" {
		nextPrevLabels, runErr = runMultiRow(queryCtx, logger, metricName, queryCfg, db, prevLabels)
	} else {
		runErr = runSingleValue(queryCtx, logger, metricName, queryCfg, db)
	}

	duration := time.Since(start).Seconds()
	queryDuration.WithLabelValues(metricName, queryCfg.DB).Observe(duration)

	if runErr != nil {
		reason := classifyError(ctx, queryCtx)

		if reason == "cancelled" {
			logger.Info("query cancelled by next tick", "query", name, "elapsed", duration)
			return prevLabels
		}

		queryErrors.WithLabelValues(metricName, queryCfg.DB, reason).Inc()
		queryUp.WithLabelValues(metricName, queryCfg.DB).Set(0)
		logger.Error("query failed", "query", name, "reason", reason, "error", runErr)

		metric, exists := lookupQueryMetric(metricName)
		if exists {
			for _, lbl := range prevLabels {
				metric.Delete(lbl)
			}
		}
		return nil
	}

	queryUp.WithLabelValues(metricName, queryCfg.DB).Set(1)
	queryLastSuccess.WithLabelValues(metricName, queryCfg.DB).SetToCurrentTime()
	logger.Info("query success", "query", name, "duration", duration)

	return nextPrevLabels
}

// classifyError определяет причину ошибки для лейбла reason в queryErrors.
//
//   - "cancelled" — запрос отменён следующим тиком (ctx.Err() == Canceled)
//   - "timeout"   — превышен таймаут (queryCtx истёк: DeadlineExceeded)
//   - "db_error"  — ошибка на стороне БД
func classifyError(workerCtx, queryCtx context.Context) string {
	if workerCtx.Err() == context.Canceled {
		return "cancelled"
	}
	if queryCtx.Err() != nil || errors.Is(queryCtx.Err(), context.DeadlineExceeded) {
		return "timeout"
	}
	return "db_error"
}

// =========================================================================
// runSingleValue
// =========================================================================

// runSingleValue — оригинальное поведение: SELECT возвращает одну строку с одним числом.
// При ошибке удаляет метрику из Prometheus — она пропадёт с графиков в Grafana.
// При успехе — устанавливает значение обратно.
func runSingleValue(
	ctx context.Context,
	logger *slog.Logger,
	name string,
	queryCfg QueryConfig,
	db *sql.DB,
) error {
	var result any
	if err := db.QueryRowContext(ctx, queryCfg.SQL).Scan(&result); err != nil {
		// Удаляем метрику — данные устарели
		metric, exists := lookupQueryMetric(name)
		if exists {
			metric.Delete(buildLabelValues(queryCfg.DB, queryCfg.Labels, nil))
		}
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

// =========================================================================
// runMultiRow
// =========================================================================

// runMultiRow выполняет SELECT с несколькими строками. Каждая строка становится
// отдельным time series в Prometheus. Столбцы кроме value_column — лейблы.
//
// Reconciliation: сравниваем текущий набор лейблов с prevLabels.
// Строки, которые были в прошлом запуске но отсутствуют в текущем — удаляются из метрики.
// Это обеспечивает что пропавшие из БД строки пропадают и с графиков Grafana.
func runMultiRow(
	ctx context.Context,
	logger *slog.Logger,
	name string,
	queryCfg QueryConfig,
	db *sql.DB,
	prevLabels []prometheus.Labels,
) ([]prometheus.Labels, error) {
	rows, err := db.QueryContext(ctx, queryCfg.SQL)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			logger.Warn("failed to close rows", "query", name, "error", cerr)
		}
	}()

	colNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Находим индекс value_column
	valueColIdx := -1
	for i, col := range colNames {
		if strings.EqualFold(col, queryCfg.ValueColumn) {
			valueColIdx = i
			break
		}
	}
	if valueColIdx == -1 {
		return nil, fmt.Errorf("value_column %q not found in result columns: %v",
			queryCfg.ValueColumn, colNames)
	}

	// Имена столбцов-лейблов = все столбцы кроме value_column
	colLabelNames := make([]string, 0, len(colNames)-1)
	for i, col := range colNames {
		if i != valueColIdx {
			colLabelNames = append(colLabelNames, col)
		}
	}

	metric := getOrCreateQueryMetric(name, queryCfg.Labels, colLabelNames)

	// Буфер сканирования
	scanBuf := make([]any, len(colNames))
	scanPtrs := make([]any, len(colNames))
	for i := range scanBuf {
		scanPtrs[i] = &scanBuf[i]
	}

	// currentLabels — набор лейблов текущего запуска, для reconciliation
	var currentLabels []prometheus.Labels

	for rows.Next() {
		if err := rows.Scan(scanPtrs...); err != nil {
			return nil, err
		}

		value, ok := toFloat64(scanBuf[valueColIdx])
		if !ok {
			logger.Warn("skipping row: value column is not numeric",
				"query", name,
				"value_column", queryCfg.ValueColumn,
				"raw", scanBuf[valueColIdx],
			)
			continue
		}

		colLabelValues := make(map[string]string, len(colLabelNames))
		for i, col := range colNames {
			if i != valueColIdx {
				colLabelValues[col] = anyToString(scanBuf[i])
			}
		}

		lbls := buildLabelValues(queryCfg.DB, queryCfg.Labels, colLabelValues)
		metric.With(lbls).Set(value)
		currentLabels = append(currentLabels, lbls)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(currentLabels) == 0 {
		logger.Warn("query returned 0 rows", "query", name)
	}

	// Reconciliation: удаляем строки которые были в прошлом запуске но исчезли сейчас.
	// Строим set текущих лейблов для быстрого поиска.
	currentSet := make(map[string]struct{}, len(currentLabels))
	for _, lbl := range currentLabels {
		currentSet[labelsKey(lbl)] = struct{}{}
	}
	for _, lbl := range prevLabels {
		if _, exists := currentSet[labelsKey(lbl)]; !exists {
			metric.Delete(lbl)
			logger.Info("removed stale metric row", "query", name, "labels", lbl)
		}
	}

	return currentLabels, nil
}

// labelsKey строит стабильный строковый ключ из prometheus.Labels.
// Ключи сортируются явно — порядок итерации по map в Go не гарантирован,
// и fmt.Sprintf("%v", map) не обеспечивает стабильности между вызовами.
func labelsKey(lbl prometheus.Labels) string {
	keys := make([]string, 0, len(lbl))
	for k := range lbl {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(lbl[k])
	}
	return sb.String()
}

// =========================================================================
// helpers
// =========================================================================

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
