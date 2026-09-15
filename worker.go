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

	metricName := queryCfg.MetricName
	if metricName == "" {
		metricName = name
	}

	timeout, err := time.ParseDuration(queryCfg.Timeout)
	if err != nil {
		logger.Error("invalid timeout, worker stopped", "query", metricName, "db", queryCfg.DB, "error", err)
		return
	}

	if queryCfg.Schedule != nil {
		loc, entries, err := parseSchedule(queryCfg.Schedule)
		if err != nil {
			logger.Error("invalid schedule, worker stopped", "query", metricName, "db", queryCfg.DB, "error", err)
			return
		}

		logger.Info("started scheduled query worker", "query", metricName, "db", queryCfg.DB)

		for {
			nr := nextRun(time.Now(), loc, entries)
			timer := time.NewTimer(time.Until(nr))
			select {
			case <-ctx.Done():
				timer.Stop()
				logger.Info("stopping query worker", "query", metricName, "db", queryCfg.DB)
				return
			case <-timer.C:
				*prevLabels = runOnce(ctx, logger, name, queryCfg, db, timeout, *prevLabels)
			}
		}
	}

	interval, err := time.ParseDuration(queryCfg.Interval)
	if err != nil {
		logger.Error("invalid interval, worker stopped", "query", metricName, "db", queryCfg.DB, "error", err)
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	logger.Info("started interval query worker", "query", metricName, "db", queryCfg.DB)

	// Первый запуск сразу, не дожидаясь тика — иначе /metrics пуст до
	// interval секунд после каждого старта/рестарта воркера.
	runIntervalTick(ctx, logger, name, metricName, queryCfg, db, timeout, interval, prevLabels)

	for {
		select {
		case <-ctx.Done():
			logger.Info("stopping query worker", "query", metricName, "db", queryCfg.DB)
			return
		case <-ticker.C:
			// Синхронно, в этой же горутине — пока runOnce занят, select не
			// читает ticker.C, и Go сам отбрасывает пропущенные тики. Даёт
			// "пропустить, если предыдущий ещё выполняется" бесплатно.
			runIntervalTick(ctx, logger, name, metricName, queryCfg, db, timeout, interval, prevLabels)
		}
	}
}

// runIntervalTick выполняет один запуск interval-запроса и предупреждает,
// если он не уложился в свой же interval — следующие тики в это время
// молча пропускались (см. комментарий в startQueryWorker).
func runIntervalTick(
	ctx context.Context,
	logger *slog.Logger,
	name, metricName string,
	queryCfg QueryConfig,
	db *sql.DB,
	timeout, interval time.Duration,
	prevLabels *[]prometheus.Labels,
) {
	start := time.Now()
	*prevLabels = runOnce(ctx, logger, name, queryCfg, db, timeout, *prevLabels)
	if elapsed := time.Since(start); elapsed > interval {
		logger.Warn("query took longer than its own interval — some ticks were skipped while it was running",
			"query", metricName, "db", queryCfg.DB, "elapsed", elapsed, "interval", interval)
	}
}

// runOnce выполняет один запуск и возвращает обновлённый prevLabels
// (multi-row) или nil (single-value) для reconciliation.
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

	metricName := queryCfg.MetricName
	if metricName == "" {
		metricName = name
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
		reason := classifyError(ctx, queryCtx, runErr)

		if reason == "cancelled" {
			logger.Info("query cancelled — worker is stopping (shutdown, reload, or query removed)",
				"query", metricName, "db", queryCfg.DB, "elapsed", duration)
			return prevLabels
		}

		queryErrors.WithLabelValues(metricName, queryCfg.DB, reason).Inc()
		queryUp.WithLabelValues(metricName, queryCfg.DB).Set(0)
		logger.Error("query failed", "query", metricName, "db", queryCfg.DB, "reason", reason, "error", runErr)

		// Только для schema_mismatch — при других reason мы не добрались до
		// проверки схемы вообще, и ничего нового про неё не знаем, поэтому
		// гейдж не трогаем ни в какую сторону.
		if reason == "schema_mismatch" {
			runtimeSchemaMismatch.WithLabelValues(metricName, queryCfg.DB).Set(1)
		}

		if metric, exists := lookupQueryMetric(metricName); exists {
			for _, lbl := range prevLabels {
				metric.Delete(lbl)
			}
		}
		return nil
	}

	queryUp.WithLabelValues(metricName, queryCfg.DB).Set(1)
	queryLastSuccess.WithLabelValues(metricName, queryCfg.DB).SetToCurrentTime()
	runtimeSchemaMismatch.DeleteLabelValues(metricName, queryCfg.DB)
	logger.Info("query success", "query", metricName, "db", queryCfg.DB, "duration", duration)

	return nextPrevLabels
}

// errSchemaMismatch — сигнальная ошибка для label set mismatch (runtime-схема
// SQL разошлась с уже зарегистрированным GaugeVec). Отдельная категория в
// classifyError — иначе она неотличима на дашборде от обычной ошибки БД,
// хотя причина и способ починки совершенно другие (нужно поправить SQL/
// схему, а не чинить сетевой доступ до БД).
var errSchemaMismatch = errors.New("label set mismatch")

// classifyError: "cancelled" (воркер останавливается — shutdown/reload/
// удаление запроса, не наложение тиков — см. startQueryWorker, оно теперь
// просто пропускает тик, если предыдущий ещё выполняется) | "timeout"
// (queryCtx истёк) | "schema_mismatch" (runtime-колонки SQL не совпали с уже
// зарегистрированной метрикой) | "db_error" (остальное).
func classifyError(workerCtx, queryCtx context.Context, runErr error) string {
	if workerCtx.Err() == context.Canceled {
		return "cancelled"
	}
	if errors.Is(queryCtx.Err(), context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(runErr, errSchemaMismatch) {
		return "schema_mismatch"
	}
	return "db_error"
}

// runSingleValue: SELECT возвращает одну строку с одним числом.
func runSingleValue(
	ctx context.Context,
	logger *slog.Logger,
	name string,
	queryCfg QueryConfig,
	db *sql.DB,
) error {
	var result any
	if err := db.QueryRowContext(ctx, queryCfg.SQL).Scan(&result); err != nil {
		if metric, exists := lookupQueryMetric(name); exists {
			metric.Delete(buildLabelValues(queryCfg.DB, queryCfg.DBEnv, queryCfg.Labels, nil))
		}
		return err
	}

	value, ok := toFloat64(result)
	if !ok {
		// Удаляем строку и здесь тоже — иначе app_query_up=0, а бизнес-метрика
		// молча остаётся со старым значением (расхождение с путём ошибки Scan()).
		if metric, exists := lookupQueryMetric(name); exists {
			metric.Delete(buildLabelValues(queryCfg.DB, queryCfg.DBEnv, queryCfg.Labels, nil))
		}
		return fmt.Errorf("query result is not numeric: %T", result)
	}

	metric, err := getOrCreateQueryMetric(name, queryCfg.Labels, nil)
	if err != nil {
		if existing, exists := lookupQueryMetric(name); exists {
			existing.Delete(buildLabelValues(queryCfg.DB, queryCfg.DBEnv, queryCfg.Labels, nil))
		}
		return err
	}
	// GetMetricWith, не With: With паникует при несовпадении лейблов со
	// схемой коллектора (возможно если getOrCreateQueryMetric вернул уже
	// существующий, зафиксированный ранее коллектор).
	gauge, err := metric.GetMetricWith(buildLabelValues(queryCfg.DB, queryCfg.DBEnv, queryCfg.Labels, nil))
	if err != nil {
		metric.Delete(buildLabelValues(queryCfg.DB, queryCfg.DBEnv, queryCfg.Labels, nil))
		return fmt.Errorf("metric %q: %w: %w", name, errSchemaMismatch, err)
	}
	gauge.Set(value)

	logger.Info("query value", "query", name, "db", queryCfg.DB, "value", value)
	return nil
}

// defaultMaxRows — лимит строк multi-row запроса, если max_rows не задан.
// Без лимита SELECT без GROUP BY/LIMIT может вернуть миллионы time series
// и привести к OOM — GOGC/GOMEMLIMIT здесь не помогают, поскольку проблема
// не в поведении GC, а в количестве живых объектов.
const defaultMaxRows = 10000

// runMultiRow выполняет SELECT с несколькими строками — каждая становится
// отдельным time series. Столбцы кроме value_column — лейблы.
//
// Двухфазно: Read буферизует все строки в памяти (до max_rows), ничего не
// записывая в Prometheus; Commit пишет буфер только если ВЕСЬ результат
// прочитан успешно и в пределах лимита. Без этого разделения ошибка на
// середине результата (лимит, несовместимая схема) оставляла бы половину
// строк записанными, а половину нет.
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

	colLabelNames := make([]string, 0, len(colNames)-1)
	for i, col := range colNames {
		if i != valueColIdx {
			colLabelNames = append(colLabelNames, col)
		}
	}

	// Имена столбцов известны только после выполнения запроса, поэтому не
	// могут быть провалидированы на этапе конфига — проверяем здесь: и на
	// коллизию с зарезервированными/статическими лейблами, и на валидность
	// как имя лейбла Prometheus.
	seenLabelNames := make(map[string]struct{}, len(colLabelNames)+len(queryCfg.Labels)+2)
	seenLabelNames["db"] = struct{}{}
	seenLabelNames["env"] = struct{}{}
	for k := range queryCfg.Labels {
		seenLabelNames[k] = struct{}{}
	}
	for _, col := range colLabelNames {
		if _, collision := seenLabelNames[col]; collision {
			return nil, fmt.Errorf("column %q collides with a reserved or static label name", col)
		}
		seenLabelNames[col] = struct{}{}
		if !isValidPrometheusLabelName(col) {
			return nil, fmt.Errorf("column %q is not a valid Prometheus label name", col)
		}
	}

	maxRows := queryCfg.MaxRows
	if maxRows <= 0 {
		maxRows = defaultMaxRows
	}

	// --- Read ---

	scanBuf := make([]any, len(colNames))
	scanPtrs := make([]any, len(colNames))
	for i := range scanBuf {
		scanPtrs[i] = &scanBuf[i]
	}

	type bufferedRow struct {
		labels prometheus.Labels
		value  float64
	}
	buffered := make([]bufferedRow, 0, 64)

	// seenSeries детектит дубликаты комбинаций лейблов внутри одного
	// результата (SQL без корректного GROUP BY) — без этого последняя
	// строка молча побеждала бы, а исход зависел от недетерминированного
	// порядка возврата строк БД.
	seenSeries := make(map[string]struct{}, 64)

	// rowsRead считает каждую прочитанную строку, не только успешно
	// распарсенные в число — иначе лимит обходился бы запросом с миллионами
	// нечисловых строк (len(buffered) остался бы нулевым).
	rowsRead := 0

	for rows.Next() {
		rowsRead++
		if rowsRead > maxRows {
			return nil, fmt.Errorf(
				"query returned more than max_rows=%d rows — aborting to avoid unbounded metric cardinality; "+
					"add GROUP BY/LIMIT to the SQL or raise max_rows explicitly if this is expected",
				maxRows)
		}

		if err := rows.Scan(scanPtrs...); err != nil {
			return nil, err
		}

		value, ok := toFloat64(scanBuf[valueColIdx])
		if !ok {
			logger.Warn("skipping row: value column is not numeric",
				"query", name,
				"db", queryCfg.DB,
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

		lbls := buildLabelValues(queryCfg.DB, queryCfg.DBEnv, queryCfg.Labels, colLabelValues)

		key := labelsKey(lbls)
		if _, dup := seenSeries[key]; dup {
			return nil, fmt.Errorf(
				"query returned duplicate label set %v — check the SQL's GROUP BY, "+
					"two rows should not produce the same combination of label values", lbls)
		}
		seenSeries[key] = struct{}{}

		buffered = append(buffered, bufferedRow{labels: lbls, value: value})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(buffered) == 0 {
		logger.Warn("query returned 0 rows", "query", name, "db", queryCfg.DB)
	}

	// --- Commit ---

	metric, err := getOrCreateQueryMetric(name, queryCfg.Labels, colLabelNames)
	if err != nil {
		return nil, err
	}

	// Сначала резолвим все gauge-хендлы, и только если ВСЕ успешны — Set().
	// Иначе ошибка на середине (несовместимая схема) оставила бы часть
	// строк записанными.
	gauges := make([]prometheus.Gauge, len(buffered))
	for i, row := range buffered {
		g, err := metric.GetMetricWith(row.labels)
		if err != nil {
			return nil, fmt.Errorf("metric %q: %w: %w", name, errSchemaMismatch, err)
		}
		gauges[i] = g
	}
	for i, row := range buffered {
		gauges[i].Set(row.value)
	}

	currentLabels := make([]prometheus.Labels, len(buffered))
	for i, row := range buffered {
		currentLabels[i] = row.labels
	}

	// Удаляем строки прошлого запуска, отсутствующие в текущем. seenSeries
	// уже содержит нужные ключи — buffered без дублей по построению.
	for _, lbl := range prevLabels {
		if _, exists := seenSeries[labelsKey(lbl)]; !exists {
			metric.Delete(lbl)
			logger.Info("removed stale metric row", "query", name, "db", queryCfg.DB, "labels", lbl)
		}
	}

	return currentLabels, nil
}

// labelsKey строит стабильный ключ из Labels (сортировка — порядок
// итерации по map не гарантирован).
//
// Length-prefixed кодирование ("<длина>:<байты>"), не разделители типа
// "key=value,key=value": значение лейбла само может содержать "," или "="
// (например JSON), и тогда разные комбинации лейблов сериализовались бы
// в одну строку. Явная длина делает коллизию невозможной, а не маловероятной.
func labelsKey(lbl prometheus.Labels) string {
	keys := make([]string, 0, len(lbl))
	for k := range lbl {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	for _, k := range keys {
		v := lbl[k]
		sb.WriteString(strconv.Itoa(len(k)))
		sb.WriteByte(':')
		sb.WriteString(k)
		sb.WriteString(strconv.Itoa(len(v)))
		sb.WriteByte(':')
		sb.WriteString(v)
	}
	return sb.String()
}

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
