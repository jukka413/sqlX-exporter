package main

import (
	"errors"
	"sort"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	queryErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "app_query_errors_total",
			Help: "Total number of query execution errors",
		},
		// reason: "timeout" | "db_error" | "cancelled"
		[]string{"query", "db", "reason"},
	)

	// queryUp показывает успешность последнего выполнения запроса: 1 = успех, 0 = ошибка.
	// Удобно для алертинга: alert when queryUp == 0 for > N minutes.
	queryUp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "app_query_up",
			Help: "1 if last query execution was successful, 0 otherwise",
		},
		[]string{"query", "db"},
	)

	// queryLastSuccess — unix timestamp последнего успешного выполнения запроса.
	// Позволяет алертить на зависание: time() - app_query_last_success_timestamp_seconds > threshold.
	queryLastSuccess = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "app_query_last_success_timestamp_seconds",
			Help: "Unix timestamp of the last successful query execution",
		},
		[]string{"query", "db"},
	)

	dbConnectionErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "app_db_connection_errors_total",
			Help: "Total number of database connection errors",
		},
		[]string{"db"},
	)

	queryDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "app_query_duration_seconds",
			Help:    "Query execution duration",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"query", "db"},
	)

	dbPoolAcquired = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "app_db_pool_acquired_connections",
			Help: "Currently acquired connections",
		},
		[]string{"db"},
	)

	dbPoolIdle = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "app_db_pool_idle_connections",
			Help: "Idle connections",
		},
		[]string{"db"},
	)

	dbPoolTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "app_db_pool_total_connections",
			Help: "Total connections in pool",
		},
		[]string{"db"},
	)
)

func init() {
	prometheus.MustRegister(
		queryErrors,
		queryUp,
		queryLastSuccess,
		dbConnectionErrors,
		queryDuration,
		dbPoolAcquired,
		dbPoolIdle,
		dbPoolTotal,
	)

	// Примечание: GoCollector и ProcessCollector уже зарегистрированы автоматически
	// в DefaultRegisterer при импорте пакета prometheus. Явная регистрация не нужна
	// и вызовет панику "duplicate metrics collector registration attempted".
	// Метрики go_memstats_*, go_gc_*, process_* уже доступны на /metrics.
}

// ================= Custom metrics ==========

var (
	queryResultMetrics = make(map[string]*prometheus.GaugeVec)
	metricsMu          sync.Mutex
)

// getOrCreateQueryMetric безопасно возвращает или создаёт GaugeVec для данного запроса.
//
// Лейблы формируются из трёх источников (все опциональны):
//   - "db"          — всегда присутствует
//   - customLabels  — статические лейблы из поля labels: в конфиге
//   - colLabels     — динамические лейблы из имён столбцов SELECT (для value_column режима)
//
// Порядок фиксируется при первом создании метрики. При hot-reload возвращается
// существующий коллектор через AlreadyRegisteredError — паники нет.
// См.: https://pkg.go.dev/github.com/prometheus/client_golang/prometheus#AlreadyRegisteredError
func getOrCreateQueryMetric(queryName string, customLabels map[string]string, colLabels []string) *prometheus.GaugeVec {
	metricsMu.Lock()
	defer metricsMu.Unlock()

	if m, ok := queryResultMetrics[queryName]; ok {
		return m
	}

	labelNames := buildLabelNames(customLabels, colLabels)

	metric := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: queryName,
			Help: "SQL query result metric for " + queryName,
		},
		labelNames,
	)

	if err := prometheus.Register(metric); err != nil {
		are := &prometheus.AlreadyRegisteredError{}
		if errors.As(err, are) {
			existing, ok := are.ExistingCollector.(*prometheus.GaugeVec)
			if ok {
				queryResultMetrics[queryName] = existing
				return existing
			}
		}
		panic(err)
	}

	queryResultMetrics[queryName] = metric
	return metric
}

// buildLabelNames возвращает упорядоченный список имён лейблов:
//  1. "db" — всегда первый
//  2. "env" — всегда второй, значение из databases.<db>.env (пустая строка если
//     не задано). Присутствует всегда, а не только когда env реально указан —
//     иначе GaugeVec для одного и того же имени метрики мог бы получить разные
//     наборы лейблов при клонировании запроса на несколько БД (одна с env,
//     другая без), что приводит к панике при With().
//  3. статические лейблы из customLabels — в алфавитном порядке
//  4. динамические лейблы из colLabels (имена столбцов) — в исходном порядке столбцов
//
// Такой порядок гарантирует стабильность между вызовами — Prometheus требует
// чтобы имена и значения лейблов передавались в одном и том же порядке.
func buildLabelNames(customLabels map[string]string, colLabels []string) []string {
	names := make([]string, 0, 2+len(customLabels)+len(colLabels))
	names = append(names, "db", "env")

	// Статические лейблы из конфига — сортируем для детерминированности
	staticKeys := make([]string, 0, len(customLabels))
	for k := range customLabels {
		staticKeys = append(staticKeys, k)
	}
	sort.Strings(staticKeys)
	names = append(names, staticKeys...)

	// Динамические лейблы из столбцов — сохраняем порядок столбцов из SELECT
	names = append(names, colLabels...)

	return names
}

// buildLabelValues возвращает map лейблов со значениями для передачи в metric.With().
// envValue — значение databases.<db>.env для БД этого запроса, может быть "".
func buildLabelValues(dbName, envValue string, customLabels map[string]string, colLabelValues map[string]string) prometheus.Labels {
	labels := make(prometheus.Labels, 2+len(customLabels)+len(colLabelValues))
	labels["db"] = dbName
	labels["env"] = envValue
	for k, v := range customLabels {
		labels[k] = v
	}
	for k, v := range colLabelValues {
		labels[k] = v
	}
	return labels
}

// lookupQueryMetric возвращает уже зарегистрированную GaugeVec для данного запроса.
// Используется когда нужно удалить метрику (Delete) без её создания.
// Возвращает (nil, false) если метрика ещё не была создана.
func lookupQueryMetric(queryName string) (*prometheus.GaugeVec, bool) {
	metricsMu.Lock()
	defer metricsMu.Unlock()
	m, ok := queryResultMetrics[queryName]
	return m, ok
}

// unregisterQueryMetric полностью убирает метрику из Prometheus registry и
// из внутренней map queryResultMetrics.
//
// Без этого вызова удаление запроса из конфига (через hot-reload или
// sanitizeQueries) останавливает воркер, но саму метрику оставляет навечно
// зарегистрированной в Prometheus с последним известным значением —
// time series просто замораживается и никогда не пропадает с /metrics.
// При частых hot-reload (несколько values файлов в одном проекте, частые
// изменения списка запросов) это постепенно копит мёртвые метрики.
//
// Вызывается из reconcileWorkers в момент когда запрос пропал из конфига.
func unregisterQueryMetric(queryName string) {
	metricsMu.Lock()
	defer metricsMu.Unlock()

	m, ok := queryResultMetrics[queryName]
	if !ok {
		return
	}
	prometheus.Unregister(m)
	delete(queryResultMetrics, queryName)
}

// deleteWorkerRows удаляет все Prometheus-строки, за которые отвечал воркер,
// не трогая регистрацию самой метрики (метрика может ещё использоваться
// другими воркерами с тем же MetricName). Для multi-row удаляет каждую строку
// из prevLabels; для single-value (prevLabels пуст) — единственную строку,
// вычисленную из текущей комбинации лейблов cfg.
//
// Используется в двух местах: когда воркер полностью убран но метрику нельзя
// снести целиком (её использует другой воркер), и когда воркер перезапускается
// с другим значением env — в обоих случаях "старые" строки иначе остались бы
// висеть в GaugeVec навсегда, так как никто их больше не перезаписывает.
func deleteWorkerRows(cfg QueryConfig, prevLabels *[]prometheus.Labels) {
	m, ok := lookupQueryMetric(cfg.MetricName)
	if !ok {
		return
	}
	if prevLabels != nil && len(*prevLabels) > 0 {
		for _, lbl := range *prevLabels {
			m.Delete(lbl)
		}
		return
	}
	m.Delete(buildLabelValues(cfg.DB, cfg.DBEnv, cfg.Labels, nil))
}
