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
//  2. статические лейблы из customLabels — в алфавитном порядке
//  3. динамические лейблы из colLabels (имена столбцов) — в исходном порядке столбцов
//
// Такой порядок гарантирует стабильность между вызовами — Prometheus требует
// чтобы имена и значения лейблов передавались в одном и том же порядке.
func buildLabelNames(customLabels map[string]string, colLabels []string) []string {
	names := make([]string, 0, 1+len(customLabels)+len(colLabels))
	names = append(names, "db")

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
// Принимает те же три источника что buildLabelNames.
func buildLabelValues(dbName string, customLabels map[string]string, colLabelValues map[string]string) prometheus.Labels {
	labels := make(prometheus.Labels, 1+len(customLabels)+len(colLabelValues))
	labels["db"] = dbName
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
