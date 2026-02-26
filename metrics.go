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
// Лейблы: всегда присутствует "db", к нему добавляются кастомные лейблы из конфига.
// Порядок лейблов фиксируется при первом создании метрики — при hot-reload набор лейблов
// должен совпадать, иначе вернётся существующая метрика со старым набором лейблов.
//
// Использует AlreadyRegisteredError вместо MustRegister, чтобы избежать паники при hot-reload.
// См. документацию: https://pkg.go.dev/github.com/prometheus/client_golang/prometheus#AlreadyRegisteredError
func getOrCreateQueryMetric(queryName string, customLabels map[string]string) *prometheus.GaugeVec {
	metricsMu.Lock()
	defer metricsMu.Unlock()

	if m, ok := queryResultMetrics[queryName]; ok {
		return m
	}

	// Формируем упорядоченный список имён лейблов: сначала "db", затем кастомные
	// в детерминированном (алфавитном) порядке — Prometheus требует стабильный порядок.
	labelNames := buildLabelNames(customLabels)

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
// "db" всегда первый, затем кастомные в алфавитном порядке.
func buildLabelNames(customLabels map[string]string) []string {
	names := make([]string, 0, 1+len(customLabels))
	names = append(names, "db")
	for k := range customLabels {
		names = append(names, k)
	}
	// Сортируем кастомные лейблы для детерминированного порядка
	sort.Strings(names[1:])
	return names
}

// buildLabelValues возвращает значения лейблов в том же порядке что buildLabelNames.
func buildLabelValues(dbName string, customLabels map[string]string) prometheus.Labels {
	labels := make(prometheus.Labels, 1+len(customLabels))
	labels["db"] = dbName
	for k, v := range customLabels {
		labels[k] = v
	}
	return labels
}
