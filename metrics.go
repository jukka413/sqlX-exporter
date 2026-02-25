package main

import (
	"errors"
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
	prometheus.MustRegister(queryErrors)
	prometheus.MustRegister(dbConnectionErrors)
	prometheus.MustRegister(queryDuration)
	prometheus.MustRegister(dbPoolAcquired)
	prometheus.MustRegister(dbPoolIdle)
	prometheus.MustRegister(dbPoolTotal)
}

// ================= Custom metrics ==========

var (
	queryResultMetrics = make(map[string]*prometheus.GaugeVec)
	metricsMu          sync.Mutex
)

// getOrCreateQueryMetric безопасно возвращает или создаёт GaugeVec для данного запроса.
// Использует AlreadyRegisteredError вместо MustRegister, чтобы избежать паники при hot-reload:
// если метрика уже была зарегистрирована (например, запрос удалили и добавили снова),
// возвращается существующий коллектор.
// См. документацию: https://pkg.go.dev/github.com/prometheus/client_golang/prometheus#AlreadyRegisteredError
func getOrCreateQueryMetric(queryName string) *prometheus.GaugeVec {
	metricsMu.Lock()
	defer metricsMu.Unlock()

	if m, ok := queryResultMetrics[queryName]; ok {
		return m
	}

	metric := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: queryName,
			Help: "SQL query result metric for " + queryName,
		},
		[]string{"db"},
	)

	if err := prometheus.Register(metric); err != nil {
		are := &prometheus.AlreadyRegisteredError{}
		if errors.As(err, are) {
			// Метрика уже зарегистрирована ранее — используем существующую.
			// Это нормальная ситуация при hot-reload конфига.
			existing, ok := are.ExistingCollector.(*prometheus.GaugeVec)
			if ok {
				queryResultMetrics[queryName] = existing
				return existing
			}
		}
		// Любая другая ошибка регистрации — несовместимый коллектор,
		// это программная ошибка, паника оправдана.
		panic(err)
	}

	queryResultMetrics[queryName] = metric
	return metric
}
