package main

import (
	"fmt"
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
		// reason: "timeout" | "db_error" | "schema_mismatch" — cancelled
		// сюда не попадает, это только исход в логе (см. runOnce), Inc()
		// для него никогда не вызывается.
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

	// invariantViolations — должен оставаться на нуле всегда. В отличие от
	// остальных метрик ошибок (БД недоступна, запрос упал — нормальные
	// операционные условия), ненулевое значение означает баг в самом
	// экспортёре, не проблему со средой/конфигом/БД.
	invariantViolations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "app_internal_invariant_violations_total",
			Help: "Count of internal invariant violations detected at runtime — should always stay at zero; any nonzero value indicates a bug in the exporter itself, not an operational issue",
		},
		[]string{"invariant"},
	)

	// configIncludeFailures — сколько инклюдов не удалось загрузить при
	// последнем reload. Такая ошибка не блокирует остальной конфиг, поэтому
	// не видна из самого факта "reload complete" — эта метрика даёт то,
	// на что можно поставить алерт.
	configIncludeFailures = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "app_config_include_failures",
		Help: "Number of includes that failed to load during the last config reload",
	})

	configReloadTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "app_config_reload_total",
			Help: "Total number of config reload attempts by result",
		},
		// result: "success" | "partial_success" | "load_error" | "invalid_config"
		[]string{"result"},
	)

	configLastReloadTimestamp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "app_config_last_reload_timestamp_seconds",
		Help: "Unix timestamp of the last config reload attempt, regardless of outcome",
	})

	configLastSuccessTimestamp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "app_config_last_reload_success_timestamp_seconds",
		Help: "Unix timestamp of the last successful config reload",
	})

	// configWatcherUp — 1 если fsnotify-watch на директорию конфига
	// установлен успешно (hot-reload работает), 0 если нет — exporter
	// тогда работает только с тем конфигом, что загрузил при старте.
	configWatcherUp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "app_config_watcher_up",
		Help: "1 if the config directory watcher (hot-reload) was set up successfully, 0 if hot-reload is unavailable and the exporter is running with a one-time config load",
	})

	// dbUp — обновляется сразу при (пере)подключении (updateDBUpMetrics) и
	// периодически активным Ping'ом (activeHealthCheck) — без последнего
	// значение отражало бы только "когда-то подключились", не текущую
	// доступность.
	dbUp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "app_db_up",
			Help: "1 if the last health check (active ping) for this database succeeded, 0 otherwise",
		},
		[]string{"db"},
	)

	// dbConfigApplied — отдельно от dbUp: не "жива ли БД", а "применён ли
	// последний желаемый конфиг". Может разойтись с dbUp во время graceful
	// degradation (ротация пароля/хоста) — старое соединение ещё пингуется
	// (dbUp=1), но новый кандидат не подключился (dbConfigApplied=0).
	dbConfigApplied = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "app_db_config_applied",
			Help: "1 if the database's currently running connection matches its latest desired config, 0 if a newer config candidate failed to connect and the exporter fell back to (or remains without) a previous connection",
		},
		[]string{"db"},
	)

	// configSchemaConflict — 1 пока у запроса config-level конфликт схемы
	// лейблов (labels:/value_column: изменились несовместимо с уже
	// зарегистрированной метрикой). Владеет исключительно workerManager —
	// worker.go эту метрику не трогает вообще. Отсутствие серии, а не 0 —
	// заметнее на дашборде. Ключ {query,db}, не только {query} — здесь и у
	// runtimeSchemaMismatch ниже: клоны одного источника на несколько БД
	// могут разойтись состоянием после рестарта, общий ключ позволил бы
	// одному клону стереть сигнал другого.
	configSchemaConflict = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "app_query_config_schema_conflict",
			Help: "1 if the query's configured label schema conflicts with its already-registered metric and cannot be applied without a process restart",
		},
		[]string{"query", "db"},
	)

	// runtimeSchemaMismatch — 1 пока колонки SQL-результата не совпадают с
	// уже зарегистрированной метрикой, обнаружено только при выполнении
	// (см. worker.go runOnce). Владеет исключительно execution path —
	// workerManager эту метрику не трогает. Отдельная от configSchemaConflict
	// метрика: раньше они делили одну series, и любой успешный запуск
	// замороженного (config-level) воркера тут же стирал сигнал, который
	// только что выставил workerManager, хотя desired config всё ещё не
	// применён — а любой обычный reconcile без изменений в config-level
	// схеме мог наоборот стереть чужой, ещё не разрешённый runtime-конфликт.
	runtimeSchemaMismatch = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "app_query_runtime_schema_mismatch",
			Help: "1 if the query's SQL result columns don't match its already-registered metric, discovered only at execution time",
		},
		[]string{"query", "db"},
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
		invariantViolations,
		configIncludeFailures,
		configReloadTotal,
		configLastReloadTimestamp,
		configLastSuccessTimestamp,
		configWatcherUp,
		dbUp,
		dbConfigApplied,
		configSchemaConflict,
		runtimeSchemaMismatch,
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

// getOrCreateQueryMetric возвращает или создаёт GaugeVec для запроса.
// Лейблы: "db", "env" (всегда), customLabels (статические из labels:),
// colLabels (динамические из имён столбцов SELECT для multi-row).
//
// Registry.Unregister() не чистит внутренний dimHashesByName — если под
// этим именем когда-либо был зарегистрирован GaugeVec с другим набором
// лейблов, Register() вернёт ошибку и после Unregister тоже; это
// ограничение client_golang, снимается только перезапуском процесса.
func getOrCreateQueryMetric(queryName string, customLabels map[string]string, colLabels []string) (*prometheus.GaugeVec, error) {
	metricsMu.Lock()
	defer metricsMu.Unlock()

	if m, ok := queryResultMetrics[queryName]; ok {
		return m, nil
	}

	labelNames := buildLabelNames(customLabels, colLabels)

	metric := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: queryName,
			Help: "SQL query result metric for " + queryName,
		},
		labelNames,
	)

	// Ownership строго через queryResultMetrics, не через факт наличия в
	// Registry коллектора с равным descriptor — "усыновление" через
	// AlreadyRegisteredError могло бы молча захватить наш же внутренний
	// коллектор (например app_query_up при коллизии имён), после чего
	// значения SQL-запроса писались бы прямо в наш сигнал здоровья.
	if err := prometheus.Register(metric); err != nil {
		return nil, fmt.Errorf(
			"metric %q: cannot register with the current label set — likely changed labels/value_column "+
				"since first registration, or a name collision with another metric; restart the process to fix: %w: %w",
			queryName, errSchemaMismatch, err)
	}

	queryResultMetrics[queryName] = metric
	return metric, nil
}

// buildLabelNames: "db", "env" (всегда — фиксированная схема нужна даже
// когда env не задан, иначе клонирование запроса на несколько БД могло бы
// дать разные наборы лейблов и вызвать панику в With()), затем статические
// лейблы (сорт.), затем динамические из колонок (порядок SELECT).
func buildLabelNames(customLabels map[string]string, colLabels []string) []string {
	names := make([]string, 0, 2+len(customLabels)+len(colLabels))
	names = append(names, "db", "env")

	staticKeys := make([]string, 0, len(customLabels))
	for k := range customLabels {
		staticKeys = append(staticKeys, k)
	}
	sort.Strings(staticKeys)
	names = append(names, staticKeys...)
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
// из queryResultMetrics. Без этого удалённый из конфига запрос оставлял бы
// метрику навечно зарегистрированной с последним известным значением.
// Вызывается из workerManager.reconcile, когда запрос пропал из конфига.
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

// deleteWorkerRows удаляет строки, за которые отвечал воркер, не трогая
// регистрацию метрики (её может ещё использовать другой воркер с тем же
// MetricName). Multi-row — все строки из prevLabels; single-value — одна,
// вычисленная из текущих лейблов cfg.
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

// deleteQueryHealthMetrics удаляет служебные метрики о состоянии запроса —
// app_query_up, app_query_last_success_timestamp_seconds, app_query_errors_total,
// app_query_duration_seconds. Ключ — (metricName, db), не сам MetricName
// целиком: при клонировании на несколько БД у каждого клона db разное,
// удаление одного не задевает остальные.
func deleteQueryHealthMetrics(metricName, db string) {
	if metricName == "" {
		return
	}
	queryUp.DeleteLabelValues(metricName, db)
	queryLastSuccess.DeleteLabelValues(metricName, db)
	queryDuration.DeleteLabelValues(metricName, db)
	// cancelled не входит — Inc() для этого reason никогда не вызывается
	// (см. runOnce), чистить нечего.
	for _, reason := range []string{"timeout", "db_error", "schema_mismatch"} {
		queryErrors.DeleteLabelValues(metricName, db, reason)
	}
}
