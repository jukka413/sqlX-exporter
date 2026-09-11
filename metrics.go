package main

import (
	"errors"
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

	// dbUp — обновляется в двух местах: сразу при (пере)подключении (см.
	// updateDBUpMetrics) для быстрой положительной реакции, и периодически
	// activeHealthCheck'ом через реальный PingContext — иначе значение
	// отражало бы только факт "когда-то подключились", а не текущую
	// доступность: тихо умершая сеть держала бы 1 бесконечно, пока
	// какой-нибудь query не наткнётся на неё сам. Существует для каждой БД
	// конфига всегда, в отличие от app_query_up, которой может не быть,
	// если ни один воркер для этой БД не стартовал.
	dbUp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "app_db_up",
			Help: "1 if the last health check (active ping) for this database succeeded, 0 otherwise",
		},
		[]string{"db"},
	)

	// dbConfigApplied — отдельный вопрос от dbUp: не "жива ли БД сейчас",
	// а "то ли применено, что сейчас написано в конфиге". Может законно
	// разойтись с dbUp во время graceful degradation (ротация пароля/хоста):
	// старое соединение продолжает пинговаться (dbUp=1), но новый кандидат
	// конфига не подключился (dbConfigApplied=0) — без отдельной метрики
	// эту ситуацию видно только по логам, не по текущему состоянию.
	dbConfigApplied = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "app_db_config_applied",
			Help: "1 if the database's currently running connection matches its latest desired config, 0 if a newer config candidate failed to connect and the exporter fell back to (or remains without) a previous connection",
		},
		[]string{"db"},
	)

	// queryHealthSchemaConflict — 1 пока у запроса конфликт схемы лейблов,
	// из двух источников: config-level (см. workerManager.reconcile —
	// labels:/value_column: изменились, известно заранее) и runtime
	// (см. worker.go runOnce — колонки SQL-результата не совпали с уже
	// зарегистрированной метрикой, известно только после выполнения).
	// Отсутствие серии, а не 0 — так проще заметить на дашборде, не читая
	// логи построчно. Ключ {query,db}, не только {query} — при
	// клонировании на несколько БД клоны одного источника могут разойтись
	// состоянием после рестарта (одному пул заменили, другому нет), и
	// общий ключ на всех позволил бы одному клону стереть сигнал,
	// актуальный для другого.
	queryHealthSchemaConflict = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "app_query_schema_conflict",
			Help: "1 if the running query's label schema conflicts with its current config and cannot be applied without a process restart",
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
		configIncludeFailures,
		configReloadTotal,
		configLastReloadTimestamp,
		configLastSuccessTimestamp,
		dbUp,
		dbConfigApplied,
		queryHealthSchemaConflict,
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
// Если под этим именем уже был зарегистрирован GaugeVec с другим набором
// лейблов (когда-либо в этом процессе, до или после Unregister) — Register()
// вернёт ошибку: Registry.Unregister() намеренно не чистит внутренний
// dimHashesByName ("must be consistent throughout the lifetime of a program").
// "Снять и пересоздать" метрику с другой схемой поэтому не работает — это
// ограничение client_golang, не решаемая здесь проблема. Ошибка возвращается
// как обычная ошибка запроса (query_up=0), не паника — снимается только
// перезапуском процесса.
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

	if err := prometheus.Register(metric); err != nil {
		are := &prometheus.AlreadyRegisteredError{}
		if errors.As(err, are) {
			if existing, ok := are.ExistingCollector.(*prometheus.GaugeVec); ok {
				queryResultMetrics[queryName] = existing
				return existing, nil
			}
		}
		return nil, fmt.Errorf(
			"metric %q: cannot register with the current label set — likely changed labels/value_column "+
				"since first registration, or a name collision with another query; restart the process to fix: %w: %w",
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
// app_query_duration_seconds. В отличие от бизнес-метрики (unregisterQueryMetric/
// deleteWorkerRows), про них раньше забывали при остановке воркера — они
// оставались замороженными на последнем значении навсегда, включая
// app_query_up=0, что выглядело бы как вечно горящий алерт для запроса,
// которого уже нет в конфиге.
//
// Ключ здесь — (metricName, db), а не сам MetricName целиком (как для
// business-метрики) — при клонировании на несколько БД у каждого клона
// db разное, поэтому удаление одного клона не задевает остальные.
func deleteQueryHealthMetrics(metricName, db string) {
	if metricName == "" {
		return
	}
	queryUp.DeleteLabelValues(metricName, db)
	queryLastSuccess.DeleteLabelValues(metricName, db)
	queryDuration.DeleteLabelValues(metricName, db)
	// cancelled не входит в список — runOnce возвращается до Inc() именно
	// для этого reason, значения counter'а с ним никогда не бывает, чистить
	// нечего. schema_mismatch — реальный, накапливающийся reason, отсутствие
	// его здесь раньше оставляло {query,db,reason="schema_mismatch"}
	// навсегда после удаления запроса.
	for _, reason := range []string{"timeout", "db_error", "schema_mismatch"} {
		queryErrors.DeleteLabelValues(metricName, db, reason)
	}
}
