package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Databases map[string]DBConfig    `yaml:"databases"`
	Queries   map[string]QueryConfig `yaml:"queries"`
}

type DBConfig struct {
	URL      string `yaml:"url"`
	MaxConns int    `yaml:"max_conns"`
	MinConns int    `yaml:"min_conns"`
}

type QueryConfig struct {
	DB       string `yaml:"db"`
	SQL      string `yaml:"sql"`
	Timeout  string `yaml:"timeout"`
	Interval string `yaml:"interval"`

	// Optional schedule. If provided, query runs at specified times instead of interval ticker.
	Schedule *ScheduleConfig `yaml:"schedule,omitempty"`
}

type ScheduleConfig struct {
	// IANA timezone name, e.g. "Europe/Berlin". If empty -> time.Local.
	Timezone string       `yaml:"timezone,omitempty"`
	At       []ScheduleAt `yaml:"at"`
}

type ScheduleAt struct {
	// "wed"/"wednesday", "fri"/"friday", etc.
	Weekday string `yaml:"weekday"`
	// "HH:MM" (24h), e.g. "03:00", "15:00"
	Time string `yaml:"time"`
}

//
// ================= METRICS =================
//

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

	prometheus.MustRegister(metric)
	queryResultMetrics[queryName] = metric
	return metric
}

//
// ================= RUNTIME STRUCTS =================
//

type worker struct {
	cancel context.CancelFunc
	wg     *sync.WaitGroup
	cfg    QueryConfig
}

type dbPool struct {
	cfg  DBConfig
	pool *pgxpool.Pool
}

type app struct {
	logger *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	configPath string

	mu      sync.Mutex
	workers map[string]*worker
	pools   map[string]*dbPool
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	appCtx, appCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer appCancel()

	a := &app{
		logger:     logger,
		ctx:        appCtx,
		cancel:     appCancel,
		configPath: "./config.yaml",
		workers:    make(map[string]*worker),
		pools:      make(map[string]*dbPool),
	}

	// ---- Metrics server ----
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		srv := &http.Server{
			Addr:              ":2112",
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		}
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server failed", "error", err)
		}
	}()

	// ---- Pool metrics updater ----
	go a.poolMetricsUpdater(5 * time.Second)

	// First load
	a.reload()

	// FSNotify watcher
	go watchConfig(a.ctx, a.logger, a.configPath, a.reload)

	<-a.ctx.Done()

	a.logger.Info("shutting down workers")
	a.stopAllWorkers()

	a.logger.Info("closing pools")
	a.closeAllPools()

	a.logger.Info("application stopped gracefully")
}

func (a *app) poolMetricsUpdater(period time.Duration) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			a.mu.Lock()
			for name, p := range a.pools {
				if p == nil || p.pool == nil {
					continue
				}
				stat := p.pool.Stat()
				dbPoolAcquired.WithLabelValues(name).Set(float64(stat.AcquiredConns()))
				dbPoolIdle.WithLabelValues(name).Set(float64(stat.IdleConns()))
				dbPoolTotal.WithLabelValues(name).Set(float64(stat.TotalConns()))
			}
			a.mu.Unlock()
		}
	}
}

func (a *app) reload() {
	a.logger.Info("reloading configuration")

	newCfg, err := loadConfig(a.configPath)
	if err != nil {
		a.logger.Error("failed to load config", "error", err)
		return
	}

	// Важно: проверки duration делаем заранее, чтобы не получить NewTicker(0) panic.
	if err := validateConfigDurations(newCfg); err != nil {
		a.logger.Error("invalid config durations", "error", err)
		return
	}

	// 1) Обновляем/создаём пулы (вне lock — там I/O и сеть)
	newPools, toClose := a.buildPools(newCfg.Databases)

	// 2) Применяем изменения в рантайм-структуры под lock минимально
	a.mu.Lock()
	// закрываем старые после unlock, но пометим их сейчас
	for name, p := range newPools {
		a.pools[name] = p
	}
	// оставляем в a.pools старые, если не переопределили — логика прежняя (конфиг может не содержать их)
	a.mu.Unlock()

	// закрытие старых пулов — уже без блокировки
	for name, p := range toClose {
		if p != nil && p.pool != nil {
			a.logger.Info("closing old pool", "db", name)
			p.pool.Close()
		}
	}

	// 3) Перезапускаем/запускаем воркеры
	a.reconcileWorkers(newCfg.Queries)

	a.logger.Info("smart reload complete")
}

func (a *app) buildPools(dbs map[string]DBConfig) (map[string]*dbPool, map[string]*dbPool) {
	newPools := make(map[string]*dbPool, len(dbs))
	toClose := make(map[string]*dbPool)

	// снимем текущий снапшот, чтобы сравнивать без удержания lock
	a.mu.Lock()
	current := make(map[string]*dbPool, len(a.pools))
	for k, v := range a.pools {
		current[k] = v
	}
	a.mu.Unlock()

	for name, dbCfg := range dbs {
		old, exists := current[name]
		needUpdate := !exists ||
			old.cfg.URL != dbCfg.URL ||
			old.cfg.MaxConns != dbCfg.MaxConns ||
			old.cfg.MinConns != dbCfg.MinConns

		if !needUpdate {
			newPools[name] = old
			continue
		}

		// старый пул закрываем позже
		if exists && old != nil && old.pool != nil {
			toClose[name] = old
		}

		pool, ok := a.createAndPingPool(name, dbCfg)
		if !ok {
			continue
		}

		newPools[name] = &dbPool{
			cfg:  dbCfg,
			pool: pool,
		}
	}

	return newPools, toClose
}

func (a *app) createAndPingPool(name string, dbCfg DBConfig) (*pgxpool.Pool, bool) {
	poolCfg, err := pgxpool.ParseConfig(dbCfg.URL)
	if err != nil {
		a.logger.Error("bad db config", "db", name, "error", err)
		dbConnectionErrors.WithLabelValues(name).Inc()
		return nil, false
	}

	poolCfg.MaxConns = int32(dbCfg.MaxConns)
	poolCfg.MinConns = int32(dbCfg.MinConns)

	pool, err := pgxpool.NewWithConfig(a.ctx, poolCfg)
	if err != nil {
		a.logger.Error("failed to create pool", "db", name, "error", err)
		dbConnectionErrors.WithLabelValues(name).Inc()
		return nil, false
	}

	pingCtx, cancel := context.WithTimeout(a.ctx, 5*time.Second)
	defer cancel()

	if err := pool.Ping(pingCtx); err != nil {
		a.logger.Error("ping failed", "db", name, "error", err)
		dbConnectionErrors.WithLabelValues(name).Inc()
		pool.Close()
		return nil, false
	}

	a.logger.Info("connected to database", "db", name)
	return pool, true
}

func (a *app) reconcileWorkers(queries map[string]QueryConfig) {
	// снапшоты под коротким lock
	a.mu.Lock()
	currentWorkers := make(map[string]*worker, len(a.workers))
	for k, v := range a.workers {
		currentWorkers[k] = v
	}
	currentPools := make(map[string]*dbPool, len(a.pools))
	for k, v := range a.pools {
		currentPools[k] = v
	}
	a.mu.Unlock()

	// старт/рестарт
	for name, q := range queries {
		pEntry, ok := currentPools[q.DB]
		if !ok || pEntry == nil || pEntry.pool == nil {
			a.logger.Error("db not available for query", "query", name, "db", q.DB)
			continue
		}

		w, exists := currentWorkers[name]
		changed := !exists || !sameQueryConfig(w.cfg, q)

		if !changed {
			continue
		}

		if exists {
			a.logger.Info("stopping changed query worker", "query", name)
			w.cancel()
			w.wg.Wait()
		}

		wg := &sync.WaitGroup{}
		wg.Add(1)
		ctx, cancel := context.WithCancel(a.ctx)

		go startQueryWorker(ctx, wg, a.logger, name, q, pEntry.pool)

		newW := &worker{cancel: cancel, wg: wg, cfg: q}

		a.mu.Lock()
		a.workers[name] = newW
		a.mu.Unlock()

		a.logger.Info("started new/updated query worker", "query", name)
	}

	// удалённые
	a.mu.Lock()
	for name, w := range a.workers {
		if _, ok := queries[name]; !ok {
			a.logger.Info("stopping removed query", "query", name)
			w.cancel()
			w.wg.Wait()
			delete(a.workers, name)
		}
	}
	a.mu.Unlock()
}

func (a *app) stopAllWorkers() {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, w := range a.workers {
		w.cancel()
		w.wg.Wait()
	}
}

func (a *app) closeAllPools() {
	// Важно: close pools без удержания lock на время Close()
	a.mu.Lock()
	local := make(map[string]*dbPool, len(a.pools))
	for name, p := range a.pools {
		local[name] = p
	}
	a.mu.Unlock()

	for name, p := range local {
		if p == nil || p.pool == nil {
			continue
		}
		a.logger.Info("closing pool", "db", name)
		p.pool.Close()
	}
}

func loadConfig(path string) (Config, error) {
	var cfg Config
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	err = yaml.Unmarshal(data, &cfg)
	return cfg, err
}

func validateConfigDurations(cfg Config) error {
	for name, q := range cfg.Queries {
		// Timeout always required
		if _, err := time.ParseDuration(q.Timeout); err != nil {
			return errors.New("query " + name + " has invalid timeout: " + err.Error())
		}

		// Schedule OR Interval is required.
		if q.Schedule != nil {
			if err := validateSchedule(q.Schedule); err != nil {
				return errors.New("query " + name + " has invalid schedule: " + err.Error())
			}
		} else {
			if _, err := time.ParseDuration(q.Interval); err != nil {
				return errors.New("query " + name + " has invalid interval: " + err.Error())
			}
		}
	}
	return nil
}

func validateSchedule(s *ScheduleConfig) error {
	if s == nil {
		return errors.New("schedule is nil")
	}
	if s.Timezone != "" {
		if _, err := time.LoadLocation(s.Timezone); err != nil {
			return err
		}
	}
	if len(s.At) == 0 {
		return errors.New("schedule.at is empty")
	}
	for _, a := range s.At {
		if _, err := parseWeekday(a.Weekday); err != nil {
			return err
		}
		if _, _, err := parseHHMM(a.Time); err != nil {
			return err
		}
	}
	return nil
}

func sameQueryConfig(a, b QueryConfig) bool {
	// NOTE: Schedule comparison is by value: if schedule changes in config, worker restarts.
	// Minimal approach: compare marshaled representation would be heavy; we do a shallow+deep compare manually.
	return a.DB == b.DB &&
		a.SQL == b.SQL &&
		a.Timeout == b.Timeout &&
		a.Interval == b.Interval &&
		sameSchedule(a.Schedule, b.Schedule)
}

func sameSchedule(a, b *ScheduleConfig) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.Timezone != b.Timezone {
		return false
	}
	if len(a.At) != len(b.At) {
		return false
	}
	for i := range a.At {
		if a.At[i].Weekday != b.At[i].Weekday || a.At[i].Time != b.At[i].Time {
			return false
		}
	}
	return true
}

func startQueryWorker(
	ctx context.Context,
	wg *sync.WaitGroup,
	logger *slog.Logger,
	name string,
	queryCfg QueryConfig,
	pool *pgxpool.Pool,
) {
	defer wg.Done()

	timeout, err := time.ParseDuration(queryCfg.Timeout)
	if err != nil {
		logger.Error("invalid timeout, worker stopped", "query", name, "error", err)
		return
	}

	// --- Scheduled mode (if schedule is provided) ---
	if queryCfg.Schedule != nil {
		loc, entries, err := parseSchedule(queryCfg.Schedule)
		if err != nil {
			logger.Error("invalid schedule, worker stopped", "query", name, "error", err)
			return
		}

		logger.Info("started scheduled query worker", "query", name)

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
				runOnce(ctx, logger, name, queryCfg, pool, timeout)
			}
		}
	}

	// --- Interval mode (existing behaviour) ---
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
			runOnce(ctx, logger, name, queryCfg, pool, timeout)
		}
	}
}

func runOnce(
	ctx context.Context,
	logger *slog.Logger,
	name string,
	queryCfg QueryConfig,
	pool *pgxpool.Pool,
	timeout time.Duration,
) {
	start := time.Now()

	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	var result any
	err := pool.QueryRow(queryCtx, queryCfg.SQL).Scan(&result)
	cancel()

	duration := time.Since(start).Seconds()
	queryDuration.WithLabelValues(name, queryCfg.DB).Observe(duration)

	if err != nil {
		queryErrors.WithLabelValues(name, queryCfg.DB).Inc()
		logger.Error("query failed", "query", name, "error", err)
		return
	}

	value, ok := toFloat64(result)
	if !ok {
		logger.Error("query result is not numeric", "query", name)
		return
	}

	metric := getOrCreateQueryMetric(name)
	metric.WithLabelValues(queryCfg.DB).Set(value)

	logger.Info("query success",
		"query", name,
		"value", value,
		"duration", duration,
	)
}

// ---- Schedule helpers (minimal, no external deps) ----

type scheduleEntry struct {
	weekday time.Weekday
	hour    int
	min     int
}

func parseSchedule(cfg *ScheduleConfig) (*time.Location, []scheduleEntry, error) {
	loc := time.Local
	var err error
	if cfg.Timezone != "" {
		loc, err = time.LoadLocation(cfg.Timezone)
		if err != nil {
			return nil, nil, err
		}
	}

	out := make([]scheduleEntry, 0, len(cfg.At))
	for _, a := range cfg.At {
		wd, err := parseWeekday(a.Weekday)
		if err != nil {
			return nil, nil, err
		}
		h, m, err := parseHHMM(a.Time)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, scheduleEntry{weekday: wd, hour: h, min: m})
	}

	if len(out) == 0 {
		return nil, nil, errors.New("schedule.at is empty")
	}

	return loc, out, nil
}

// nextRun returns the nearest future time matching any schedule entry, in the provided location.
func nextRun(now time.Time, loc *time.Location, entries []scheduleEntry) time.Time {
	n := now.In(loc)

	var best time.Time
	for _, e := range entries {
		// Start candidate at today's date in loc with target hh:mm
		c := time.Date(n.Year(), n.Month(), n.Day(), e.hour, e.min, 0, 0, loc)

		// Shift to target weekday
		dayDiff := (int(e.weekday) - int(n.Weekday()) + 7) % 7
		c = c.AddDate(0, 0, dayDiff)

		// If candidate is not strictly in the future, move to next week
		if !c.After(n) {
			c = c.AddDate(0, 0, 7)
		}

		if best.IsZero() || c.Before(best) {
			best = c
		}
	}
	return best
}

func parseHHMM(s string) (int, int, error) {
	// strict "HH:MM" 24h format
	t, err := time.Parse("15:04", strings.TrimSpace(s))
	if err != nil {
		return 0, 0, err
	}
	return t.Hour(), t.Minute(), nil
}

func parseWeekday(s string) (time.Weekday, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "sun", "sunday":
		return time.Sunday, nil
	case "mon", "monday":
		return time.Monday, nil
	case "tue", "tues", "tuesday":
		return time.Tuesday, nil
	case "wed", "wednesday":
		return time.Wednesday, nil
	case "thu", "thurs", "thursday":
		return time.Thursday, nil
	case "fri", "friday":
		return time.Friday, nil
	case "sat", "saturday":
		return time.Saturday, nil
	default:
		return 0, errors.New("invalid weekday: " + s)
	}
}

func watchConfig(ctx context.Context, logger *slog.Logger, path string, reload func()) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logger.Error("failed to create watcher", "error", err)
		return
	}
	defer watcher.Close()

	if err := watcher.Add(path); err != nil {
		logger.Error("failed to watch config", "error", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case event := <-watcher.Events:
			if event.Op&fsnotify.Write == fsnotify.Write {
				reload()
			}
		case err := <-watcher.Errors:
			logger.Error("watcher error", "error", err)
		}
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
	default:
		return 0, false
	}
}
