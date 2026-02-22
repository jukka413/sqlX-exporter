package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
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
	Databases map[string]struct {
		URL      string `yaml:"url"`
		MaxConns int    `yaml:"max_conns"`
		MinConns int    `yaml:"min_conns"`
	} `yaml:"databases"`

	Queries map[string]struct {
		DB       string `yaml:"db"`
		SQL      string `yaml:"sql"`
		Timeout  string `yaml:"timeout"`
		Interval string `yaml:"interval"`
	} `yaml:"queries"`
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

//
// ================= STRUCTS =================
//

type Worker struct {
	cancel context.CancelFunc
	wg     *sync.WaitGroup
	cfg    struct {
		SQL      string
		Timeout  string
		Interval string
		DB       string
	}
}

type DBPool struct {
	cfg struct {
		URL      string
		MaxConns int
		MinConns int
	}
	pool *pgxpool.Pool
}

//
// ================= MAIN =================
//

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	appCtx, appCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer appCancel()

	// ---- Metrics server ----
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(":2112", nil); err != nil {
			logger.Error("metrics server failed", "error", err)
		}
	}()

	configPath := "./config.yaml"

	var (
		workers = make(map[string]*Worker)
		pools   = make(map[string]*DBPool)
		mu      sync.Mutex
	)

	// ---- Pool metrics updater ----
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-appCtx.Done():
				return
			case <-ticker.C:
				mu.Lock()
				for name, p := range pools {
					stat := p.pool.Stat()
					dbPoolAcquired.WithLabelValues(name).Set(float64(stat.AcquiredConns()))
					dbPoolIdle.WithLabelValues(name).Set(float64(stat.IdleConns()))
					dbPoolTotal.WithLabelValues(name).Set(float64(stat.TotalConns()))
				}
				mu.Unlock()
			}
		}
	}()

	reload := func() {
		mu.Lock()
		defer mu.Unlock()

		logger.Info("reloading configuration")
		newCfg, err := loadConfig(configPath)
		if err != nil {
			logger.Error("failed to load config", "error", err)
			return
		}

		// ---------------- DB Pools ----------------
		for name, dbCfg := range newCfg.Databases {
			old, exists := pools[name]
			needUpdate := !exists || old.cfg.URL != dbCfg.URL || old.cfg.MaxConns != dbCfg.MaxConns || old.cfg.MinConns != dbCfg.MinConns
			if needUpdate {
				if exists && old.pool != nil {
					logger.Info("closing old pool", "db", name)
					old.pool.Close()
				}

				poolCfg, err := pgxpool.ParseConfig(dbCfg.URL)
				if err != nil {
					logger.Error("bad db config", "db", name, "error", err)
					dbConnectionErrors.WithLabelValues(name).Inc()
					continue
				}
				poolCfg.MaxConns = int32(dbCfg.MaxConns)
				poolCfg.MinConns = int32(dbCfg.MinConns)

				pool, err := pgxpool.NewWithConfig(appCtx, poolCfg)
				if err != nil {
					logger.Error("failed to create pool", "db", name, "error", err)
					dbConnectionErrors.WithLabelValues(name).Inc()
					continue
				}

				pingCtx, cancel := context.WithTimeout(appCtx, 5*time.Second)
				if err := pool.Ping(pingCtx); err != nil {
					cancel()
					logger.Error("ping failed", "db", name, "error", err)
					dbConnectionErrors.WithLabelValues(name).Inc()
					pool.Close()
					continue
				}
				cancel()

				logger.Info("connected to database", "db", name)
				pools[name] = &DBPool{
					cfg: struct {
						URL                string
						MaxConns, MinConns int
					}{dbCfg.URL, dbCfg.MaxConns, dbCfg.MinConns},
					pool: pool,
				}
			}
		}

		// ---------------- Workers ----------------
		for name, q := range newCfg.Queries {
			w, exists := workers[name]
			poolEntry, poolExists := pools[q.DB]
			if !poolExists {
				logger.Error("db not available for query", "query", name, "db", q.DB)
				continue
			}

			changed := !exists ||
				w.cfg.SQL != q.SQL ||
				w.cfg.Timeout != q.Timeout ||
				w.cfg.Interval != q.Interval ||
				w.cfg.DB != q.DB

			if changed {
				// если воркер существует, останавливаем его
				if exists {
					logger.Info("stopping changed query worker", "query", name)
					w.cancel()
					w.wg.Wait()
				}

				// создаём новый воркер
				wg := &sync.WaitGroup{}
				wg.Add(1)
				ctx, cancel := context.WithCancel(appCtx)
				go startQueryWorker(ctx, wg, logger, name, q, poolEntry.pool)

				workers[name] = &Worker{
					cancel: cancel,
					wg:     wg,
					cfg: struct {
						SQL      string
						Timeout  string
						Interval string
						DB       string
					}{q.SQL, q.Timeout, q.Interval, q.DB},
				}

				logger.Info("started new/updated query worker", "query", name)
			}
		}

		// Останавливаем удалённые воркеры
		for name, w := range workers {
			if _, ok := newCfg.Queries[name]; !ok {
				logger.Info("stopping removed query", "query", name)
				w.cancel()
				w.wg.Wait()
				delete(workers, name)
			}
		}

		logger.Info("smart reload complete")
	}

	// Первый старт
	reload()

	// FSNotify watcher
	go watchConfig(appCtx, logger, configPath, reload)

	<-appCtx.Done()

	logger.Info("shutting down workers")

	mu.Lock()
	for _, w := range workers {
		w.cancel()
		w.wg.Wait()
	}
	mu.Unlock()

	for name, p := range pools {
		logger.Info("closing pool", "db", name)
		p.pool.Close()
	}

	logger.Info("application stopped gracefully")

}

// ---------------- Helpers ----------------

func loadConfig(path string) (Config, error) {
	var cfg Config
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	err = yaml.Unmarshal(data, &cfg)
	return cfg, err
}

func startQueryWorker(
	ctx context.Context,
	wg *sync.WaitGroup,
	logger *slog.Logger,
	name string,
	queryCfg struct {
		DB       string `yaml:"db"`
		SQL      string `yaml:"sql"`
		Timeout  string `yaml:"timeout"`
		Interval string `yaml:"interval"`
	},
	pool *pgxpool.Pool,
) {
	defer wg.Done()

	interval, _ := time.ParseDuration(queryCfg.Interval)
	timeout, _ := time.ParseDuration(queryCfg.Timeout)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	logger.Info("started query worker", "query", name)

	for {
		select {
		case <-ctx.Done():
			logger.Info("stopping query worker", "query", name)
			return
		case <-ticker.C:
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
				continue
			}

			logger.Info("query success", "query", name, "result", result, "duration", time.Since(start).String())
		}
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
