package main

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type worker struct {
	cancel     context.CancelFunc
	wg         *sync.WaitGroup
	cfg        QueryConfig
	prevLabels *[]prometheus.Labels
}

type dbPool struct {
	cfg DBConfig
	db  *sql.DB
}

type app struct {
	logger *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	configPath string

	mu sync.Mutex

	workers map[string]*worker
	pools   map[string]*dbPool

	// failedPools хранит конфиги БД, к которым не удалось подключиться.
	// poolHealthChecker периодически пытается переподключиться к ним.
	// При успехе — пул добавляется в pools и запускаются воркеры.
	failedPools map[string]DBConfig

	// queriesCfg хранит последний валидный конфиг запросов.
	// Нужен poolHealthChecker чтобы запустить воркеры после восстановления БД
	// без повторного чтения файла конфига.
	queriesCfg map[string]QueryConfig

	// reconnectInterval — текущий интервал переподключения к упавшим БД.
	// Обновляется при reload если значение в конфиге изменилось.
	reconnectInterval time.Duration

	// defaultDB — имя БД по умолчанию для запросов без явного поля db.
	defaultDB string
}

// =========================================================================
// poolMetricsUpdater
// =========================================================================

func (a *app) poolMetricsUpdater(period time.Duration) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			a.mu.Lock()
			snapshot := make(map[string]*dbPool, len(a.pools))
			for name, p := range a.pools {
				snapshot[name] = p
			}
			a.mu.Unlock()

			for name, p := range snapshot {
				if p == nil || p.db == nil {
					continue
				}
				stat := p.db.Stats()
				dbPoolAcquired.WithLabelValues(name).Set(float64(stat.InUse))
				dbPoolIdle.WithLabelValues(name).Set(float64(stat.Idle))
				dbPoolTotal.WithLabelValues(name).Set(float64(stat.OpenConnections))
			}
		}
	}
}

// =========================================================================
// poolHealthChecker
// =========================================================================

// poolHealthChecker периодически пытается переподключиться к БД из failedPools.
// При успехе добавляет пул в a.pools и запускает воркеры для этой БД.
// Интервал берётся из a.reconnectInterval и может меняться при hot-reload конфига.
func (a *app) poolHealthChecker() {
	a.mu.Lock()
	period := a.reconnectInterval
	a.mu.Unlock()

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			// Проверяем не изменился ли интервал после reload
			a.mu.Lock()
			newPeriod := a.reconnectInterval
			failed := make(map[string]DBConfig, len(a.failedPools))
			for k, v := range a.failedPools {
				failed[k] = v
			}
			queries := make(map[string]QueryConfig, len(a.queriesCfg))
			for k, v := range a.queriesCfg {
				queries[k] = v
			}
			a.mu.Unlock()

			// Пересоздаём тикер если интервал изменился в конфиге
			if newPeriod != period {
				ticker.Reset(newPeriod)
				period = newPeriod
				a.logger.Info("db reconnect interval updated", "interval", period)
			}

			if len(failed) == 0 {
				continue
			}

			for name, dbCfg := range failed {
				a.logger.Info("retrying db connection", "db", name)

				db, ok := a.createAndPingPool(name, dbCfg)
				if !ok {
					continue
				}

				a.mu.Lock()
				delete(a.failedPools, name)
				a.pools[name] = &dbPool{cfg: dbCfg, db: db}
				a.mu.Unlock()

				a.logger.Info("db reconnected", "db", name)
				a.reconcileWorkers(queries)
			}
		}
	}
}

// =========================================================================
// reload
// =========================================================================

func (a *app) reload() {
	a.logger.Info("reloading configuration")

	newCfg, err := loadConfig(a.configPath)
	if err != nil {
		a.logger.Error("failed to load config", "error", err)
		return
	}

	// Логируем инклюды которые были пропущены из-за отсутствия
	// записи в include_defaults — это осознанное поведение, но должно
	// быть видно в логах а не тихо проигнорировано.
	for _, reason := range newCfg.skippedIncludes {
		a.logger.Info("include skipped (no include_defaults entry)", "detail", reason)
	}

	// Критичные проверки — структурные ошибки конфига (драйверы, URL,
	// durations баз данных). Ошибка здесь означает что конфиг сломан
	// целиком, поэтому reload прерывается полностью.
	if err := validateDatabasesAndSettings(newCfg); err != nil {
		a.logger.Error("invalid config", "error", err)
		return
	}

	// Мягкая валидация запросов — каждый запрос проверяется независимо.
	// Невалидные запросы (например без db и без default_db) удаляются
	// из конфига с логированием, но остальные запросы продолжают работать.
	if removed := sanitizeQueries(&newCfg); len(removed) > 0 {
		for _, reason := range removed {
			a.logger.Error("skipping invalid query", "reason", reason)
		}
	}

	newPools, toClose, newFailed := a.buildPools(newCfg.Databases)

	a.mu.Lock()
	for name, p := range newPools {
		a.pools[name] = p
	}
	// Обновляем failedPools: добавляем новые упавшие,
	// убираем те которых больше нет в конфиге
	for name, cfg := range newFailed {
		a.failedPools[name] = cfg
	}
	for name := range a.failedPools {
		if _, stillInCfg := newCfg.Databases[name]; !stillInCfg {
			delete(a.failedPools, name)
		}
	}
	// Сохраняем актуальный конфиг запросов для poolHealthChecker
	a.queriesCfg = newCfg.Queries
	// Обновляем интервал переподключения — poolHealthChecker подхватит при следующем тике
	a.reconnectInterval = newCfg.Settings.DBReconnectIntervalDuration()
	a.defaultDB = newCfg.Settings.DefaultDB
	a.mu.Unlock()

	for name, p := range toClose {
		if p != nil && p.db != nil {
			a.logger.Info("closing old pool", "db", name)
			_ = p.db.Close()
		}
	}

	a.reconcileWorkers(newCfg.Queries)

	a.logger.Info("smart reload complete")
}

// =========================================================================
// buildPools
// =========================================================================

// buildPools возвращает три map:
//   - newPools  — пулы готовые к использованию
//   - toClose   — старые пулы которые нужно закрыть
//   - failed    — конфиги БД к которым не удалось подключиться
func (a *app) buildPools(dbs map[string]DBConfig) (newPools, toClose map[string]*dbPool, failed map[string]DBConfig) {
	newPools = make(map[string]*dbPool, len(dbs))
	toClose = make(map[string]*dbPool)
	failed = make(map[string]DBConfig)

	a.mu.Lock()
	current := make(map[string]*dbPool, len(a.pools))
	for k, v := range a.pools {
		current[k] = v
	}
	a.mu.Unlock()

	for name, dbCfg := range dbs {
		dbCfg.Driver = strings.TrimSpace(dbCfg.Driver)

		old, exists := current[name]
		needUpdate := !exists ||
			old.cfg.Driver != dbCfg.Driver ||
			old.cfg.URL != dbCfg.URL ||
			old.cfg.MaxConns != dbCfg.MaxConns ||
			old.cfg.MaxIdleConns != dbCfg.MaxIdleConns ||
			old.cfg.MaxConnLifetime != dbCfg.MaxConnLifetime ||
			old.cfg.MaxConnIdleTime != dbCfg.MaxConnIdleTime ||
			old.cfg.HealthCheckPeriod != dbCfg.HealthCheckPeriod

		if !needUpdate {
			newPools[name] = old
			continue
		}

		db, ok := a.createAndPingPool(name, dbCfg)
		if !ok {
			// Не удалось создать новый пул — старый пул НЕ закрываем.
			// Воркеры продолжают работать с существующим подключением.
			// Новый конфиг запоминаем в failedPools для повторных попыток.
			if exists {
				newPools[name] = old
			}
			failed[name] = dbCfg
			continue
		}

		// Новый пул успешно создан — теперь можно закрыть старый
		if exists && old != nil && old.db != nil {
			toClose[name] = old
		}

		newPools[name] = &dbPool{cfg: dbCfg, db: db}
	}

	return newPools, toClose, failed
}

// =========================================================================
// createAndPingPool
// =========================================================================

func (a *app) createAndPingPool(name string, dbCfg DBConfig) (*sql.DB, bool) {
	driver := strings.TrimSpace(dbCfg.Driver)
	if driver == "" {
		a.logger.Error("db driver is required", "db", name)
		dbConnectionErrors.WithLabelValues(name).Inc()
		return nil, false
	}

	dbCfg.Driver = driver

	db, err := openAndPingDB(a.ctx, dbCfg)
	if err != nil {
		a.logger.Error("failed to open db", "db", name, "driver", dbCfg.Driver, "error", err)
		dbConnectionErrors.WithLabelValues(name).Inc()
		return nil, false
	}

	a.logger.Info("connected to database", "db", name, "driver", dbCfg.Driver)
	return db, true
}

// =========================================================================
// reconcileWorkers
// =========================================================================

func (a *app) reconcileWorkers(queries map[string]QueryConfig) {
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

	for name, q := range queries {
		// Подставляем БД по умолчанию если в запросе не указана явная БД
		if q.DB == "" {
			if a.defaultDB == "" {
				a.logger.Error("query has no db and no default_db is set", "query", name)
				continue
			}
			q.DB = a.defaultDB
		}

		pEntry, ok := currentPools[q.DB]
		if !ok || pEntry == nil || pEntry.db == nil {
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

		var prevLabels *[]prometheus.Labels
		if exists && w.prevLabels != nil {
			prevLabels = w.prevLabels
		} else {
			prevLabels = &[]prometheus.Labels{}
		}

		wg := &sync.WaitGroup{}
		wg.Add(1)
		ctx, cancel := context.WithCancel(a.ctx)

		go startQueryWorker(ctx, wg, a.logger, name, q, pEntry.db, prevLabels)

		newW := &worker{cancel: cancel, wg: wg, cfg: q, prevLabels: prevLabels}

		a.mu.Lock()
		a.workers[name] = newW
		a.mu.Unlock()

		a.logger.Info("started new/updated query worker", "query", name)
	}

	a.mu.Lock()
	for name, w := range a.workers {
		if _, ok := queries[name]; !ok {
			a.logger.Info("stopping removed query", "query", name)
			w.cancel()
			wg := w.wg
			delete(a.workers, name)
			a.mu.Unlock()
			wg.Wait()
			a.mu.Lock()
		}
	}
	a.mu.Unlock()
}

// =========================================================================
// stopAllWorkers / closeAllPools
// =========================================================================

func (a *app) stopAllWorkers() {
	a.mu.Lock()
	workers := make([]*worker, 0, len(a.workers))
	for _, w := range a.workers {
		w.cancel()
		workers = append(workers, w)
	}
	a.mu.Unlock()

	for _, w := range workers {
		w.wg.Wait()
	}
}

func (a *app) closeAllPools() {
	a.mu.Lock()
	local := make(map[string]*dbPool, len(a.pools))
	for name, p := range a.pools {
		local[name] = p
	}
	a.mu.Unlock()

	for name, p := range local {
		if p == nil || p.db == nil {
			continue
		}
		a.logger.Info("closing pool", "db", name)
		_ = p.db.Close()
	}
}
