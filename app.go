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
	cancel context.CancelFunc
	wg     *sync.WaitGroup
	cfg    QueryConfig
	// prevLabels хранит лейблы последнего успешного multi-row запуска.
	// Переносится в новый воркер при hot-reload чтобы reconciliation
	// мог удалить метрики исчезнувших строк даже после перезапуска воркера.
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

	mu      sync.Mutex
	workers map[string]*worker
	pools   map[string]*dbPool
}

func (a *app) poolMetricsUpdater(period time.Duration) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			// Делаем snapshot под локом, чтобы не держать его во время обновления метрик
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

func (a *app) reload() {
	a.logger.Info("reloading configuration")

	newCfg, err := loadConfig(a.configPath)
	if err != nil {
		a.logger.Error("failed to load config", "error", err)
		return
	}

	if err := validateConfigDurations(newCfg); err != nil {
		a.logger.Error("invalid config", "error", err)
		return
	}

	newPools, toClose := a.buildPools(newCfg.Databases)

	a.mu.Lock()
	for name, p := range newPools {
		a.pools[name] = p
	}
	a.mu.Unlock()

	// close old pools after unlock
	for name, p := range toClose {
		if p != nil && p.db != nil {
			a.logger.Info("closing old pool", "db", name)
			_ = p.db.Close()
		}
	}

	a.reconcileWorkers(newCfg.Queries)

	a.logger.Info("smart reload complete")
}

func (a *app) buildPools(dbs map[string]DBConfig) (map[string]*dbPool, map[string]*dbPool) {
	newPools := make(map[string]*dbPool, len(dbs))
	toClose := make(map[string]*dbPool)

	// snapshot current pools
	a.mu.Lock()
	current := make(map[string]*dbPool, len(a.pools))
	for k, v := range a.pools {
		current[k] = v
	}
	a.mu.Unlock()

	for name, dbCfg := range dbs {
		// normalize driver whitespace
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

		if exists && old != nil && old.db != nil {
			toClose[name] = old
		}

		db, ok := a.createAndPingPool(name, dbCfg)
		if !ok {
			continue
		}

		newPools[name] = &dbPool{
			cfg: dbCfg,
			db:  db,
		}
	}

	return newPools, toClose
}

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

func (a *app) reconcileWorkers(queries map[string]QueryConfig) {
	// snapshots
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

		// Переносим prevLabels из старого воркера если он существовал.
		// Это позволяет reconciliation в новом воркере корректно удалить
		// метрики строк, исчезнувших из результата запроса после reload.
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

	// removed workers
	a.mu.Lock()
	for name, w := range a.workers {
		if _, ok := queries[name]; !ok {
			a.logger.Info("stopping removed query", "query", name)
			w.cancel()
			// Не держим lock во время Wait — воркер может попытаться взять a.mu.
			// Сохраняем wg локально, разлочиваем, затем ждём.
			wg := w.wg
			delete(a.workers, name)
			a.mu.Unlock()
			wg.Wait()
			a.mu.Lock()
		}
	}
	a.mu.Unlock()
}

func (a *app) stopAllWorkers() {
	// Собираем snapshot воркеров под локом, затем разлочиваем
	// и только потом ждём завершения каждого.
	// Это предотвращает потенциальный дедлок: если воркер в момент завершения
	// попытается взять a.mu, при удержании лока в этом методе возникнет дедлок.
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
