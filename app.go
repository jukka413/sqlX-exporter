package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

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
	for name, p := range newPools {
		a.pools[name] = p
	}
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
