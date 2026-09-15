package main

import (
	"context"
	"log/slog"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type worker struct {
	cancel     context.CancelFunc
	wg         *sync.WaitGroup
	cfg        QueryConfig
	pool       *dbPool
	prevLabels *[]prometheus.Labels
}

// workerManager — единственный владелец жизненного цикла воркеров: старт,
// остановка, снятие Prometheus-метрики с регистрации. Сам воркер (worker.go)
// никогда не трогает Registry напрямую.
type workerManager struct {
	ctx    context.Context
	logger *slog.Logger

	mu          sync.Mutex
	reconcileMu sync.Mutex
	workers     map[string]*worker

	// lastQueries — последнее применённое состояние запросов, проштамповано
	// той же revision, что и poolManager в этом же reload (см. app.revision).
	lastQueries         map[string]QueryConfig
	lastQueriesRevision uint64
}

func newWorkerManager(ctx context.Context, logger *slog.Logger) *workerManager {
	return &workerManager{
		ctx:         ctx,
		logger:      logger,
		workers:     make(map[string]*worker),
		lastQueries: make(map[string]QueryConfig),
	}
}

func (wm *workerManager) snapshotLastQueries() (map[string]QueryConfig, uint64) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	out := make(map[string]QueryConfig, len(wm.lastQueries))
	for k, v := range wm.lastQueries {
		out[k] = v
	}
	return out, wm.lastQueriesRevision
}

// ReconcileStats — что сделал reconcile, для содержательного лога после reload.
type ReconcileStats struct {
	Started         int
	Restarted       int
	Stopped         int
	Unchanged       int
	Pending         int // заморожено на старом конфиге, потому что БД pending
	SchemaConflicts int // конфликт схемы лейблов — заморожен или остановлен
}

// reconcile приводит воркеры в соответствие с queries. revision — то же
// число, которым в этом reload проштампован poolManager. pendingDBs — имена
// БД, чей новый желаемый конфиг ещё не применился (см. poolManager.pendingDBs).
func (wm *workerManager) reconcile(revision uint64, queries map[string]QueryConfig, pools map[string]*dbPool, pendingDBs map[string]struct{}, defaultDB string) ReconcileStats {
	wm.reconcileMu.Lock()
	defer wm.reconcileMu.Unlock()

	var stats ReconcileStats

	wm.mu.Lock()
	// reconcileMu сериализует вызовы, но не их порядок по revision — без
	// этой проверки запоздавший reconcile(R-1) мог бы откатить уже
	// применённый reconcile(R).
	if revision < wm.lastQueriesRevision {
		current := wm.lastQueriesRevision
		wm.mu.Unlock()
		wm.logger.Warn("skipping stale worker reconcile — a newer revision was already applied",
			"revision", revision, "current_revision", current)
		return stats
	}
	currentWorkers := make(map[string]*worker, len(wm.workers))
	for k, v := range wm.workers {
		currentWorkers[k] = v
	}
	wm.lastQueries = make(map[string]QueryConfig, len(queries))
	for k, v := range queries {
		wm.lastQueries[k] = v
	}
	wm.lastQueriesRevision = revision
	wm.mu.Unlock()

	for name, q := range queries {
		if q.DB == "" {
			if defaultDB == "" {
				wm.logger.Error("query has no db and no default_db is set", "query", name)
				continue
			}
			q.DB = defaultDB
		}

		w, exists := currentWorkers[name]

		// pendingDBs: dial нового конфига этой БД провалился — pools[q.DB],
		// если вообще есть, это удержанный старый пул. Нельзя применять
		// новый QueryConfig к пулу, который не соответствует ему.
		wasPending := false
		if _, pending := pendingDBs[q.DB]; pending {
			wasPending = true
			if !exists {
				wm.logger.Error("db config is pending (new candidate failed to connect) — "+
					"not starting a new query on the stale fallback pool",
					"query", name, "db", q.DB)
				stats.Pending++
				continue
			}
			wm.logger.Error("db config is pending (new candidate failed to connect) — "+
				"keeping the previous query config running on the old pool",
				"query", name, "db", q.DB)
			q = w.cfg
		}

		// Prometheus не позволяет поменять схему лейблов зарегистрированной
		// метрики без рестарта. Замораживаем весь QueryConfig на последнем
		// рабочем состоянии (не только Labels/ValueColumn, но и SQL/DB) —
		// иначе получился бы гибрид: новый SQL со старой схемой.
		schemaChanged := exists &&
			(w.cfg.ValueColumn != q.ValueColumn || !sameLabelKeys(w.cfg.Labels, q.Labels))
		if schemaChanged {
			wm.logger.Error(
				"labels or value_column changed for a running query — Prometheus does not support "+
					"changing a metric's label schema without a process restart; the entire query "+
					"config is frozen at its last working state until restart",
				"query", name, "db", q.DB)
			q = w.cfg
		}

		pEntry, ok := pools[q.DB]
		if !ok || pEntry == nil || pEntry.db == nil {
			wm.logger.Error("db not available for query", "query", name, "db", q.DB)
			if wm.stopWorker(name) {
				stats.Stopped++
			}
			continue
		}

		// Заморозка схемы фиксирует SQL, но не пул — pEntry резолвится заново
		// по q.DB. Если URL этой же БД тоже сменился и новый хост подключился
		// (pEntry — другой объект, не w.pool), крутить старый SQL на новом
		// соединении небезопасно: это может быть физически другая БД.
		if schemaChanged && w.pool != pEntry {
			wm.logger.Error("schema conflict AND the underlying db pool also changed in this reload — "+
				"cannot safely keep running the frozen query against a different db endpoint, stopping",
				"query", name, "db", q.DB)
			stats.SchemaConflicts++
			if wm.stopWorker(name) {
				stats.Stopped++
			}
			continue
		}

		// Гейдж конфликта выставляем только теперь, когда точно знаем что
		// воркер остаётся жив (заморожен, но не остановлен) — иначе для
		// случая выше (стоп из-за смены пула) гейдж мигнул бы Set(1) и тут
		// же DeleteLabelValues внутри stopWorker в рамках одного и того же
		// прохода, и ни один реальный scrape не успел бы его увидеть.
		if schemaChanged {
			if w.cfg.MetricName != "" {
				configSchemaConflict.WithLabelValues(w.cfg.MetricName, w.cfg.DB).Set(1)
			}
			stats.SchemaConflicts++
		} else if exists && w.cfg.MetricName != "" {
			configSchemaConflict.DeleteLabelValues(w.cfg.MetricName, w.cfg.DB)
		}

		q.DBEnv = pEntry.cfg.Env

		poolChanged := exists && w.pool != pEntry

		identityChanged := exists &&
			(w.cfg.DB != q.DB || w.cfg.DBEnv != q.DBEnv || !sameLabelValues(w.cfg.Labels, q.Labels))

		changed := !exists || !sameQueryConfig(w.cfg, q) || poolChanged || identityChanged
		if !changed {
			if wasPending {
				stats.Pending++
			} else {
				stats.Unchanged++
			}
			continue
		}

		if exists {
			wm.logger.Info("stopping changed query worker", "query", name)
			w.cancel()
			w.wg.Wait()
			if identityChanged && w.cfg.MetricName != "" {
				deleteWorkerRows(w.cfg, w.prevLabels)
				deleteQueryHealthMetrics(w.cfg.MetricName, w.cfg.DB)
			}
			stats.Restarted++
		} else {
			stats.Started++
		}

		var prevLabels *[]prometheus.Labels
		if exists && w.prevLabels != nil && !identityChanged {
			prevLabels = w.prevLabels
		} else {
			prevLabels = &[]prometheus.Labels{}
		}

		wg := &sync.WaitGroup{}
		wg.Add(1)
		ctx, cancel := context.WithCancel(wm.ctx)
		go startQueryWorker(ctx, wg, wm.logger, name, q, pEntry.db, prevLabels)

		newW := &worker{cancel: cancel, wg: wg, cfg: q, pool: pEntry, prevLabels: prevLabels}
		wm.mu.Lock()
		wm.workers[name] = newW
		wm.mu.Unlock()

		wm.logger.Info("started new/updated query worker", "query", name)
	}

	wm.mu.Lock()
	var toRemove []string
	for name := range wm.workers {
		if _, ok := queries[name]; !ok {
			toRemove = append(toRemove, name)
		}
	}
	wm.mu.Unlock()

	for _, name := range toRemove {
		wm.logger.Info("stopping removed query", "query", name)
		if wm.stopWorker(name) {
			stats.Stopped++
		}
	}

	return stats
}

// stopWorker останавливает и убирает воркер name, снимая метрику целиком
// (если её больше никто не использует) или только его строки (если
// использует — cloneQueriesForDB на несколько БД). Возвращает false, если
// воркера уже не было.
func (wm *workerManager) stopWorker(name string) bool {
	wm.mu.Lock()
	w, exists := wm.workers[name]
	if !exists {
		wm.mu.Unlock()
		return false
	}
	w.cancel()
	wg := w.wg
	metricName := w.cfg.MetricName
	stoppedCfg := w.cfg
	stoppedPrevLabels := w.prevLabels
	delete(wm.workers, name)

	sharedByOthers := false
	for otherName, otherW := range wm.workers {
		if otherName != name && otherW.cfg.MetricName == metricName {
			sharedByOthers = true
			break
		}
	}
	wm.mu.Unlock()

	wg.Wait()

	if metricName != "" {
		if !sharedByOthers {
			unregisterQueryMetric(metricName)
		} else {
			deleteWorkerRows(stoppedCfg, stoppedPrevLabels)
		}
		deleteQueryHealthMetrics(metricName, stoppedCfg.DB)
		// (query,db) уникален для этого воркера даже при клонировании —
		// в отличие от бизнес-метрики, sharedByOthers тут не имеет значения.
		configSchemaConflict.DeleteLabelValues(metricName, stoppedCfg.DB)
		runtimeSchemaMismatch.DeleteLabelValues(metricName, stoppedCfg.DB)
	}
	return true
}

func (wm *workerManager) stopAll() {
	wm.mu.Lock()
	workers := make([]*worker, 0, len(wm.workers))
	for _, w := range wm.workers {
		w.cancel()
		workers = append(workers, w)
	}
	wm.mu.Unlock()

	for _, w := range workers {
		w.wg.Wait()
	}
}
