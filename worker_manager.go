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
	Started   int
	Restarted int
	Stopped   int
	Unchanged int
	Pending   int // заморожено на старом конфиге, потому что БД pending
}

// reconcile приводит воркеры в соответствие с queries. revision — то же
// число, которым в этом reload проштампован poolManager. pendingDBs — имена
// БД, чей новый желаемый конфиг ещё не применился (см. poolManager.pendingDBs).
func (wm *workerManager) reconcile(revision uint64, queries map[string]QueryConfig, pools map[string]*dbPool, pendingDBs map[string]struct{}, defaultDB string) ReconcileStats {
	wm.reconcileMu.Lock()
	defer wm.reconcileMu.Unlock()

	var stats ReconcileStats

	wm.mu.Lock()
	// reconcileMu сериализует вызовы, но не их порядок по revision —
	// reconcile(R-1) может физически выполниться после reconcile(R), если
	// health-checker снял свой снэпшот раньше, а дошёл до вызова позже.
	// Без этой проверки такой запоздавший reconcile тихо откатил бы
	// уже применённое состояние.
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

		// pendingDBs: новый кандидат конфига для этой БД не подключился в
		// этом reload — pools[q.DB], если он вообще есть, это УДЕРЖАННЫЙ
		// старый пул, а не то, что реально хотел применить конфиг. Нельзя
		// запускать НОВЫЙ QueryConfig на пуле, который физически ведёт к
		// другой БД (URL мог смениться на другой хост/кластер) — иначе
		// новый SQL выполнится не там, где рассчитывал оператор.
		wasPending := false
		if _, pending := pendingDBs[q.DB]; pending {
			wasPending = true
			if !exists {
				// Этот запрос никогда раньше не выполнялся на этой БД —
				// нет "последнего рабочего состояния", к которому можно
				// откатиться. Просто не стартуем, пока БД не подключится.
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
		// метрики без рестарта процесса. Вместо гибрида (новый SQL со старым
		// value_column, который тут же упал бы с "column not found") целиком
		// замораживаем QueryConfig на последнем рабочем состоянии — не
		// только Labels/ValueColumn, но и SQL/DB/timeout. Проверка идёт до
		// резолва пула: замороженный q.DB может отличаться от нового.
		schemaChanged := exists &&
			(w.cfg.ValueColumn != q.ValueColumn || !sameLabelKeys(w.cfg.Labels, q.Labels))
		if schemaChanged {
			wm.logger.Error(
				"labels or value_column changed for a running query — Prometheus does not support "+
					"changing a metric's label schema without a process restart; the entire query "+
					"config is frozen at its last working state until restart",
				"query", name, "db", q.DB)
			if w.cfg.MetricName != "" {
				queryHealthSchemaConflict.WithLabelValues(w.cfg.MetricName).Set(1)
			}
			q = w.cfg
		} else if exists && w.cfg.MetricName != "" {
			queryHealthSchemaConflict.DeleteLabelValues(w.cfg.MetricName)
		}

		pEntry, ok := pools[q.DB]
		if !ok || pEntry == nil || pEntry.db == nil {
			wm.logger.Error("db not available for query", "query", name, "db", q.DB)
			// Останавливаем, а не оставляем на старой БД — её пул может
			// быть закрыт этим же reload'ом через пару строк.
			if wm.stopWorker(name) {
				stats.Stopped++
			}
			continue
		}
		q.DBEnv = pEntry.cfg.Env

		poolChanged := exists && w.pool != pEntry

		// DB/env/значение лейбла изменились — данные переезжают на другую
		// time series, старую нужно удалить явно.
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
			queryHealthSchemaConflict.DeleteLabelValues(metricName)
		} else {
			deleteWorkerRows(stoppedCfg, stoppedPrevLabels)
		}
		// (metricName, db) уникален для этого воркера даже при клонировании
		// на несколько БД — в отличие от бизнес-метрики, sharedByOthers
		// здесь не имеет значения.
		deleteQueryHealthMetrics(metricName, stoppedCfg.DB)
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
