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
// остановка, и решение о снятии Prometheus-метрики с регистрации. Сам
// воркер (worker.go) никогда не трогает Registry напрямую.
type workerManager struct {
	ctx    context.Context
	logger *slog.Logger

	mu          sync.Mutex
	reconcileMu sync.Mutex
	workers     map[string]*worker

	// lastQueries — снэпшот последнего применённого состояния запросов.
	// Нужен poolHealthChecker'у чтобы после реконнекта БД запустить
	// воркеры без повторного чтения файла конфига.
	lastQueries map[string]QueryConfig
}

func newWorkerManager(ctx context.Context, logger *slog.Logger) *workerManager {
	return &workerManager{
		ctx:         ctx,
		logger:      logger,
		workers:     make(map[string]*worker),
		lastQueries: make(map[string]QueryConfig),
	}
}

func (wm *workerManager) snapshotLastQueries() map[string]QueryConfig {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	out := make(map[string]QueryConfig, len(wm.lastQueries))
	for k, v := range wm.lastQueries {
		out[k] = v
	}
	return out
}

// reconcile приводит воркеры в соответствие с queries, используя снэпшот
// доступных пулов и резолвленное имя БД по умолчанию.
func (wm *workerManager) reconcile(queries map[string]QueryConfig, pools map[string]*dbPool, defaultDB string) {
	// Сериализует reconcile сам с собой — reload() и poolHealthChecker()
	// вызывают его из разных горутин.
	wm.reconcileMu.Lock()
	defer wm.reconcileMu.Unlock()

	wm.mu.Lock()
	currentWorkers := make(map[string]*worker, len(wm.workers))
	for k, v := range wm.workers {
		currentWorkers[k] = v
	}
	wm.lastQueries = make(map[string]QueryConfig, len(queries))
	for k, v := range queries {
		wm.lastQueries[k] = v
	}
	wm.mu.Unlock()

	for name, q := range queries {
		if q.DB == "" {
			if defaultDB == "" {
				wm.logger.Error("query has no db and no default_db is set", "query", name)
				continue
			}
			q.DB = defaultDB
		}

		pEntry, ok := pools[q.DB]
		if !ok || pEntry == nil || pEntry.db == nil {
			wm.logger.Error("db not available for query", "query", name, "db", q.DB)
			continue
		}
		q.DBEnv = pEntry.cfg.Env

		w, exists := currentWorkers[name]

		// Пул этой БД пересоздан (например сменился url) — старая ссылка
		// на *sql.DB уже недействительна, даже если сам query не менялся.
		poolChanged := exists && w.pool != pEntry

		// Смена набора имён лейблов/value_column меняет схему GaugeVec.
		// Prometheus не допускает regenerate схемы под тем же именем даже
		// после Unregister (Registry хранит dimHashesByName на весь срок
		// жизни процесса) — поэтому эти два поля откатываем к уже
		// зарегистрированным, а остальные изменения (SQL, DB, pool и т.д.)
		// применяем как обычно.
		schemaChanged := exists &&
			(w.cfg.ValueColumn != q.ValueColumn || !sameLabelKeys(w.cfg.Labels, q.Labels))
		if schemaChanged {
			wm.logger.Error(
				"labels or value_column changed for a running query — Prometheus does not support "+
					"changing a metric's label schema without a process restart; applying every other "+
					"change (sql/db/timeout/interval) but keeping the previous label schema until restart",
				"query", name, "db", q.DB)
			q.Labels = w.cfg.Labels
			q.ValueColumn = w.cfg.ValueColumn
		}

		// DB/env/значение лейбла изменились (набор имён — тот же, иначе
		// поймали бы это выше) — данные переезжают на другую time series,
		// старую нужно удалить явно, иначе она зависнет в GaugeVec навсегда.
		identityChanged := exists &&
			(w.cfg.DB != q.DB || w.cfg.DBEnv != q.DBEnv || !sameLabelValues(w.cfg.Labels, q.Labels))

		changed := !exists || !sameQueryConfig(w.cfg, q) || poolChanged || identityChanged
		if !changed {
			continue
		}

		if exists {
			wm.logger.Info("stopping changed query worker", "query", name)
			w.cancel()
			w.wg.Wait()
			if identityChanged && w.cfg.MetricName != "" {
				deleteWorkerRows(w.cfg, w.prevLabels)
			}
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
	for name, w := range wm.workers {
		if _, ok := queries[name]; !ok {
			wm.logger.Info("stopping removed query", "query", name)
			w.cancel()
			wg := w.wg
			metricName := w.cfg.MetricName
			stoppedCfg := w.cfg
			stoppedPrevLabels := w.prevLabels
			delete(wm.workers, name)

			// Другой воркер может делить это же MetricName (cloneQueriesForDB
			// на несколько БД) — тогда снести метрику целиком нельзя.
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
			}
			wm.mu.Lock()
		}
	}
	wm.mu.Unlock()
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
