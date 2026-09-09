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

	// lastQueries — снэпшот последнего применённого состояния запросов, с
	// revision (см. app.revision и комментарий у poolManager), которым он
	// был проштампован. poolHealthChecker сверяет эту revision с той, что
	// у poolManager, прежде чем реконсилировать воркеры после реконнекта —
	// если они разошлись, значит health-checker застал reload() между тем,
	// как обновился пул, и тем, как обновились воркеры, и снэпшот здесь
	// ещё не отражает актуальное состояние.
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

// snapshotLastQueries возвращает последнее применённое состояние запросов
// вместе с revision, которым оно проштамповано.
func (wm *workerManager) snapshotLastQueries() (map[string]QueryConfig, uint64) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	out := make(map[string]QueryConfig, len(wm.lastQueries))
	for k, v := range wm.lastQueries {
		out[k] = v
	}
	return out, wm.lastQueriesRevision
}

// reconcile приводит воркеры в соответствие с queries, используя снэпшот
// доступных пулов и резолвленное имя БД по умолчанию. revision — то же
// число, которым в этом же reload проштампован poolManager (см. app.revision).
func (wm *workerManager) reconcile(revision uint64, queries map[string]QueryConfig, pools map[string]*dbPool, defaultDB string) {
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

		pEntry, ok := pools[q.DB]
		if !ok || pEntry == nil || pEntry.db == nil {
			wm.logger.Error("db not available for query", "query", name, "db", q.DB)
			// Если для этой query уже был воркер — его нужно остановить, а не
			// оставить работать на прежней БД. Конфиг для этой query теперь
			// указывает на БД, к которой нет пула — держать воркер на СТАРОМ
			// пуле нельзя: та БД могла быть убрана из конфига этим же reload
			// и её пул через пару строк закроет pm.applyConfig, а воркер
			// продолжил бы получать "sql: database is closed".
			wm.stopWorker(name)
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
	var toRemove []string
	for name := range wm.workers {
		if _, ok := queries[name]; !ok {
			toRemove = append(toRemove, name)
		}
	}
	wm.mu.Unlock()

	for _, name := range toRemove {
		wm.logger.Info("stopping removed query", "query", name)
		wm.stopWorker(name)
	}
}

// stopWorker останавливает и убирает воркер name из wm.workers, снимая
// метрику целиком (если её больше никто не использует) или только его
// собственные строки (если использует — cloneQueriesForDB на несколько БД).
//
// Общий путь для двух случаев: запрос исчез из конфига целиком, и запрос
// остался, но резолвится в БД, к которой сейчас нет пула (см. reconcile —
// без этого вызова там воркер остался бы работать на СТАРОЙ, уже не
// актуальной БД, и рисковал держать ссылку на пул, который вот-вот закроют).
func (wm *workerManager) stopWorker(name string) {
	wm.mu.Lock()
	w, exists := wm.workers[name]
	if !exists {
		wm.mu.Unlock()
		return
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
	}
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
