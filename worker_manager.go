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
	pool       *dbPool // пул к которому привязан воркер — см. poolChanged в reconcile
	prevLabels *[]prometheus.Labels
}

// workerManager — единственный владелец жизненного цикла воркеров запросов:
// старт, остановка, и решение о том, когда изменение конфига требует
// рестарта. Это также единственное место, которое решает когда Prometheus-
// метрику можно снять с регистрации целиком, а когда нужно удалить только
// строки конкретного воркера — сам воркер (worker.go) никогда не трогает
// Registry lifecycle напрямую.
type workerManager struct {
	ctx    context.Context
	logger *slog.Logger

	mu          sync.Mutex
	reconcileMu sync.Mutex
	workers     map[string]*worker

	// lastQueries — снэпшот последнего применённого желаемого состояния
	// запросов. Нужен poolHealthChecker'у в app.go, чтобы после успешного
	// реконнекта БД запустить воркеры для неё без повторного чтения файла
	// конфига.
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

// reconcile приводит запущенные воркеры в соответствие с желаемым состоянием
// queries, используя переданный снэпшот доступных пулов и резолвленное имя
// БД по умолчанию.
func (wm *workerManager) reconcile(queries map[string]QueryConfig, pools map[string]*dbPool, defaultDB string) {
	// Без этого лока reload() и poolHealthChecker() могут вызвать reconcile
	// параллельно: обе горутины одновременно видят отсутствие воркера для
	// одного query name, обе стартуют свой воркер, один теряется без cancel.
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

		// DBEnv резолвится здесь, не при загрузке конфига — только тут известна
		// связка "запрос → его пул".
		q.DBEnv = pEntry.cfg.Env

		w, exists := currentWorkers[name]

		// poolChanged: тот же query config, но пул этой БД был пересоздан
		// (например изменился url или пароль). Без этой проверки воркер
		// продолжал бы держать ссылку на *sql.DB который уже закрыт извне —
		// все последующие тики падали бы с "sql: database is closed", хотя
		// сам query не менялся вообще.
		poolChanged := exists && w.pool != pEntry

		// schemaChanged: изменился НАБОР ИМЁН статических лейблов или
		// value_column — то, что определяет схему GaugeVec.
		schemaChanged := exists &&
			(w.cfg.ValueColumn != q.ValueColumn || !sameLabelKeys(w.cfg.Labels, q.Labels))

		if schemaChanged {
			// Prometheus не позволяет поменять схему лейблов уже
			// зарегистрированной метрики без пересоздания процесса —
			// Registry.Unregister() намеренно не чистит внутренний
			// dimHashesByName ("must be consistent throughout the lifetime
			// of a program"), поэтому повторная регистрация под тем же
			// именем с ДРУГИМ набором лейблов продолжает падать даже после
			// Unregister. Раньше здесь всё равно делался unregister+restart:
			// в лучшем случае query оставался постоянно сломанным (ошибка
			// регистрации на каждый запуск) вплоть до рестарта Pod'а, а в
			// худшем — несогласованно ломал метрику для ДРУГОГО воркера,
			// который случайно делит с этим то же MetricName (multi-DB
			// клонирование), но сам ещё не увидел это изменение схемы.
			// Честнее явно отказаться применять именно эту часть изменения:
			// старый воркер продолжает работать со старой схемой, а
			// исправить это может только рестарт процесса.
			wm.logger.Error(
				"labels or value_column changed for a running query — Prometheus does not support "+
					"changing a metric's label schema without a process restart; keeping the previous "+
					"version running until the process is restarted",
				"query", name, "db", q.DB)
			continue
		}

		// identityChanged: DB, env или ЗНАЧЕНИЕ статического лейбла
		// изменились (при неизменном наборе имён лейблов — это уже
		// проверено выше). Такое изменение не трогает схему метрики, но
		// перемещает данные на другую time series — старая строка больше
		// никогда не будет перезаписана новым воркером и должна быть
		// удалена явно, иначе она останется висеть в GaugeVec навсегда.
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
			// Переиспользуем prevLabels только если identity не менялась —
			// иначе в нём остались лейблы под старой identity и
			// reconciliation попытается Delete() строки, которые уже не
			// совпадают с текущим набором (для multi-row) либо просто
			// бессмысленны (для single-value, где prevLabels не используется).
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

			// Проверяем не использует ли ЕЩЁ ЖИВОЙ воркер тот же MetricName —
			// случай cloneQueriesForDB, когда один файл метрик подключён для
			// нескольких БД. Если убрать только одну БД из include_defaults,
			// нельзя снести метрику целиком: воркер для второй БД продолжает
			// в неё писать.
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

// stopAll останавливает все воркеры и дожидается их завершения.
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
