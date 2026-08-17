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
	pool       *dbPool // пул к которому привязан воркер — см. комментарий в reconcileWorkers
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
	// Защищается тем же mu — читать напрямую без лока это data race с записью в reload().
	defaultDB string

	// reconcileMu гарантирует что reconcileWorkers не выполняется параллельно
	// сам с собой. Вызывается из двух независимых горутин — reload() (по сигналу
	// watcher) и poolHealthChecker() (по таймеру) — без этого лока возможен сценарий:
	// обе горутины одновременно видят отсутствие воркера для одного query name,
	// обе стартуют свой воркер, один из них теряется без cancel — утечка горутины
	// и дублирующиеся запросы к БД для одной и той же метрики.
	reconcileMu sync.Mutex

	// reloadMu гарантирует что reload() не выполняется параллельно сам с собой.
	// Без этого лока watcher debounce мог бы вызвать reload() ещё раз пока
	// предыдущий вызов (open/ping нескольких БД, до 5с на каждую) ещё не завершился —
	// два конкурентных reload() читают один и тот же a.pools как "текущий",
	// оба создают новые пулы, один из результатов теряется без Close(): утечка
	// соединения которая никогда не попадёт в toClose ни одного из двух вызовов.
	reloadMu sync.Mutex
}

// getDefaultDB безопасно читает defaultDB под локом.
func (a *app) getDefaultDB() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.defaultDB
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
	// Сериализуем reload() сам с собой — см. комментарий у reloadMu в типе app.
	a.reloadMu.Lock()
	defer a.reloadMu.Unlock()

	a.logger.Info("reloading configuration")

	newCfg, err := loadConfig(a.configPath)
	if err != nil {
		a.logger.Error("failed to load config", "error", err)
		return
	}

	for _, reason := range newCfg.skippedIncludes {
		a.logger.Info("include skipped (no include_defaults entry)", "detail", reason)
	}
	for _, reason := range newCfg.missingEnvVars {
		a.logger.Error("environment variable not set, substituted as empty string", "detail", reason)
	}
	// Коллизии имён database/query между инклюдами — не фатально, но должно
	// быть видно: метрика могла молча начать собирать данные с другой БД.
	for _, reason := range newCfg.overwritten {
		a.logger.Warn("config key overwritten by a later include", "detail", reason)
	}

	if err := validateDatabasesAndSettings(newCfg); err != nil {
		a.logger.Error("invalid config", "error", err)
		return
	}

	if removed := sanitizeQueries(&newCfg); len(removed) > 0 {
		for _, reason := range removed {
			a.logger.Error("skipping invalid query", "reason", reason)
		}
	}

	newPools, toClose, newFailed := a.buildPools(newCfg.Databases)

	// БД, которых больше нет в конфиге вообще (не изменились, а именно удалены),
	// buildPools никогда не видит — его аргумент это только новый набор БД.
	// Без этого шага такие пулы оставались бы в a.pools вечно: соединение не
	// закрывается, pool-метрики продолжают публиковаться для удалённой БД.
	a.mu.Lock()
	var removedDBs []string
	for name, p := range a.pools {
		if _, stillInCfg := newCfg.Databases[name]; !stillInCfg {
			removedDBs = append(removedDBs, name)
			if p != nil && p.db != nil {
				toClose[name] = p
			}
		}
	}
	for _, name := range removedDBs {
		delete(a.pools, name)
	}
	a.mu.Unlock()

	a.mu.Lock()
	for name, p := range newPools {
		a.pools[name] = p
		// Успешно подключились — эта БД больше не "failed", даже если была
		// в failedPools с прошлого reload. Без этой строки poolHealthChecker
		// продолжал бы переподключаться к уже живой БД и мог перезаписать
		// a.pools[name] вторым, лишним соединением.
		delete(a.failedPools, name)
	}
	for name, cfg := range newFailed {
		a.failedPools[name] = cfg
	}
	for name := range a.failedPools {
		if _, stillInCfg := newCfg.Databases[name]; !stillInCfg {
			delete(a.failedPools, name)
		}
	}
	a.queriesCfg = newCfg.Queries
	a.reconnectInterval = newCfg.Settings.DBReconnectIntervalDuration()
	a.defaultDB = newCfg.Settings.DefaultDB
	a.mu.Unlock()

	for name, p := range toClose {
		if p != nil && p.db != nil {
			a.logger.Info("closing old pool", "db", name)
			_ = p.db.Close()
		}
	}

	// Чистим pool-метрики удалённых БД — иначе их gauge остаются на /metrics
	// навсегда с последним известным значением.
	for _, name := range removedDBs {
		dbPoolAcquired.DeleteLabelValues(name)
		dbPoolIdle.DeleteLabelValues(name)
		dbPoolTotal.DeleteLabelValues(name)
	}

	a.reconcileWorkers(newCfg.Queries)

	a.logger.Info("reload complete")
}

// =========================================================================
// buildPools
// =========================================================================

// buildPools возвращает три map:
//   - newPools  — пулы готовые к использованию (переиспользованные и новые)
//   - toClose   — старые пулы которые нужно закрыть (заменены новым подключением)
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
			if exists && old.cfg.Env != dbCfg.Env {
				// Только Env изменился — реконнект не нужен (тот же *sql.DB),
				// но мутировать old.cfg.Env "на месте" нельзя: это shared
				// указатель, который reconcileWorkers может читать из другой
				// горутины без синхронизации — гонка данных. Вместо этого
				// создаём новую обёртку dbPool с тем же соединением. Смена
				// указателя естественным образом сработает через уже
				// существующую проверку poolChanged в reconcileWorkers —
				// воркеры, использующие эту БД, перезапустятся и подхватят
				// новое значение q.DBEnv, без единой лишней строчки кода
				// специально под "env изменился".
				newPools[name] = &dbPool{cfg: dbCfg, db: old.db}
				continue
			}
			newPools[name] = old
			continue
		}

		db, ok := a.createAndPingPool(name, dbCfg)
		if !ok {
			// Не удалось создать новый пул — старый пул НЕ закрываем.
			// Воркеры продолжают работать с существующим подключением.
			if exists {
				newPools[name] = old
			}
			failed[name] = dbCfg
			continue
		}

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

	db, err := openAndPingDB(a.ctx, dbCfg, a.logger)
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
	// Без этого лока reload() и poolHealthChecker() могут вызвать
	// reconcileWorkers параллельно — см. комментарий у reconcileMu в типе app.
	a.reconcileMu.Lock()
	defer a.reconcileMu.Unlock()

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

	defaultDB := a.getDefaultDB()

	for name, q := range queries {
		if q.DB == "" {
			if defaultDB == "" {
				a.logger.Error("query has no db and no default_db is set", "query", name)
				continue
			}
			q.DB = defaultDB
		}

		pEntry, ok := currentPools[q.DB]
		if !ok || pEntry == nil || pEntry.db == nil {
			a.logger.Error("db not available for query", "query", name, "db", q.DB)
			continue
		}

		// DBEnv резолвится здесь, не при загрузке конфига — только тут известна
		// связка "запрос → его пул". Должно быть выставлено ДО sameQueryConfig
		// ниже, иначе сравнение всегда считало бы конфиг изменившимся
		// (у нового q.DBEnv ещё пусто, а у старого воркера уже проставлено).
		q.DBEnv = pEntry.cfg.Env

		w, exists := currentWorkers[name]

		// poolChanged: тот же query config, но пул этой БД был пересоздан
		// (например изменился url или пароль). Без этой проверки воркер
		// продолжал бы держать ссылку на *sql.DB который reload() уже закрыл
		// в toClose — все последующие тики этого воркера падали бы с
		// "sql: database is closed", хотя по факту query не менялся вообще.
		poolChanged := exists && w.pool != pEntry

		// schemaChanged: изменились Labels или ValueColumn — это то, что
		// определяет схему лейблов GaugeVec. Если рестартовать воркер но не
		// снять старую метрику, getOrCreateQueryMetric вернёт закешированный
		// коллектор со старой схемой, а metric.With(новые_лейблы) запаникует
		// (GaugeVec.With паникует там, где GetMetricWith вернул бы ошибку).
		schemaChanged := exists &&
			(w.cfg.ValueColumn != q.ValueColumn || !sameLabels(w.cfg.Labels, q.Labels))

		// envChanged: у БД этого запроса поменялось значение env (например
		// БД переклассифицировали test → prod). Схема лейблов не меняется
		// (env как лейбл присутствует всегда), меняется только значение —
		// поэтому schemaChanged это не ловит, а без явной обработки старая
		// строка с прежним env остаётся висеть в метрике навсегда, потому
		// что новый воркер пишет уже под другим значением env.
		envChanged := exists && w.cfg.DBEnv != q.DBEnv

		changed := !exists || !sameQueryConfig(w.cfg, q) || poolChanged || envChanged
		if !changed {
			continue
		}

		if exists {
			a.logger.Info("stopping changed query worker", "query", name)
			w.cancel()
			w.wg.Wait()

			if schemaChanged && w.cfg.MetricName != "" {
				// Клоны одного файла метрик на несколько БД (cloneQueriesForDB)
				// меняют Labels/ValueColumn синхронно в одном и том же reload —
				// поэтому безопасно снять метрику сразу, не дожидаясь остальных
				// клонов: они тоже увидят schemaChanged и просто не найдут метрику
				// в кэше, getOrCreateQueryMetric создаст её заново с новой схемой.
				unregisterQueryMetric(w.cfg.MetricName)
			} else if envChanged && w.cfg.MetricName != "" {
				// Снести всю метрику нельзя (другие БД/значения env могут
				// её ещё использовать), но собственные строки этого воркера
				// со старым env нужно убрать явно — иначе они останутся
				// висеть в GaugeVec навсегда, потому что новый воркер пишет
				// уже под другим значением env и никогда их не перезапишет.
				deleteWorkerRows(w.cfg, w.prevLabels)
			}
		}

		var prevLabels *[]prometheus.Labels
		if exists && w.prevLabels != nil && !schemaChanged && !envChanged {
			// Переиспользуем prevLabels только если ни схема лейблов, ни
			// значение env не менялись — иначе в нём остались лейблы под
			// старым env и reconciliation попытается Delete() строки,
			// которые уже не совпадают с текущим набором.
			prevLabels = w.prevLabels
		} else {
			prevLabels = &[]prometheus.Labels{}
		}

		wg := &sync.WaitGroup{}
		wg.Add(1)
		ctx, cancel := context.WithCancel(a.ctx)

		go startQueryWorker(ctx, wg, a.logger, name, q, pEntry.db, prevLabels)

		newW := &worker{cancel: cancel, wg: wg, cfg: q, pool: pEntry, prevLabels: prevLabels}

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
			metricName := w.cfg.MetricName
			stoppedCfg := w.cfg
			stoppedPrevLabels := w.prevLabels
			delete(a.workers, name)

			// Проверяем не использует ли ЕЩЁ ЖИВОЙ воркер тот же MetricName —
			// случай cloneQueriesForDB, когда один файл метрик подключён для
			// нескольких БД. Если убрать только одну БД из include_defaults,
			// нельзя снести метрику целиком: воркер для второй БД продолжает в неё писать.
			sharedByOthers := false
			for otherName, otherW := range a.workers {
				if otherName != name && otherW.cfg.MetricName == metricName {
					sharedByOthers = true
					break
				}
			}

			a.mu.Unlock()
			wg.Wait()

			if metricName == "" {
				a.mu.Lock()
				continue
			}

			if !sharedByOthers {
				// Метрика больше никому не нужна — сносим целиком.
				unregisterQueryMetric(metricName)
			} else {
				// Метрику снести нельзя — с ней ещё работает другой воркер
				// (например для другой БД из того же include_defaults списка).
				// Но СВОИ СТРОКИ этого воркера всё равно нужно явно удалить —
				// иначе они останутся висеть в GaugeVec навсегда со старым
				// значением. Это конкретно тот случай когда БД временно убрали
				// из include_defaults списка (worker-ключ стал без суффикса __db),
				// затем вернули обратно (ключ снова с суффиксом) без полного
				// рестарта процесса: старый воркер с ключом без суффикса
				// останавливается, но новый воркер с суффиксом стартует под
				// ДРУГИМ ключом карты a.workers и ничего не знает о старых
				// строках прежнего воркера — они не перезаписываются и
				// выглядят как "два значения метрики одновременно".
				deleteWorkerRows(stoppedCfg, stoppedPrevLabels)
			}
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
