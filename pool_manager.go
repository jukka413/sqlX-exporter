package main

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// dbPool связывает *sql.DB с конфигом, из которого он был создан.
type dbPool struct {
	cfg DBConfig
	db  *sql.DB
}

// poolManager — единственный владелец подключений к БД в этом приложении.
// Это единственное место, которое имеет право вызывать sql.DB.Close().
// pools, failedPools и generation всегда мутируются вместе, под одним локом,
// в applyConfig() и commitReconnect(). Это не случайность, а то, что закрывает
// P0.1 из ревью: reconnect-попытка снимает (generation, failedPools) единым
// атомарным снэпшотом через snapshotForReconnect(), дозванивается ВНЕ лока
// (может занимать секунды), и перепроверяет generation И конкретный DBConfig
// перед коммитом. Раньше generation увеличивался в начале reload(), ДО того
// как buildPools вообще начинал дозваниваться — это оставляло реальное окно,
// в котором health-checker мог снять снэпшот УЖЕ С НОВЫМ generation, но ещё
// СО СТАРЫМИ failedPools/queriesCfg (если reload как раз между "увеличил
// generation" и "закоммитил новое состояние"), и проверка "не устарело ли"
// проходила бы успешно на паре из разных момента времени. Теперь generation
// увеличивается СТРОГО в том же критическом участке, что и сам коммит pools/
// failedPools — рассинхронизация этих двух вещей структурно невозможна.
type poolManager struct {
	ctx    context.Context
	logger *slog.Logger

	mu          sync.Mutex
	pools       map[string]*dbPool
	failedPools map[string]DBConfig
	generation  uint64
}

func newPoolManager(ctx context.Context, logger *slog.Logger) *poolManager {
	return &poolManager{
		ctx:         ctx,
		logger:      logger,
		pools:       make(map[string]*dbPool),
		failedPools: make(map[string]DBConfig),
	}
}

// snapshotPools возвращает копию текущих активных пулов.
func (pm *poolManager) snapshotPools() map[string]*dbPool {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	out := make(map[string]*dbPool, len(pm.pools))
	for k, v := range pm.pools {
		out[k] = v
	}
	return out
}

// snapshotForReconnect возвращает согласованную пару (generation, failedPools).
// Согласованность гарантируется тем, что оба значения читаются под одним
// локом и нигде в коде не мутируются раздельно — см. комментарий у типа.
func (pm *poolManager) snapshotForReconnect() (uint64, map[string]DBConfig) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	failed := make(map[string]DBConfig, len(pm.failedPools))
	for k, v := range pm.failedPools {
		failed[k] = v
	}
	return pm.generation, failed
}

// dial открывает и пингует соединение для одной БД. Не трогает состояние
// poolManager — вызывающий сам решает что делать с результатом. Дозвон
// намеренно вынесен из-под лока (может занимать секунды на каждую БД).
func (pm *poolManager) dial(name string, dbCfg DBConfig) (*sql.DB, bool) {
	driver := strings.TrimSpace(dbCfg.Driver)
	if driver == "" {
		pm.logger.Error("db driver is required", "db", name)
		dbConnectionErrors.WithLabelValues(name).Inc()
		return nil, false
	}
	dbCfg.Driver = driver

	db, err := openAndPingDB(pm.ctx, dbCfg, pm.logger)
	if err != nil {
		pm.logger.Error("failed to open db", "db", name, "driver", dbCfg.Driver, "error", err)
		dbConnectionErrors.WithLabelValues(name).Inc()
		return nil, false
	}
	pm.logger.Info("connected to database", "db", name, "driver", dbCfg.Driver)
	return db, true
}

// applyConfig реконсилирует пулы с только что загруженным набором конфигов БД.
// Коммитит pools/failedPools/generation атомарно в одном критическом участке
// (см. комментарий у типа). Сам дозвон идёт ВНЕ лока — под локом только
// работа с map.
//
// Возвращает:
//   - toClose — пулы, которые были заменены или удалены и должны быть закрыты
//     ВЫЗЫВАЮЩИМ после того как он убедится что ни один воркер их больше не
//     использует (см. app.reload — реконсиляция воркеров обязана произойти
//     до Close()).
//   - removedDBs — имена БД, которые пропали из конфига целиком (нужно
//     вызывающему чтобы дополнительно почистить их pool-метрики).
func (pm *poolManager) applyConfig(dbs map[string]DBConfig) (toClose map[string]*dbPool, removedDBs []string) {
	toClose = make(map[string]*dbPool)
	current := pm.snapshotPools()

	newPools := make(map[string]*dbPool, len(dbs))
	newFailed := make(map[string]DBConfig)

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
				// указатель, который воркеры могут читать из другой горутины
				// без синхронизации. Новая обёртка с тем же *sql.DB естественно
				// сработает через poolChanged в workerManager.reconcile —
				// воркеры перезапустятся и подхватят новое значение env.
				newPools[name] = &dbPool{cfg: dbCfg, db: old.db}
				continue
			}
			newPools[name] = old
			continue
		}

		db, ok := pm.dial(name, dbCfg)
		if !ok {
			// Не удалось создать новый пул — старый пул НЕ закрываем.
			// Воркеры продолжают работать с существующим подключением.
			if exists {
				newPools[name] = old
			}
			newFailed[name] = dbCfg
			continue
		}

		if exists && old != nil && old.db != nil {
			toClose[name] = old
		}
		newPools[name] = &dbPool{cfg: dbCfg, db: db}
	}

	pm.mu.Lock()
	for name, p := range pm.pools {
		if _, stillInCfg := dbs[name]; !stillInCfg {
			removedDBs = append(removedDBs, name)
			if p != nil && p.db != nil {
				toClose[name] = p
			}
		}
	}
	pm.pools = newPools
	for name, cfg := range newFailed {
		pm.failedPools[name] = cfg
	}
	for name := range pm.failedPools {
		if _, connected := newPools[name]; connected {
			delete(pm.failedPools, name) // успешно (пере)подключились в этом же applyConfig
			continue
		}
		if _, stillInCfg := dbs[name]; !stillInCfg {
			delete(pm.failedPools, name) // БД убрали из конфига целиком
		}
	}
	pm.generation++
	pm.mu.Unlock()

	return toClose, removedDBs
}

// commitReconnect проверяет завершённую попытку дозвона против ТЕКУЩЕГО
// состояния перед применением и возвращает пул, который нужно закрыть в
// результате замены (если есть). ok=false означает что попытка устарела и
// должна быть отброшена — тогда вызывающий обязан сам закрыть переданный db.
func (pm *poolManager) commitReconnect(name string, dbCfg DBConfig, expectedGen uint64, db *sql.DB) (oldPool *dbPool, ok bool) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.generation != expectedGen {
		return nil, false
	}
	// Дополнительная проверка сверх generation: конкретная запись в
	// failedPools для этой БД должна быть именно той, для которой мы
	// дозванивались. Структурно это уже гарантировано проверкой generation
	// (оба мутируются только вместе), но сравнение по значению здесь ничего
	// не стоит и защищает от будущих рефакторингов, которые могли бы это
	// инвариант ненамеренно нарушить.
	current, stillFailed := pm.failedPools[name]
	if !stillFailed || current != dbCfg {
		return nil, false
	}

	oldPool = pm.pools[name]
	delete(pm.failedPools, name)
	pm.pools[name] = &dbPool{cfg: dbCfg, db: db}
	return oldPool, true
}

// closePools закрывает переданный набор пулов. Единственное место в коде,
// которое вызывает sql.DB.Close() — весь остальной код должен просить об
// этом poolManager, а не делать это самостоятельно.
func (pm *poolManager) closePools(pools map[string]*dbPool) {
	for name, p := range pools {
		if p != nil && p.db != nil {
			pm.logger.Info("closing pool", "db", name)
			_ = p.db.Close()
		}
	}
}

// closeAll закрывает все текущие пулы — используется при graceful shutdown.
func (pm *poolManager) closeAll() {
	pm.closePools(pm.snapshotPools())
}

// deletePoolMetrics чистит pool-gauge метрики для перечисленных БД — иначе
// они остаются на /metrics навсегда с последним известным значением после
// того как БД убрали из конфига.
func (pm *poolManager) deletePoolMetrics(names []string) {
	for _, name := range names {
		dbPoolAcquired.DeleteLabelValues(name)
		dbPoolIdle.DeleteLabelValues(name)
		dbPoolTotal.DeleteLabelValues(name)
	}
}

// metricsUpdater периодически публикует метрики состояния пулов (acquired/
// idle/total connections) для всех текущих БД.
func (pm *poolManager) metricsUpdater(period time.Duration) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-pm.ctx.Done():
			return
		case <-ticker.C:
			for name, p := range pm.snapshotPools() {
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
