package main

import (
	"context"
	"database/sql"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// intPtrEqual сравнивает *int по значению, а не по адресу — прямое `!=`
// на двух *int сравнивало бы указатели: перепарсенный при каждом reload
// YAML всегда аллоцирует новый *int, даже если число в нём не изменилось,
// и наивное сравнение считало бы это изменением каждый раз.
func intPtrEqual(a, b *int) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// dbPool связывает *sql.DB с конфигом, из которого он был создан.
type dbPool struct {
	cfg DBConfig
	db  *sql.DB
}

// poolManager — единственный владелец подключений к БД; только он вызывает
// sql.DB.Close(). pools, failedPools и revision всегда мутируются вместе под
// одним локом — reconnect-попытка, снявшая их единым снэпшотом, не может
// увидеть рассинхронизированную пару при повторной проверке перед коммитом.
//
// revision присваивает app.reload() (см. app.revision), тем же значением,
// что и workerManager в этом же вызове — оба менеджера штампуются одним
// числом за один reload, а не независимо друг от друга.
type poolManager struct {
	ctx    context.Context
	logger *slog.Logger

	// transitionMu сериализует applyConfig и commitReconnect целиком —
	// от снэпшота до коммита, не только доступ к map (для этого есть mu).
	// Без него коммит applyConfig (полная замена pools) мог бы затереть
	// pool, который commitReconnect только что восстановил параллельно.
	transitionMu sync.Mutex

	mu          sync.Mutex
	pools       map[string]*dbPool
	failedPools map[string]DBConfig
	revision    uint64
}

func newPoolManager(ctx context.Context, logger *slog.Logger) *poolManager {
	return &poolManager{
		ctx:         ctx,
		logger:      logger,
		pools:       make(map[string]*dbPool),
		failedPools: make(map[string]DBConfig),
	}
}

func (pm *poolManager) snapshotPools() map[string]*dbPool {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	out := make(map[string]*dbPool, len(pm.pools))
	for k, v := range pm.pools {
		out[k] = v
	}
	return out
}

// snapshotForReconnect возвращает revision и failedPools как согласованную
// пару — см. комментарий у типа.
func (pm *poolManager) snapshotForReconnect() (uint64, map[string]DBConfig) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	failed := make(map[string]DBConfig, len(pm.failedPools))
	for k, v := range pm.failedPools {
		failed[k] = v
	}
	return pm.revision, failed
}

// pendingDBs возвращает имена БД, для которых текущий пул НЕ соответствует
// последнему желаемому конфигу (новый кандидат не подключился, работает
// удержанный старый пул, если он был). workerManager использует это чтобы
// не применять новый QueryConfig к запросам на такие БД — иначе новый SQL
// мог бы выполниться на пуле, который физически ведёт к другой БД.
func (pm *poolManager) pendingDBs() map[string]struct{} {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	out := make(map[string]struct{}, len(pm.failedPools))
	for name := range pm.failedPools {
		out[name] = struct{}{}
	}
	return out
}

// dial открывает и пингует соединение. Не трогает состояние poolManager —
// вызывающий сам решает что делать с результатом.
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

// applyConfig реконсилирует пулы с новым набором конфигов БД, штампуя
// результат переданным revision (см. комментарий у типа — тем же значением
// в этом же reload штампуется workerManager).
//
// toClose — пулы к закрытию; вызывающий обязан сначала остановить воркеры,
// использующие эти пулы, и только потом закрыть их (см. app.reload).
// removedDBs — БД, пропавшие из конфига целиком.
func (pm *poolManager) applyConfig(revision uint64, dbs map[string]DBConfig) (toClose map[string]*dbPool, removedDBs []string) {
	pm.transitionMu.Lock()
	defer pm.transitionMu.Unlock()

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
			!intPtrEqual(old.cfg.MaxConns, dbCfg.MaxConns) ||
			!intPtrEqual(old.cfg.MaxIdleConns, dbCfg.MaxIdleConns) ||
			old.cfg.MaxConnLifetime != dbCfg.MaxConnLifetime ||
			old.cfg.MaxConnIdleTime != dbCfg.MaxConnIdleTime

		if !needUpdate {
			if old.cfg.Env != dbCfg.Env {
				// Меняем только обёртку, не old.cfg.Env на месте — old может
				// читаться из другой горутины без синхронизации. Новый
				// указатель сработает через poolChanged в reconcile.
				newPools[name] = &dbPool{cfg: dbCfg, db: old.db}
				continue
			}
			newPools[name] = old
			continue
		}

		db, ok := pm.dial(name, dbCfg)
		if !ok {
			if exists {
				newPools[name] = old // старое соединение продолжает работать
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
	// Union pools ∪ failedPools, не только pools — БД, которая ни разу не
	// подключилась, существует только в failedPools. Проверка одного pools
	// пропускала бы такую БД при удалении из конфига: removedDBs её не
	// содержал бы, deletePoolMetrics никогда не вызывался бы для неё, и
	// app_db_up{db=...}=0 (выставленный когда-то давно неудачным dial)
	// оставался бы в реестре Prometheus навсегда.
	seen := make(map[string]struct{}, len(pm.pools)+len(pm.failedPools))
	for name := range pm.pools {
		seen[name] = struct{}{}
	}
	for name := range pm.failedPools {
		seen[name] = struct{}{}
	}
	for name := range seen {
		if _, stillInCfg := dbs[name]; stillInCfg {
			continue
		}
		removedDBs = append(removedDBs, name)
		if p, ok := pm.pools[name]; ok && p != nil && p.db != nil {
			toClose[name] = p
		}
	}
	// newFailed уже полон: applyConfig проходит по ВСЕМ БД желаемого
	// состояния каждый раз, поэтому мержить с прошлым failedPools не нужно.
	pm.pools = newPools
	pm.failedPools = newFailed
	pm.revision = revision
	pm.mu.Unlock()

	pm.updateDBUpMetrics()
	pm.updateDBConfigAppliedMetrics()

	return toClose, removedDBs
}

// updateDBUpMetrics публикует app_db_up для каждой известной БД — быстрый,
// немедленный сигнал сразу при (пере)подключении. Периодически значение
// дополнительно перепроверяется активным пингом (см. poolManager.activeHealthCheck),
// иначе "1" отсюда так и остался бы навсегда, даже если соединение потом тихо умрёт.
func (pm *poolManager) updateDBUpMetrics() {
	pools := pm.snapshotPools()
	_, failed := pm.snapshotForReconnect()
	for name := range pools {
		dbUp.WithLabelValues(name).Set(1)
	}
	for name := range failed {
		// БД может быть и в pools (старое соединение работает), и в failed
		// (новый кандидат конфига не подключился) — намеренное graceful
		// degradation в applyConfig. Метрика должна отражать "есть ли
		// рабочее соединение сейчас", не "применился ли последний кандидат".
		if _, hasWorkingPool := pools[name]; hasWorkingPool {
			continue
		}
		dbUp.WithLabelValues(name).Set(0)
	}
}

// updateDBConfigAppliedMetrics публикует app_db_config_applied. В отличие
// от updateDBUpMetrics, порядок здесь обратный: failed побеждает pools
// безусловно — раз БД в failedPools, значит последний желаемый конфиг НЕ
// применился, даже если старое соединение продолжает исправно работать.
func (pm *poolManager) updateDBConfigAppliedMetrics() {
	pools := pm.snapshotPools()
	_, failed := pm.snapshotForReconnect()
	for name := range pools {
		dbConfigApplied.WithLabelValues(name).Set(1)
	}
	for name := range failed {
		dbConfigApplied.WithLabelValues(name).Set(0)
	}
}

// commitReconnect проверяет попытку дозвона против текущего состояния перед
// применением. ok=false — попытка устарела, вызывающий сам закрывает db.
func (pm *poolManager) commitReconnect(name string, dbCfg DBConfig, expectedRevision uint64, db *sql.DB) (oldPool *dbPool, ok bool) {
	pm.transitionMu.Lock()
	defer pm.transitionMu.Unlock()

	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.revision != expectedRevision {
		return nil, false
	}
	current, stillFailed := pm.failedPools[name]
	if !stillFailed || current != dbCfg {
		return nil, false
	}

	oldPool = pm.pools[name]
	delete(pm.failedPools, name)
	pm.pools[name] = &dbPool{cfg: dbCfg, db: db}
	return oldPool, true
}

// closePools — единственное место, вызывающее sql.DB.Close().
func (pm *poolManager) closePools(pools map[string]*dbPool) {
	for name, p := range pools {
		if p != nil && p.db != nil {
			pm.logger.Info("closing pool", "db", name)
			_ = p.db.Close()
		}
	}
}

func (pm *poolManager) closeAll() {
	pm.closePools(pm.snapshotPools())
}

func (pm *poolManager) deletePoolMetrics(names []string) {
	for _, name := range names {
		dbPoolAcquired.DeleteLabelValues(name)
		dbPoolIdle.DeleteLabelValues(name)
		dbPoolTotal.DeleteLabelValues(name)
		dbUp.DeleteLabelValues(name)
		dbConfigApplied.DeleteLabelValues(name)
		dbConnectionErrors.DeleteLabelValues(name)
	}
}

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

// activeHealthCheck периодически пингует каждый активный пул и обновляет
// app_db_up по результату. Без этого app_db_up отражал бы только "когда-то
// подключились успешно" — sql.DB.Stats() (см. metricsUpdater) сообщает
// состояние пула соединений, а не то, отвечает ли сервер сейчас; тихо
// умершая сеть оставила бы app_db_up=1 бесконечно, пока какой-нибудь
// query не наткнётся на неё сам. Отдельная, более редкая горутина —
// в отличие от Stats() (чисто локальная операция), Ping — реальный
// сетевой запрос к БД, не хочется слать его так же часто, как читаем
// статистику пула.
func (pm *poolManager) activeHealthCheck(period time.Duration) {
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
				pingCtx, cancel := context.WithTimeout(pm.ctx, 5*time.Second)
				err := p.db.PingContext(pingCtx)
				cancel()
				if err != nil {
					pm.logger.Warn("active health check failed", "db", name, "error", err)
					dbUp.WithLabelValues(name).Set(0)
					continue
				}
				dbUp.WithLabelValues(name).Set(1)
			}
		}
	}
}
