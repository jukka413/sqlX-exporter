package main

import (
	"context"
	"log/slog"
	"path/filepath"
	"sync"
	"time"
)

// app — тонкий оркестратор: координирует poolManager (владелец *sql.DB) и
// workerManager (владелец воркеров) в правильном порядке. Сам не хранит
// пулы/воркеры.
//
// Lock order (всегда снаружи внутрь, никогда наоборот):
//
//	reloadMu (только вокруг reload())
//	    -> stateMu
//	        -> poolManager.transitionMu -> poolManager.mu
//	        -> workerManager.reconcileMu -> workerManager.mu
//
// Ни один метод poolManager/workerManager не берёт app.stateMu — они ничего
// не знают о его существовании. Захват идёт только сверху, из app.
type app struct {
	logger *slog.Logger

	ctx context.Context

	configPath string

	pm *poolManager
	wm *workerManager

	mu                sync.Mutex
	reconnectInterval time.Duration
	defaultDB         string

	// reloadMu не даёт reload() выполниться параллельно самому себе.
	reloadMu sync.Mutex

	// stateMu сериализует переход состояния целиком — и коммит пулов, и
	// реконсиляцию воркеров — как один неделимый блок, для обоих путей
	// применения конфига (reload и reconnect). Без него между коммитом
	// poolManager и вызовом workerManager.reconcile есть зазор в несколько
	// инструкций, в который может втиснуться конкурентный commitReconnect:
	// он увидит уже обновлённый pm.revision, но ещё не обновлённый
	// wm.lastQueriesRevision, и реконсилирует новые пулы со старыми
	// desired-запросами. Stale-revision guard внутри workerManager.reconcile
	// этот конкретный случай не ловит — там revision формально не "старее"
	// уже применённой, она просто отстаёт от pm на долю секунды. dial()
	// (сетевой I/O) остаётся вне stateMu в обоих путях — под локом только
	// сам commit.
	stateMu sync.Mutex

	// revision — счётчик применённой конфигурации, растёт на 1 в начале
	// каждого reload(). Одним значением в одном reload штампуются и
	// poolManager (applyConfig), и workerManager (reconcile) — см.
	// комментарий у poolManager.
	revision uint64

	// configReady — успешно ли прошёл хотя бы один reload (структурная
	// валидация). Используется /readyz. Не зависит от доступности
	// отдельных БД — partial outage переживается штатно (см. failedPools).
	configReady bool

	// watchDirsCh — актуальный список директорий для watcher'а после
	// каждого reload. Каждое значение — полный снимок, не дельта, поэтому
	// пропуск промежуточного обновления в publishWatchDirs безопасен.
	watchDirsCh chan []string
}

func newApp(ctx context.Context, logger *slog.Logger, configPath string) *app {
	return &app{
		logger:            logger,
		ctx:               ctx,
		configPath:        configPath,
		pm:                newPoolManager(ctx, logger),
		wm:                newWorkerManager(ctx, logger),
		reconnectInterval: 5 * time.Minute,
		watchDirsCh:       make(chan []string, 1),
	}
}

// publishWatchDirs передаёт watcher'у свежий список директорий, не блокируясь.
func (a *app) publishWatchDirs(dirs []string) {
	select {
	case <-a.watchDirsCh:
	default:
	}
	select {
	case a.watchDirsCh <- dirs:
	default:
	}
}

func uniqueDirs(paths []string) []string {
	seen := make(map[string]struct{}, len(paths))
	var dirs []string
	for _, p := range paths {
		d := filepath.Dir(p)
		if _, ok := seen[d]; !ok {
			seen[d] = struct{}{}
			dirs = append(dirs, d)
		}
	}
	return dirs
}

func (a *app) getDefaultDB() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.defaultDB
}

func (a *app) isReady() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.configReady
}

// poolHealthChecker периодически пытается переподключиться к БД из
// failedPools.
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
			a.mu.Unlock()
			if newPeriod != period {
				ticker.Reset(newPeriod)
				period = newPeriod
				a.logger.Info("db reconnect interval updated", "interval", period)
			}

			rev, failed := a.pm.snapshotForReconnect()
			if len(failed) == 0 {
				continue
			}

			for name, dbCfg := range failed {
				a.logger.Info("retrying db connection", "db", name)

				db, ok := a.pm.dial(name, dbCfg)
				if !ok {
					continue
				}

				// commitReconnect + reconcile — тот же неделимый блок под
				// stateMu, что и в reload() (см. комментарий у поля). dial
				// выше — сетевой I/O, специально вне лока.
				a.stateMu.Lock()

				oldPool, ok := a.pm.commitReconnect(name, dbCfg, rev, db)
				if !ok {
					a.stateMu.Unlock()
					a.logger.Info("discarding stale reconnect result — config changed during dial", "db", name)
					_ = db.Close()
					continue
				}

				a.logger.Info("db reconnected", "db", name)
				a.pm.updateDBUpMetrics()

				queries, queriesRev := a.wm.snapshotLastQueries()
				if queriesRev != rev {
					// Под stateMu commitReconnect и reconcile всегда идут одним
					// блоком — если revision тут разошлись, это не тайминг,
					// а нарушение самого инварианта stateMu где-то ещё.
					// Реконсилировать в таком состоянии небезопасно.
					a.logger.Error("worker state revision does not match pool revision right after "+
						"reconnect under stateMu — this should be impossible, skipping reconcile",
						"db", name, "pool_revision", rev, "worker_revision", queriesRev)
					a.stateMu.Unlock()
					continue
				}
				stats := a.wm.reconcile(queriesRev, queries, a.pm.snapshotPools(), a.pm.pendingDBs(), a.getDefaultDB())
				a.logger.Info("workers reconciled after reconnect", "db", name,
					"queries_started", stats.Started, "queries_restarted", stats.Restarted)

				a.stateMu.Unlock()

				// После reconcile — он гарантированно останавливает воркеры,
				// использующие старый пул, прежде чем этот пул закрывается.
				if oldPool != nil && oldPool.db != nil {
					a.pm.closePools(map[string]*dbPool{name: oldPool})
				}
			}
		}
	}
}

func (a *app) reload() {
	a.reloadMu.Lock()
	defer a.reloadMu.Unlock()

	a.logger.Info("reloading configuration")
	configLastReloadTimestamp.SetToCurrentTime()

	newCfg, err := loadConfig(a.configPath)
	if err != nil {
		a.logger.Error("failed to load config", "error", err)
		configReloadTotal.WithLabelValues("load_error").Inc()
		return
	}

	a.publishWatchDirs(uniqueDirs(newCfg.dependencies))

	for _, reason := range newCfg.skippedIncludes {
		a.logger.Info("include skipped (no include_defaults entry)", "detail", reason)
	}
	for _, reason := range newCfg.missingEnvVars {
		a.logger.Error("environment variable not set, substituted as empty string", "detail", reason)
	}
	for _, reason := range newCfg.overwritten {
		a.logger.Warn("config key overwritten by a later include", "detail", reason)
	}
	for _, reason := range newCfg.failedIncludes {
		a.logger.Error("include failed to load — the rest of the config was still applied normally",
			"detail", reason)
	}
	configIncludeFailures.Set(float64(len(newCfg.failedIncludes)))

	if err := validateDatabasesAndSettings(newCfg); err != nil {
		a.logger.Error("invalid config", "error", err)
		configReloadTotal.WithLabelValues("invalid_config").Inc()
		return
	}

	if removed := sanitizeQueries(&newCfg); len(removed) > 0 {
		for _, reason := range removed {
			a.logger.Error("skipping invalid query", "reason", reason)
		}
	}

	// Всё от увеличения revision до reconcile — один неделимый блок под
	// stateMu (см. комментарий у поля). dial внутри applyConfig может занять
	// секунды на несколько БД — это осознанная цена: без stateMu на всю эту
	// секцию оставался бы зазор, в который мог втиснуться commitReconnect
	// с уже новым pm.revision, но ещё старым wm.lastQueriesRevision.
	a.stateMu.Lock()

	a.mu.Lock()
	a.revision++
	rev := a.revision
	a.mu.Unlock()

	toClose, removedDBs := a.pm.applyConfig(rev, newCfg.Databases)

	a.mu.Lock()
	a.reconnectInterval = newCfg.Settings.DBReconnectIntervalDuration()
	a.defaultDB = newCfg.Settings.DefaultDB
	a.configReady = true
	a.mu.Unlock()

	// reconcile ДО закрытия старых пулов — иначе воркер может тикнуть в
	// окне между Close() и остановкой воркера и получить "database is closed".
	stats := a.wm.reconcile(rev, newCfg.Queries, a.pm.snapshotPools(), a.pm.pendingDBs(), a.getDefaultDB())

	a.stateMu.Unlock()

	// Закрытие старых пулов — уже безопасно вне stateMu: reconcile выше
	// гарантированно остановил все воркеры, которые их использовали.
	a.pm.closePools(toClose)
	a.pm.deletePoolMetrics(removedDBs)

	// success только если реально ВСЁ применилось; наличие failedIncludes
	// означает что часть конфига (запросы/БД одного или нескольких файлов)
	// не была применена — раньше это всё равно писалось как "success",
	// хотя app_config_include_failures уже мог быть > 0. Pending-запросы —
	// та же история: их желаемый конфиг не применился, потому что новая
	// БД не подключилась.
	result := "success"
	if len(newCfg.failedIncludes) > 0 || stats.Pending > 0 {
		result = "partial_success"
	}
	configReloadTotal.WithLabelValues(result).Inc()
	configLastSuccessTimestamp.SetToCurrentTime()

	a.logger.Info("reload complete",
		"result", result,
		"failed_includes", len(newCfg.failedIncludes),
		"queries_started", stats.Started,
		"queries_restarted", stats.Restarted,
		"queries_stopped", stats.Stopped,
		"queries_unchanged", stats.Unchanged,
		"queries_pending", stats.Pending,
	)
}

func (a *app) stopAllWorkers() {
	a.wm.stopAll()
}

func (a *app) closeAllPools() {
	a.pm.closeAll()
}
