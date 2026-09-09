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
type app struct {
	logger *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	configPath string

	pm *poolManager
	wm *workerManager

	mu                sync.Mutex
	reconnectInterval time.Duration
	defaultDB         string

	// reloadMu не даёт reload() выполниться параллельно самому себе.
	reloadMu sync.Mutex

	// revision — счётчик применённой конфигурации, растёт на 1 в начале
	// каждого reload(). Одним и тем же значением в одном reload штампуются
	// и poolManager (applyConfig), и workerManager (reconcile) — см.
	// комментарий у poolManager. poolHealthChecker сверяет revision обоих
	// менеджеров перед тем как реконсилировать воркеры после реконнекта:
	// если они разошлись, значит health-checker снял снэпшоты в момент
	// когда reload() уже обновил один из менеджеров, но ещё не второй —
	// раньше это могло привести к тому что health-checker реконсилировал
	// воркеры по снэпшоту queries, снятому ДО применения текущего reload,
	// то есть эффективно откатывал только что применённые изменения.
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

func newApp(ctx context.Context, cancel context.CancelFunc, logger *slog.Logger, configPath string) *app {
	return &app{
		logger:            logger,
		ctx:               ctx,
		cancel:            cancel,
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

				oldPool, ok := a.pm.commitReconnect(name, dbCfg, rev, db)
				if !ok {
					a.logger.Info("discarding stale reconnect result — config changed during dial", "db", name)
					_ = db.Close()
					continue
				}

				a.logger.Info("db reconnected", "db", name)
				a.pm.updateDBUpMetrics()

				// Снэпшот queries берём здесь, а не в начале тика — сжимает
				// окно, в котором он может относиться к другому revision чем
				// только что закоммиченный пул (см. комментарий у app.revision).
				// Передаём queriesRev в reconcile, а не rev пула — если они
				// разошлись, wm.lastQueriesRevision должен честно отражать
				// ревизию ПЕРЕДАННЫХ queries, а не создавать видимость что
				// воркеры уже на актуальном состоянии.
				queries, queriesRev := a.wm.snapshotLastQueries()
				if queriesRev != rev {
					a.logger.Warn("worker state revision does not match pool revision right after reconnect — "+
						"reconciling with the best available snapshot, a concurrent reload will correct this shortly",
						"db", name, "pool_revision", rev, "worker_revision", queriesRev)
				}
				a.wm.reconcile(queriesRev, queries, a.pm.snapshotPools(), a.getDefaultDB())

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
	a.wm.reconcile(rev, newCfg.Queries, a.pm.snapshotPools(), a.getDefaultDB())

	a.pm.closePools(toClose)
	a.pm.deletePoolMetrics(removedDBs)

	configReloadTotal.WithLabelValues("success").Inc()
	configLastSuccessTimestamp.SetToCurrentTime()

	a.logger.Info("reload complete")
}

func (a *app) stopAllWorkers() {
	a.wm.stopAll()
}

func (a *app) closeAllPools() {
	a.pm.closeAll()
}
