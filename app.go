package main

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// app — тонкий оркестратор. Сам не хранит ни пулы, ни воркеры — только
// координирует poolManager (единственный владелец *sql.DB) и workerManager
// (единственный владелец жизненного цикла воркеров) в правильном порядке.
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

	// reloadMu гарантирует что reload() не выполняется параллельно сам с собой.
	// Без этого лока watcher debounce мог бы вызвать reload() ещё раз пока
	// предыдущий вызов (open/ping нескольких БД, до 5с на каждую) ещё не
	// завершился.
	reloadMu sync.Mutex
}

func newApp(ctx context.Context, cancel context.CancelFunc, logger *slog.Logger, configPath string) *app {
	return &app{
		logger:            logger,
		ctx:               ctx,
		cancel:            cancel,
		configPath:        configPath,
		pm:                newPoolManager(ctx, logger),
		wm:                newWorkerManager(ctx, logger),
		reconnectInterval: 5 * time.Minute, // дефолт до первого reload
	}
}

func (a *app) getDefaultDB() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.defaultDB
}

// =========================================================================
// poolHealthChecker
// =========================================================================

// poolHealthChecker периодически пытается переподключиться к БД из
// failedPools. При успехе просит poolManager закоммитить новый пул и
// workerManager — реконсилировать воркеры для него.
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

			gen, failed := a.pm.snapshotForReconnect()
			if len(failed) == 0 {
				continue
			}
			queries := a.wm.snapshotLastQueries()

			for name, dbCfg := range failed {
				a.logger.Info("retrying db connection", "db", name)

				db, ok := a.pm.dial(name, dbCfg)
				if !ok {
					continue
				}

				oldPool, ok := a.pm.commitReconnect(name, dbCfg, gen, db)
				if !ok {
					// Пока шёл дозвон, reload() успел применить более новый
					// конфиг — этот результат устарел, закрываем соединение
					// и ничего не трогаем: reload() уже разобрался с этой БД.
					a.logger.Info("discarding stale reconnect result — config changed during dial", "db", name)
					_ = db.Close()
					continue
				}

				a.logger.Info("db reconnected", "db", name)

				a.wm.reconcile(queries, a.pm.snapshotPools(), a.getDefaultDB())

				// Закрываем старый пул ПОСЛЕ reconcile — reconcile гарантированно
				// останавливает и дожидается любого воркера, который использовал
				// старый пул (через poolChanged), прежде чем Close().
				if oldPool != nil && oldPool.db != nil {
					a.pm.closePools(map[string]*dbPool{name: oldPool})
				}
			}
		}
	}
}

// =========================================================================
// reload
// =========================================================================

func (a *app) reload() {
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
	// Сломанный инклюд (файл не найден, битый YAML) больше не блокирует
	// весь reload — только тот конкретный файл. Явно проговариваем это в
	// сообщении, иначе легко решить что раз "reload complete" ниже —
	// значит применилось вообще всё, включая содержимое этого файла.
	for _, reason := range newCfg.failedIncludes {
		a.logger.Error("include failed to load — its databases/queries are missing from this config, "+
			"the rest of the config was still applied normally", "detail", reason)
	}
	configIncludeFailures.Set(float64(len(newCfg.failedIncludes)))

	if err := validateDatabasesAndSettings(newCfg); err != nil {
		a.logger.Error("invalid config", "error", err)
		return
	}

	if removed := sanitizeQueries(&newCfg); len(removed) > 0 {
		for _, reason := range removed {
			a.logger.Error("skipping invalid query", "reason", reason)
		}
	}

	toClose, removedDBs := a.pm.applyConfig(newCfg.Databases)

	a.mu.Lock()
	a.reconnectInterval = newCfg.Settings.DBReconnectIntervalDuration()
	a.defaultDB = newCfg.Settings.DefaultDB
	a.mu.Unlock()

	// Воркеры реконсилируются ДО закрытия старых пулов, не после. reconcile
	// гарантированно делает cancel()+wg.Wait() для каждого воркера, который
	// использовал пул из toClose (через poolChanged), прежде чем этот воркер
	// уступает место новому. Если закрыть toClose раньше — в окне между
	// Close() и вызовом reconcile тикер старого воркера может успеть
	// сработать и получить "sql: database is closed" на живом, но уже
	// закрытом соединении.
	a.wm.reconcile(newCfg.Queries, a.pm.snapshotPools(), a.getDefaultDB())

	a.pm.closePools(toClose)
	a.pm.deletePoolMetrics(removedDBs)

	a.logger.Info("reload complete")
}

// =========================================================================
// shutdown helpers
// =========================================================================

func (a *app) stopAllWorkers() {
	a.wm.stopAll()
}

func (a *app) closeAllPools() {
	a.pm.closeAll()
}
