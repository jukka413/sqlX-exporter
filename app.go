package main

import (
	"context"
	"log/slog"
	"path/filepath"
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

	// configReady — true как только хотя бы один reload успешно прошёл
	// структурную валидацию (загрузился и не провалился на
	// validateDatabasesAndSettings). Используется /readyz — раньше при
	// невалидном стартовом конфиге процесс продолжал жить и /metrics
	// отдавал только runtime-метрики, а Kubernetes считал Pod полностью
	// рабочим. Намеренно НЕ зависит от доступности отдельных БД — частичный
	// outage одной БД штатно переживается архитектурой (см. failedPools),
	// и не должен выталкивать здоровый Pod из Service.
	configReady bool

	// watchDirsCh передаёт watcher'у актуальный список директорий для
	// отслеживания после каждого успешного reload. Раньше этот список
	// собирался ОДИН раз при старте процесса (main.collectIncludeDirs) —
	// новый инклюд, добавленный через hot-reload в директорию, которая ещё
	// не отслеживалась, был не виден watcher'у до рестарта Pod'а. Каждое
	// значение в канале — ПОЛНЫЙ снимок нужных директорий, не дельта,
	// поэтому пропуск промежуточного обновления (см. publishWatchDirs)
	// ничего не портит — следующий reload пришлёт уже актуальный полный список.
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
		reconnectInterval: 5 * time.Minute, // дефолт до первого reload
		watchDirsCh:       make(chan []string, 1),
	}
}

// publishWatchDirs передаёт watcher'у свежий список директорий, не блокируясь.
// Если предыдущее значение ещё не было прочитано — заменяет его (каждое
// значение самодостаточный полный снимок, промежуточные версии не нужны).
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

// uniqueDirs возвращает уникальные директории для списка абсолютных путей файлов.
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

// isReady сообщает применялся ли конфиг успешно хотя бы раз — см. комментарий
// у configReady в типе app.
func (a *app) isReady() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.configReady
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
				a.pm.updateDBUpMetrics()

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
		configReloadTotal.WithLabelValues("invalid_config").Inc()
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
	a.configReady = true
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

	configReloadTotal.WithLabelValues("success").Inc()
	configLastSuccessTimestamp.SetToCurrentTime()

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
