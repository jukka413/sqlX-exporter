package main

import (
	"context"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

// watchConfig следит за изменениями конфигурационного файла и вызывает reload при обнаружении.
//
// Поведение в Kubernetes (ConfigMap/Secret volumes):
//
//	kubelet обновляет файлы через AtomicWriter: создаёт новую директорию,
//	записывает файлы, атомарно переключает симлинк ..data → новая директория,
//	удаляет старую. В результате inotify получает IN_DELETE_SELF (Remove/Rename),
//	а не IN_MODIFY/IN_CLOSE_WRITE как при обычном обновлении файла.
//	Кроме того, после Remove inotify watch ломается и нужно переподписываться.
//	Источник: https://ahmet.im/blog/kubernetes-inotify/
//
// Поведение вне Kubernetes (обычная ФС, vim, nano и др.):
//
//	Редакторы часто пишут через временный файл + rename → приходит Create.
//	Прямая запись → приходит Write.
//
// Решение: следим за ДИРЕКТОРИЕЙ (не файлом), реагируем на Write/Create/Remove/Rename,
// при Remove/Rename переподписываемся на директорию.
// Debounce 150ms защищает от burst-событий.
// watchConfig следит за основным конфигом и директориями инклюд-файлов.
// extraDirs — дополнительные директории для наблюдения (из includes в конфиге).
// При изменении любого файла в отслеживаемых директориях вызывается reload.
func watchConfig(ctx context.Context, logger *slog.Logger, path string, reload func(), extraDirs ...string) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logger.Error("failed to create watcher", "error", err)
		return
	}
	defer watcher.Close()

	dir := filepath.Dir(path)
	absPath, err := filepath.Abs(path)
	if err != nil {
		logger.Error("failed to resolve config path", "error", err)
		return
	}

	if err := watcher.Add(dir); err != nil {
		logger.Error("failed to watch config directory", "dir", dir, "error", err)
		return
	}
	logger.Info("watching config directory", "dir", dir, "file", absPath)

	// Следим за директориями инклюд-файлов
	watchedDirs := map[string]struct{}{dir: {}}
	for _, extraDir := range extraDirs {
		absDir, err := filepath.Abs(extraDir)
		if err != nil {
			continue
		}
		if _, already := watchedDirs[absDir]; already {
			continue
		}
		if err := watcher.Add(absDir); err != nil {
			logger.Error("failed to watch include directory", "dir", absDir, "error", err)
			continue
		}
		watchedDirs[absDir] = struct{}{}
		logger.Info("watching include directory", "dir", absDir)
	}

	const debounceDelay = 150 * time.Millisecond
	var debounceTimer *time.Timer

	triggerReload := func() {
		if debounceTimer != nil {
			debounceTimer.Stop()
		}
		debounceTimer = time.AfterFunc(debounceDelay, func() {
			logger.Info("config change detected, reloading")
			reload()
		})
	}

	for {
		select {
		case <-ctx.Done():
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			return

		case event, ok := <-watcher.Events:
			if !ok {
				return
			}

			// Фильтруем по имени файла — реагируем только на наш конфиг.
			// В k8s события приходят на файлы внутри директории (..data, symlinks).
			// Проверяем и прямое совпадение пути и совпадение базового имени —
			// потому что в k8s реальный путь может быть вида:
			// /etc/sqlx-exporter/..2024_04_24_12_00_00.123456789/config.yaml
			eventAbs, _ := filepath.Abs(event.Name)
			matchesDirect := eventAbs == absPath
			matchesName := filepath.Base(event.Name) == filepath.Base(absPath)
			// ..data — специальный симлинк который kubelet переключает атомарно
			isDataSymlink := filepath.Base(event.Name) == "..data"

			if !matchesDirect && !matchesName && !isDataSymlink {
				continue
			}

			logger.Debug("watcher event", "op", event.Op, "file", event.Name)

			switch {
			case event.Has(fsnotify.Write) || event.Has(fsnotify.Create):
				// Обычная ФС: прямая запись или atomic rename редактора
				triggerReload()

			case event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename):
				// Kubernetes AtomicWriter: симлинк переключился → файл "удалён".
				// Watch на директорию после Remove не ломается (в отличие от watch на файл),
				// но переподписываемся на случай если директория была пересоздана.
				_ = watcher.Remove(dir)
				if err := watcher.Add(dir); err != nil {
					logger.Error("failed to re-watch config directory after remove",
						"dir", dir, "error", err)
				}
				triggerReload()
			}

		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			logger.Error("watcher error", "error", err)
		}
	}
}
