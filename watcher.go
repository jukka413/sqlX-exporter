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
// Критичные исправления по сравнению с предыдущей версией:
//
//  1. Наблюдаем за родительской ДИРЕКТОРИЕЙ, а не за самим файлом.
//     Согласно документации fsnotify: "Watching individual files is generally not recommended
//     as many programs (especially editors) update files atomically: it will write to a
//     temporary file which is then moved to destination, overwriting the original.
//     The watcher on the original file is now lost."
//     Источник: https://pkg.go.dev/github.com/fsnotify/fsnotify
//
//  2. Фильтруем события по имени файла — реагируем только на наш config.
//
//  3. Добавлен debounce (150ms) для защиты от burst-событий:
//     редакторы могут генерировать несколько событий Write/Create за одно сохранение,
//     а также от параллельного вызова reload() из нескольких событий одновременно.
func watchConfig(ctx context.Context, logger *slog.Logger, path string, reload func()) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logger.Error("failed to create watcher", "error", err)
		return
	}
	defer watcher.Close()

	// Наблюдаем за директорией, а не за файлом
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

	// debounce: задержка перед вызовом reload чтобы "дождаться тишины" после burst-событий
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

			// Фильтруем: реагируем только на наш файл конфигурации
			eventPath, err := filepath.Abs(event.Name)
			if err != nil || eventPath != absPath {
				continue
			}

			// Реагируем на запись и создание (Create покрывает atomic rename)
			if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) {
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
