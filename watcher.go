package main

import (
	"context"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

// watchConfig следит за директорией конфига (не за файлом — kubelet при
// обновлении ConfigMap меняет ..data симлинк, а не сам файл; watch на
// директорию переживает Remove/Rename внутри неё, watch на файл — нет).
// reload() вызывается синхронно в этой же горутине, не через отдельный
// таймер — гарантирует, что reload() не переживает shutdown.
//
// updateDirs — актуальный список директорий после каждого reload (новый
// инклюд подхватывается без рестарта). Директории только добавляются;
// события фильтруются по директории и расширению (.yaml/.yml), не по
// точному пути — любой такой файл в отслеживаемой директории вызовет
// reload, даже не относящийся к конфигу. Обычно неважно (ConfigMap
// directory обычно содержит только этот конфиг), reload на неизменившийся
// конфиг — дешёвый no-op.
//
// ready — одно значение сразу после того как watch реально установлен (nil
// — успех, иначе ошибка). Без этого сигнала между стартом watchConfig и
// установкой watcher.Add() есть окно, в которое ConfigMap мог бы обновиться
// незамеченным — fsnotify не воспроизводит события задним числом.
func watchConfig(ctx context.Context, logger *slog.Logger, path string, reload func(), updateDirs <-chan []string, ready chan<- error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logger.Error("failed to create watcher", "error", err)
		ready <- err
		return
	}
	defer func() {
		if err := watcher.Close(); err != nil {
			logger.Warn("failed to close fsnotify watcher", "error", err)
		}
	}()

	// absPath — до вычисления dir, иначе при относительном --config путь
	// dir был бы относительным, а eventDir при событиях всегда абсолютный.
	absPath, err := filepath.Abs(path)
	if err != nil {
		logger.Error("failed to resolve config path", "error", err)
		ready <- err
		return
	}
	absPath = filepath.Clean(absPath)
	dir := filepath.Dir(absPath)

	if err := watcher.Add(dir); err != nil {
		logger.Error("failed to watch config directory", "dir", dir, "error", err)
		ready <- err
		return
	}
	logger.Info("watching config directory", "dir", dir, "file", absPath)
	ready <- nil

	watchedDirs := map[string]struct{}{dir: {}}

	addDir := func(rawDir string) {
		absDir, err := filepath.Abs(rawDir)
		if err != nil {
			return
		}
		absDir = filepath.Clean(absDir)
		if _, already := watchedDirs[absDir]; already {
			return
		}
		if err := watcher.Add(absDir); err != nil {
			logger.Error("failed to watch include directory", "dir", absDir, "error", err)
			return
		}
		watchedDirs[absDir] = struct{}{}
		logger.Info("watching include directory", "dir", absDir)
	}

	const debounceDelay = 150 * time.Millisecond
	var (
		debounceTimer *time.Timer
		debounceC     <-chan time.Time
	)

	triggerReload := func() {
		if debounceTimer == nil {
			debounceTimer = time.NewTimer(debounceDelay)
			debounceC = debounceTimer.C
			return
		}
		if !debounceTimer.Stop() {
			select {
			case <-debounceTimer.C:
			default:
			}
		}
		debounceTimer.Reset(debounceDelay)
		debounceC = debounceTimer.C
	}

	for {
		select {
		case <-ctx.Done():
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			return

		case dirs := <-updateDirs:
			for _, d := range dirs {
				addDir(d)
			}

		case <-debounceC:
			debounceC = nil
			if ctx.Err() != nil {
				continue // shutdown уже начался — не стартуем новый reload
			}
			logger.Info("config change detected, reloading")
			reload()

		case event, ok := <-watcher.Events:
			if !ok {
				return
			}

			eventAbs, err := filepath.Abs(event.Name)
			if err != nil {
				continue
			}
			eventAbs = filepath.Clean(eventAbs)

			// Полный путь, не basename — иначе ложно совпало бы с
			// одноимённым файлом в другой отслеживаемой директории.
			isMainConfig := eventAbs == absPath
			isDataSymlink := filepath.Base(eventAbs) == "..data"
			// Инклюды лежат под своими именами, не под basename основного
			// конфига — без этого их изменения тихо игнорировались бы.
			isYAMLInWatchedDir := func() bool {
				ext := filepath.Ext(eventAbs)
				if ext != ".yaml" && ext != ".yml" {
					return false
				}
				_, watched := watchedDirs[filepath.Dir(eventAbs)]
				return watched
			}()

			if !isMainConfig && !isDataSymlink && !isYAMLInWatchedDir {
				continue
			}

			logger.Debug("watcher event", "op", event.Op, "file", eventAbs)

			if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) ||
				event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename) {
				triggerReload() // watch на директорию не ломается от Remove/Rename её содержимого
			}

		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			logger.Error("watcher error", "error", err)
		}
	}
}
