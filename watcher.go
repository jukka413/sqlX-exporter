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
//	удаляет старую. В результате inotify получает событие на ..data внутри
//	отслеживаемой директории (Remove/Rename), а не IN_MODIFY/IN_CLOSE_WRITE
//	как при обычном обновлении файла.
//	Источник: https://ahmet.im/blog/kubernetes-inotify/
//
//	Важно: мы следим за ДИРЕКТОРИЕЙ, а не за отдельным файлом/симлинком.
//	IN_DELETE_SELF/IN_MOVE_SELF (события которые ломают watch) срабатывают
//	только когда удаляют/переименовывают САМ отслеживаемый объект — то есть
//	директорию целиком. Удаление или переименование файла ВНУТРИ отслеживаемой
//	директории (в том числе симлинка ..data) — это обычное дочернее событие,
//	watch на директорию от него не ломается и переподписываться не нужно.
//	См. inotify(7) и https://pkg.go.dev/github.com/fsnotify/fsnotify.
//
// Поведение вне Kubernetes (обычная ФС, vim, nano и др.):
//
//	Редакторы часто пишут через временный файл + rename → приходит Create.
//	Прямая запись → приходит Write.
//
// Debounce и shutdown: reload() вызывается СИНХРОННО в этой же горутине
// (не через time.AfterFunc в отдельной горутине, как было раньше) — таймер
// дебаунса читается тем же select, что и ctx.Done()/события fsnotify. Это
// гарантирует что: (a) reload() никогда не переживёт возврат из watchConfig —
// вызывающий, дождавшись возврата watchConfig, точно знает что никакой
// reload() больше не выполняется и не может внезапно стартовать; (b) если
// reload() уже идёт (может занимать секунды — open/ping нескольких БД) в
// момент отмены ctx, цикл select не увидит ctx.Done() пока reload() не
// вернётся — таймер и ctx.Done() физически не могут обрабатываться
// одновременно в этом однопоточном цикле.
//
// updateDirs — канал, по которому вызывающий (app.reload) присылает
// АКТУАЛЬНЫЙ ПОЛНЫЙ список директорий для отслеживания после каждого
// успешного парсинга конфига. Раньше этот список собирался один раз при
// старте процесса — новый инклюд, добавленный через hot-reload в директорию,
// которая ещё не отслеживалась, был не виден watcher'у до рестарта Pod'а.
// Директории только добавляются, никогда не убираются — лишний watch на
// более не нужную директорию безвреден (события из неё просто не пройдут
// фильтр isYAMLInWatchedDir по конкретным файлам).
func watchConfig(ctx context.Context, logger *slog.Logger, path string, reload func(), updateDirs <-chan []string) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logger.Error("failed to create watcher", "error", err)
		return
	}
	defer func() {
		if err := watcher.Close(); err != nil {
			logger.Warn("failed to close fsnotify watcher", "error", err)
		}
	}()

	// absPath вычисляется ДО того как из него берётся директория — иначе при
	// запуске с относительным путём (--config ./config.yaml, дефолт в main.go)
	// dir получался бы относительным ("."), а eventDir при событиях всегда
	// абсолютный — они бы никогда не совпадали.
	absPath, err := filepath.Abs(path)
	if err != nil {
		logger.Error("failed to resolve config path", "error", err)
		return
	}
	absPath = filepath.Clean(absPath)
	dir := filepath.Dir(absPath)

	if err := watcher.Add(dir); err != nil {
		logger.Error("failed to watch config directory", "dir", dir, "error", err)
		return
	}
	logger.Info("watching config directory", "dir", dir, "file", absPath)

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
				// select мог одновременно увидеть готовый таймер и отменённый
				// ctx — явная проверка на случай если выбор пал на таймер:
				// не стартуем новый reload когда shutdown уже начался.
				continue
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

			// Сравниваем только по полному абсолютному пути — сравнение по
			// одному basename могло бы ложно совпасть с одноимённым файлом
			// в другой отслеживаемой директории.
			isMainConfig := eventAbs == absPath
			// ..data — специальный симлинк который kubelet переключает атомарно
			// при обновлении ЛЮБОГО файла в ConfigMap. Реагируем всегда.
			isDataSymlink := filepath.Base(eventAbs) == "..data"
			// Инклюд-файлы лежат в watchedDirs под своими собственными именами
			// (oracle-metrics.yaml и т.п.), не совпадающими с basename основного
			// конфига — без этой проверки их изменения тихо игнорировались бы.
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
				// Никакого Remove/Add директории здесь не требуется — мы следим
				// за директорией, а не за файлом внутри неё, а watch на директорию
				// не ломается от Remove/Rename её содержимого (см. комментарий
				// в начале функции).
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
