package main

import (
	"context"
	"log/slog"

	"github.com/fsnotify/fsnotify"
)

func watchConfig(ctx context.Context, logger *slog.Logger, path string, reload func()) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logger.Error("failed to create watcher", "error", err)
		return
	}
	defer watcher.Close()

	if err := watcher.Add(path); err != nil {
		logger.Error("failed to watch config", "error", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case event := <-watcher.Events:
			if event.Op&fsnotify.Write == fsnotify.Write {
				reload()
			}
		case err := <-watcher.Errors:
			logger.Error("watcher error", "error", err)
		}
	}
}
