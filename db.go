package main

import (
	"context"
	"database/sql"
	"time"
)

func openAndPingDB(ctx context.Context, cfg DBConfig) (*sql.DB, error) {
	db, err := sql.Open(cfg.Driver, cfg.URL)
	if err != nil {
		return nil, err
	}

	if cfg.MaxConns > 0 {
		db.SetMaxOpenConns(cfg.MaxConns)
	}

	// ИСПРАВЛЕНО: MaxIdleConns (бывший MinConns) правильно маппится в SetMaxIdleConns.
	// SetMaxIdleConns задаёт максимум простаивающих соединений в пуле.
	// В database/sql нет "минимальных" соединений — пул создаёт их по требованию.
	// Документация: https://pkg.go.dev/database/sql#DB.SetMaxIdleConns
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}

	if cfg.MaxConnLifetime != "" {
		d, err := time.ParseDuration(cfg.MaxConnLifetime)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		db.SetConnMaxLifetime(d)
	}

	if cfg.MaxConnIdleTime != "" {
		d, err := time.ParseDuration(cfg.MaxConnIdleTime)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		db.SetConnMaxIdleTime(d)
	}

	// health_check_period intentionally ignored (as requested)

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}
