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

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}
