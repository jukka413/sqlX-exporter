package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	go_ora "github.com/sijms/go-ora/v2"
)

// resolveDSN возвращает итоговую строку подключения для sql.Open.
//
// Для Oracle с TNS-дескриптором:
//
//	URL в конфиге содержит сырой TNS вида:
//	(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=...)(PORT=...))(CONNECT_DATA=(SERVICE_NAME=...)))
//	go-ora не умеет принимать его напрямую через sql.Open — нужно собрать
//	через go_ora.BuildJDBC который правильно оборачивает дескриптор.
//
//	Формат в config.yaml:
//	  driver: "oracle"
//	  url: "oracle+tns://user:password@(DESCRIPTION=...)"
//
//	Для всех остальных драйверов и обычных oracle:// URL — передаём как есть.
func resolveDSN(cfg DBConfig) (string, error) {
	const oracleTNSPrefix = "oracle+tns://"

	if !strings.HasPrefix(cfg.URL, oracleTNSPrefix) {
		return cfg.URL, nil
	}

	// Парсим oracle+tns://user:password@(DESCRIPTION=...)
	// Отрезаем префикс, разбиваем на credentials и TNS-дескриптор
	rest := strings.TrimPrefix(cfg.URL, oracleTNSPrefix)

	// Ищем @ после которого идёт TNS-дескриптор
	atIdx := strings.Index(rest, "@")
	if atIdx == -1 {
		return "", fmt.Errorf("oracle+tns:// URL must contain @ before TNS descriptor")
	}

	credentials := rest[:atIdx] // "user:password"
	tns := rest[atIdx+1:]       // "(DESCRIPTION=...)"

	// Разбиваем credentials на user и password
	user, password, _ := strings.Cut(credentials, ":")

	// BuildJDBC собирает строку которую go-ora понимает с TNS-дескриптором
	dsn := go_ora.BuildJDBC(user, password, tns, nil)
	return dsn, nil
}

func openAndPingDB(ctx context.Context, cfg DBConfig) (*sql.DB, error) {
	dsn, err := resolveDSN(cfg)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open(cfg.Driver, dsn)
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
