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

	// Формат: oracle+tns://user:password@(DESCRIPTION=...)
	// или:     oracle+tns://user:password@/?CONNSTR=(DESCRIPTION=...)
	// Оба варианта нормализуем к чистому TNS-дескриптору для BuildJDBC.
	rest := strings.TrimPrefix(cfg.URL, oracleTNSPrefix)

	atIdx := strings.Index(rest, "@")
	if atIdx == -1 {
		return "", fmt.Errorf("oracle+tns:// URL must contain @ before TNS descriptor")
	}

	credentials := rest[:atIdx]
	tns := rest[atIdx+1:]

	// Нормализуем варианты написания после @:
	//   /?CONNSTR=(DESCRIPTION=...) → (DESCRIPTION=...)
	//   /?(DESCRIPTION=...)         → (DESCRIPTION=...)
	//   (DESCRIPTION=...)           → (DESCRIPTION=...) без изменений
	for _, prefix := range []string{"/?CONNSTR=", "/?", "/?"} {
		if strings.HasPrefix(tns, prefix) {
			tns = strings.TrimPrefix(tns, prefix)
			break
		}
	}
	// Убираем ведущий / если остался
	tns = strings.TrimPrefix(tns, "/")

	// Убираем пробелы — в TNS дескрипторах пробелы между скобками валидны
	// для человека но go-ora их не всегда корректно обрабатывает
	tns = strings.Join(strings.Fields(tns), "")

	user, password, _ := strings.Cut(credentials, ":")

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
