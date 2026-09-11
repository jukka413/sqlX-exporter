package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	go_ora "github.com/sijms/go-ora/v2"
)

// openAndPingDB открывает соединение с БД и проверяет его через Ping.
// Для Oracle использует go_ora.BuildUrl который правильно кодирует
// параметры с пробелами (client charset).
func openAndPingDB(ctx context.Context, cfg DBConfig, logger *slog.Logger) (*sql.DB, error) {
	var (
		db  *sql.DB
		err error
	)

	if strings.EqualFold(strings.TrimSpace(cfg.Driver), "oracle") {
		db, err = openOracleDB(cfg, logger)
	} else {
		db, err = sql.Open(cfg.Driver, cfg.URL)
	}
	if err != nil {
		return nil, err
	}

	if cfg.MaxConns != nil {
		db.SetMaxOpenConns(*cfg.MaxConns)
	}
	if cfg.MaxIdleConns != nil {
		// nil-проверка, не ">0" — 0 значащее ("не держать простаивающие
		// соединения"), не "не задано".
		db.SetMaxIdleConns(*cfg.MaxIdleConns)
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

// openOracleDB строит DSN через go_ora.BuildUrl передавая все параметры
// включая "client charset" который нельзя надёжно закодировать в URL строке.
//
// Поддерживаемые форматы cfg.URL:
//
//	oracle://user:pass@host:port/service
//	oracle+tns://user:pass@(DESCRIPTION=...)
//	oracle+tns://user:pass@/?CONNSTR=(DESCRIPTION=...)
func openOracleDB(cfg DBConfig, logger *slog.Logger) (*sql.DB, error) {
	connStr, err := buildOracleDSN(cfg.URL)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("oracle", connStr)
	if err != nil {
		return nil, err
	}

	// Устанавливаем язык сообщений на английский чтобы избежать проблем
	// с кодировкой кириллицы в текстах ошибок Oracle.
	// Не фатально если не удалось — подключение продолжает работать,
	// просто сообщения об ошибках могут прийти не на английском.
	if err := go_ora.AddSessionParam(db, "nls_language", "AMERICAN"); err != nil {
		if logger != nil {
			logger.Warn("failed to set nls_language session param", "error", err)
		}
	}

	return db, nil
}

// buildOracleDSN разбирает URL конфига и строит финальный DSN через go_ora.BuildUrl.
// BuildUrl правильно кодирует параметры с пробелами в имени ("client charset").
func buildOracleDSN(rawURL string) (string, error) {
	const tnsPrefix = "oracle+tns://"

	// --- TNS формат ---
	if strings.HasPrefix(rawURL, tnsPrefix) {
		return buildTNSDSN(rawURL, tnsPrefix)
	}

	// --- Стандартный oracle:// URL ---
	// Парсим его чтобы извлечь компоненты и передать в BuildUrl
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("invalid oracle URL: %w", err)
	}

	host := u.Hostname()
	portStr := u.Port()
	port := 1521
	if portStr != "" {
		if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
			return "", fmt.Errorf("invalid oracle port %q: %w", portStr, err)
		}
	}

	service := strings.TrimPrefix(u.Path, "/")
	user := u.User.Username()
	password, _ := u.User.Password()

	// Переносим существующие query параметры из URL
	urlOptions := make(map[string]string)
	for k, vs := range u.Query() {
		if len(vs) > 0 {
			urlOptions[k] = vs[0]
		}
	}

	// client charset — иначе сообщения об ошибках сервера с кириллицей
	// (Oracle с локалью CL8MSWIN1251 и др.) приходят как \ufffd\ufffd\ufffd.
	// Через urlOptions, не через URL строку — пробел в имени параметра не
	// кодируется надёжно в query string.
	if _, exists := urlOptions["client charset"]; !exists {
		urlOptions["client charset"] = "UTF8"
	}

	return go_ora.BuildUrl(host, port, service, user, password, urlOptions), nil
}

// buildTNSDSN извлекает компоненты из oracle+tns:// URL
// и строит DSN через go_ora.BuildJDBC.
func buildTNSDSN(rawURL, prefix string) (string, error) {
	rest := strings.TrimPrefix(rawURL, prefix)

	atIdx := strings.Index(rest, "@")
	if atIdx == -1 {
		return "", fmt.Errorf("oracle+tns:// URL must contain @ before TNS descriptor")
	}

	credentials := rest[:atIdx]
	tns := rest[atIdx+1:]

	// Нормализуем варианты написания после @:
	//   /?CONNSTR=(DESCRIPTION=...) → (DESCRIPTION=...)
	//   /?(DESCRIPTION=...)         → (DESCRIPTION=...)
	for _, pfx := range []string{"/?CONNSTR=", "/?", "/"} {
		if strings.HasPrefix(tns, pfx) {
			tns = strings.TrimPrefix(tns, pfx)
			break
		}
	}

	// Убираем пробелы — go-ora не всегда корректно обрабатывает
	// пробелы внутри TNS дескриптора
	tns = strings.Join(strings.Fields(tns), "")

	rawUser, rawPassword, _ := strings.Cut(credentials, ":")

	// PathUnescape, не QueryUnescape: этот путь разбирает URL через strings.Cut,
	// раскодировать нужно вручную. QueryUnescape трактует "+" как пробел —
	// пароль с литеральным "+" приехал бы в Oracle искажённым.
	user, err := url.PathUnescape(rawUser)
	if err != nil {
		return "", fmt.Errorf("oracle+tns:// invalid encoding in username: %w", err)
	}
	password, err := url.PathUnescape(rawPassword)
	if err != nil {
		return "", fmt.Errorf("oracle+tns:// invalid encoding in password: %w", err)
	}

	urlOptions := map[string]string{
		"client charset": "UTF8",
	}

	return go_ora.BuildJDBC(user, password, tns, urlOptions), nil
}
