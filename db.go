package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	go_ora "github.com/sijms/go-ora/v2"
)

// openAndPingDB открывает соединение с БД и проверяет его через Ping.
// Для Oracle использует go_ora.BuildUrl который правильно кодирует
// параметры с пробелами (client charset).
func openAndPingDB(ctx context.Context, cfg DBConfig) (*sql.DB, error) {
	var (
		db  *sql.DB
		err error
	)

	if strings.EqualFold(strings.TrimSpace(cfg.Driver), "oracle") {
		db, err = openOracleDB(cfg)
	} else {
		db, err = sql.Open(cfg.Driver, cfg.URL)
	}
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

// openOracleDB строит DSN через go_ora.BuildUrl передавая все параметры
// включая "client charset" который нельзя надёжно закодировать в URL строке.
//
// Поддерживаемые форматы cfg.URL:
//
//	oracle://user:pass@host:port/service
//	oracle+tns://user:pass@(DESCRIPTION=...)
//	oracle+tns://user:pass@/?CONNSTR=(DESCRIPTION=...)
func openOracleDB(cfg DBConfig) (*sql.DB, error) {
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
	// Это работает даже без NLS_LANG в окружении.
	if err := go_ora.AddSessionParam(db, "nls_language", "AMERICAN"); err != nil {
		// Не фатально — продолжаем без NLS настройки
		_ = err
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

	// client charset — декодировать сообщения сервера в UTF-8.
	// Критично для Oracle с кириллической локалью (CL8MSWIN1251 и др.):
	// без этого сообщения об ошибках приходят как \ufffd\ufffd\ufffd.
	// Передаём через urlOptions а не через URL строку — пробел в имени параметра
	// не позволяет надёжно закодировать его в query string.
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

	user, password, _ := strings.Cut(credentials, ":")

	urlOptions := map[string]string{
		"client charset": "UTF8",
	}

	return go_ora.BuildJDBC(user, password, tns, urlOptions), nil
}
