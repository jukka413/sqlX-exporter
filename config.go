package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Settings AppSettings `yaml:"settings"`
	// Includes — список путей к дополнительным конфиг файлам.
	// Пути относительны к директории основного конфига.
	// Инклюд файлы могут содержать databases и queries.
	// При конфликте ключей — основной файл имеет приоритет.
	// Поддерживается рекурсия: инклюд может инклюдить другие файлы.
	Includes  []string               `yaml:"includes,omitempty"`
	Databases map[string]DBConfig    `yaml:"databases"`
	Queries   map[string]QueryConfig `yaml:"queries"`
}

// AppSettings — глобальные настройки приложения.
type AppSettings struct {
	// DBReconnectInterval — как часто пытаться переподключиться к недоступным БД.
	// Формат: Go duration string, например "5m", "30s", "1h".
	// По умолчанию: 5m.
	DBReconnectInterval string `yaml:"db_reconnect_interval,omitempty"`

	// DefaultDB — имя БД которая используется если в запросе не указано поле db.
	// Должна быть объявлена в секции databases.
	// Пример: если default_db: "main", то запросы без db: берут подключение "main".
	DefaultDB string `yaml:"default_db,omitempty"`
}

// DBReconnectIntervalDuration возвращает интервал переподключения как time.Duration.
// Если не задан или невалиден — возвращает дефолтные 5 минут.
func (s AppSettings) DBReconnectIntervalDuration() time.Duration {
	if s.DBReconnectInterval == "" {
		return 5 * time.Minute
	}
	d, err := time.ParseDuration(s.DBReconnectInterval)
	if err != nil {
		return 5 * time.Minute
	}
	return d
}

type DBConfig struct {
	Driver string `yaml:"driver"` // "pgx", "sqlserver", "mysql", "oracle"

	// Поддерживает подстановку env-переменных: "postgres://user:${DB_PASS}@host/db"
	URL string `yaml:"url"`

	MaxConns     int `yaml:"max_conns"`
	MaxIdleConns int `yaml:"max_idle_conns"`

	MaxConnLifetime   string `yaml:"max_conn_lifetime,omitempty"`
	MaxConnIdleTime   string `yaml:"max_conn_idle_time,omitempty"`
	HealthCheckPeriod string `yaml:"health_check_period,omitempty"` // ignored
}

type QueryConfig struct {
	DB       string `yaml:"db"`
	SQL      string `yaml:"sql"`
	Timeout  string `yaml:"timeout"`
	Interval string `yaml:"interval"`

	Schedule *ScheduleConfig `yaml:"schedule,omitempty"`

	Labels      map[string]string `yaml:"labels,omitempty"`
	ValueColumn string            `yaml:"value_column,omitempty"`
}

type ScheduleConfig struct {
	Timezone string       `yaml:"timezone,omitempty"`
	At       []ScheduleAt `yaml:"at"`
}

type ScheduleAt struct {
	Weekday string `yaml:"weekday"`
	Time    string `yaml:"time"`
}

// loadConfig загружает конфиг из файла path и рекурсивно обрабатывает includes.
// includes мержатся в основной конфиг — основной файл имеет приоритет при конфликте ключей.
func loadConfig(path string) (Config, error) {
	return loadConfigWithDepth(path, 0)
}

// maxIncludeDepth ограничивает глубину рекурсии инклюдов для защиты от циклических ссылок.
const maxIncludeDepth = 10

func loadConfigWithDepth(path string, depth int) (Config, error) {
	var cfg Config

	if depth > maxIncludeDepth {
		return cfg, fmt.Errorf("include depth limit (%d) exceeded at %q — possible circular include", maxIncludeDepth, path)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}

	// Подставляем переменные окружения только в url — не затрагиваем SQL.
	// $ в SQL запросах (v$session, gv$instance) не должен интерпретироваться.
	expandURLs(&cfg)

	// Обрабатываем инклюды — рекурсивно загружаем и мержим
	if len(cfg.Includes) > 0 {
		baseDir := filepath.Dir(path)
		for _, includePath := range cfg.Includes {
			fullPath := filepath.Join(baseDir, includePath)
			inc, err := loadConfigWithDepth(fullPath, depth+1)
			if err != nil {
				return cfg, fmt.Errorf("include %q: %w", fullPath, err)
			}
			// Инклюды применяются последовательно и перезаписывают основной конфиг.
			// Последний инклюд в списке имеет наивысший приоритет.
			mergeConfig(&cfg, inc)
		}
		// Очищаем includes из финального конфига — они уже обработаны
		cfg.Includes = nil
	}

	return cfg, nil
}

// expandURLs подставляет переменные окружения только в поля url баз данных.
func expandURLs(cfg *Config) {
	for name, db := range cfg.Databases {
		db.URL = os.ExpandEnv(db.URL)
		cfg.Databases[name] = db
	}
}

// mergeConfig мержит src в dst.
// Инклюд (src) имеет приоритет — перезаписывает существующие ключи в dst.
// Порядок инклюдов в списке includes определяет приоритет: последний побеждает.
func mergeConfig(dst *Config, src Config) {
	// Settings: инклюд перезаписывает если задан
	if src.Settings.DBReconnectInterval != "" {
		dst.Settings.DBReconnectInterval = src.Settings.DBReconnectInterval
	}
	if src.Settings.DefaultDB != "" {
		dst.Settings.DefaultDB = src.Settings.DefaultDB
	}

	// Databases: инклюд перезаписывает существующие и добавляет новые
	if dst.Databases == nil {
		dst.Databases = make(map[string]DBConfig)
	}
	for name, db := range src.Databases {
		dst.Databases[name] = db
	}

	// Queries: инклюд перезаписывает существующие и добавляет новые
	if dst.Queries == nil {
		dst.Queries = make(map[string]QueryConfig)
	}
	for name, q := range src.Queries {
		dst.Queries[name] = q
	}
}

func validateConfigDurations(cfg Config) error {
	if cfg.Settings.DBReconnectInterval != "" {
		if _, err := time.ParseDuration(cfg.Settings.DBReconnectInterval); err != nil {
			return fmt.Errorf("settings.db_reconnect_interval is invalid: %w", err)
		}
	}
	if cfg.Settings.DefaultDB != "" {
		if _, ok := cfg.Databases[cfg.Settings.DefaultDB]; !ok {
			return fmt.Errorf("settings.default_db %q is not defined in databases", cfg.Settings.DefaultDB)
		}
	}

	for name, db := range cfg.Databases {
		if db.Driver == "" {
			return fmt.Errorf("database %q: driver is required", name)
		}
		if db.URL == "" {
			return fmt.Errorf("database %q: url is required", name)
		}
		if db.MaxIdleConns > 0 && db.MaxConns > 0 && db.MaxIdleConns > db.MaxConns {
			return fmt.Errorf("database %q: max_idle_conns (%d) must be <= max_conns (%d)",
				name, db.MaxIdleConns, db.MaxConns)
		}
		if db.MaxConnLifetime != "" {
			if _, err := time.ParseDuration(db.MaxConnLifetime); err != nil {
				return fmt.Errorf("database %q has invalid max_conn_lifetime: %w", name, err)
			}
		}
		if db.MaxConnIdleTime != "" {
			if _, err := time.ParseDuration(db.MaxConnIdleTime); err != nil {
				return fmt.Errorf("database %q has invalid max_conn_idle_time: %w", name, err)
			}
		}
		if db.HealthCheckPeriod != "" {
			if _, err := time.ParseDuration(db.HealthCheckPeriod); err != nil {
				return fmt.Errorf("database %q has invalid health_check_period: %w", name, err)
			}
		}
	}

	for name, q := range cfg.Queries {
		// q.DB может быть пустым если задан settings.default_db
		if q.DB == "" && cfg.Settings.DefaultDB == "" {
			return fmt.Errorf("query %q: db is required (or set settings.default_db)", name)
		}
		if q.DB != "" {
			if _, ok := cfg.Databases[q.DB]; !ok {
				return fmt.Errorf("query %q: db %q is not defined in databases", name, q.DB)
			}
		}
		if _, err := time.ParseDuration(q.Timeout); err != nil {
			return errors.New("query " + name + " has invalid timeout: " + err.Error())
		}
		if q.Schedule != nil {
			if err := validateSchedule(q.Schedule); err != nil {
				return fmt.Errorf("query %q has invalid schedule: %w", name, err)
			}
		} else {
			if _, err := time.ParseDuration(q.Interval); err != nil {
				return fmt.Errorf("query %q has invalid interval: %w", name, err)
			}
		}
	}
	return nil
}
