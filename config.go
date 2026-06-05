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

	// DefaultDB — локальная БД по умолчанию для запросов в этом файле.
	// Работает на уровне каждого файла независимо — в основном конфиге и
	// в каждом инклюде можно задать свою, они не перезатирают друг друга.
	// Запросы без явного поля db: получат это значение при загрузке файла.
	//
	// Отличие от settings.default_db:
	//   - default_db здесь: подставляется в запросы этого файла при загрузке
	//   - settings.default_db: глобальный фолбэк для запросов без db: после мержа всех файлов
	DefaultDB string `yaml:"default_db,omitempty"`

	// Includes — список путей к дополнительным конфиг файлам.
	// Пути относительны к директории основного конфига.
	// Инклюд файлы могут содержать databases, queries и свой default_db.
	// Поддерживается рекурсия: инклюд может инклюдить другие файлы.
	Includes []string `yaml:"includes,omitempty"`

	Databases map[string]DBConfig    `yaml:"databases"`
	Queries   map[string]QueryConfig `yaml:"queries"`
}

// AppSettings — глобальные настройки приложения.
type AppSettings struct {
	// DBReconnectInterval — как часто пытаться переподключиться к недоступным БД.
	DBReconnectInterval string `yaml:"db_reconnect_interval,omitempty"`

	// DefaultDB — глобальный фолбэк для запросов без db: после мержа всех файлов.
	// Используется только если запрос не получил db: ни из своего файла (default_db),
	// ни явно. Должна быть объявлена в секции databases итогового конфига.
	DefaultDB string `yaml:"default_db,omitempty"`
}

// DBReconnectIntervalDuration возвращает интервал переподключения как time.Duration.
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
	Driver string `yaml:"driver"`
	URL    string `yaml:"url"`

	MaxConns     int `yaml:"max_conns"`
	MaxIdleConns int `yaml:"max_idle_conns"`

	MaxConnLifetime   string `yaml:"max_conn_lifetime,omitempty"`
	MaxConnIdleTime   string `yaml:"max_conn_idle_time,omitempty"`
	HealthCheckPeriod string `yaml:"health_check_period,omitempty"`
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
func loadConfig(path string) (Config, error) {
	// Сначала делаем предварительную загрузку чтобы получить settings.default_db,
	// затем перезагружаем с передачей его как parentDefaultDB для инклюдов.
	// Это позволяет settings.default_db из основного конфига работать как глобальный
	// фолбэк для всех вложенных файлов без своего default_db.
	var preview Config
	if data, err := os.ReadFile(path); err == nil {
		_ = yaml.Unmarshal(data, &preview)
	}
	globalDefault := preview.DefaultDB
	if globalDefault == "" {
		globalDefault = preview.Settings.DefaultDB
	}
	return loadConfigWithContext(path, 0, globalDefault)
}

const maxIncludeDepth = 10

func loadConfigWithDepth(path string, depth int) (Config, error) {
	return loadConfigWithContext(path, depth, "")
}

// loadConfigWithContext загружает конфиг передавая parentDefaultDB из родительского файла.
// Если у текущего файла нет своего default_db — используется parentDefaultDB.
// Это позволяет глобальному default_db из основного конфига распространяться
// на все вложенные файлы у которых нет своего локального default_db.
func loadConfigWithContext(path string, depth int, parentDefaultDB string) (Config, error) {
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
	expandURLs(&cfg)

	// Определяем эффективный default_db для этого файла:
	// - если у файла есть свой → используем его
	// - если нет → берём от родителя (основного конфига или settings.default_db)
	effectiveDefaultDB := cfg.DefaultDB
	if effectiveDefaultDB == "" {
		effectiveDefaultDB = parentDefaultDB
	}

	// Подставляем эффективный default_db в запросы без явного db:
	if effectiveDefaultDB != "" {
		applyDefaultDB(&cfg, effectiveDefaultDB)
	}

	// Обрабатываем инклюды — передаём эффективный default_db дочерним файлам
	if len(cfg.Includes) > 0 {
		baseDir := filepath.Dir(path)
		for _, includePath := range cfg.Includes {
			fullPath := filepath.Join(baseDir, includePath)
			inc, err := loadConfigWithContext(fullPath, depth+1, effectiveDefaultDB)
			if err != nil {
				return cfg, fmt.Errorf("include %q: %w", fullPath, err)
			}
			mergeConfig(&cfg, inc)
		}
		cfg.Includes = nil
	}

	return cfg, nil
}

// applyDefaultDB подставляет defaultDB в запросы файла у которых db не указан.
// defaultDB может быть локальным (из самого файла) или унаследованным от родителя.
// Запросы с явным db: не затрагиваются.
func applyDefaultDB(cfg *Config, defaultDB string) {
	for name, q := range cfg.Queries {
		if q.DB == "" {
			q.DB = defaultDB
			cfg.Queries[name] = q
		}
	}
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
	if src.Settings.DBReconnectInterval != "" {
		dst.Settings.DBReconnectInterval = src.Settings.DBReconnectInterval
	}
	if src.Settings.DefaultDB != "" {
		dst.Settings.DefaultDB = src.Settings.DefaultDB
	}

	if dst.Databases == nil {
		dst.Databases = make(map[string]DBConfig)
	}
	for name, db := range src.Databases {
		dst.Databases[name] = db
	}

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
		if q.DB == "" && cfg.Settings.DefaultDB == "" {
			return fmt.Errorf("query %q: db is required (or set default_db / settings.default_db)", name)
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
