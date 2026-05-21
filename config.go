package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Settings  AppSettings            `yaml:"settings"`
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

func loadConfig(path string) (Config, error) {
	var cfg Config
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}

	// Парсим YAML как есть — без ExpandEnv на весь файл.
	// Применять os.ExpandEnv ко всему файлу нельзя: $ в SQL запросах
	// (например v$session, gv$instance) будет воспринят как переменная окружения
	// и затёрт пустой строкой.
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}

	// Подставляем переменные окружения только в поля url баз данных.
	// Это позволяет писать: url: "postgres://user:${DB_PASS}@host:5432/mydb"
	// не затрагивая SQL запросы.
	for name, db := range cfg.Databases {
		db.URL = os.ExpandEnv(db.URL)
		cfg.Databases[name] = db
	}

	return cfg, nil
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
