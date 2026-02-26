package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Databases map[string]DBConfig    `yaml:"databases"`
	Queries   map[string]QueryConfig `yaml:"queries"`
}

type DBConfig struct {
	Driver string `yaml:"driver"` // e.g. "pgx", "sqlserver", "mysql", "godror"
	URL    string `yaml:"url"`

	MaxConns int `yaml:"max_conns"`

	// ИСПРАВЛЕНО: переименовано из min_conns в max_idle_conns.
	// В database/sql нет понятия "минимальных соединений" — пул создаёт их лениво.
	// SetMaxIdleConns задаёт максимальное количество ПРОСТАИВАЮЩИХ соединений,
	// которые пул держит открытыми для повторного использования.
	// Документация: https://pkg.go.dev/database/sql#DB.SetMaxIdleConns
	// Важно: MaxIdleConns должен быть <= MaxConns, иначе Go автоматически его уменьшит.
	MaxIdleConns int `yaml:"max_idle_conns"`

	MaxConnLifetime string `yaml:"max_conn_lifetime,omitempty"`  // e.g. "1h"
	MaxConnIdleTime string `yaml:"max_conn_idle_time,omitempty"` // e.g. "30m"

	// Kept for backward compatibility, but ignored (by request).
	HealthCheckPeriod string `yaml:"health_check_period,omitempty"` // e.g. "1m" (ignored)
}

type QueryConfig struct {
	DB       string `yaml:"db"`
	SQL      string `yaml:"sql"`
	Timeout  string `yaml:"timeout"`
	Interval string `yaml:"interval"`

	Schedule *ScheduleConfig `yaml:"schedule,omitempty"`

	// Labels — опциональные статические кастомные лейблы, добавляемые к метрике.
	//   labels:
	//     env: "prod"
	//     team: "analytics"
	Labels map[string]string `yaml:"labels,omitempty"`

	// ValueColumn — имя столбца, значение которого становится значением метрики.
	// Остальные столбцы автоматически становятся динамическими лейблами.
	// Если не указан — старое поведение: SELECT возвращает одну строку с одним числом.
	//
	// Пример:
	//   sql: "SELECT region, env, active_users FROM stats"
	//   value_column: "active_users"
	// Результат: my_metric{db="main", region="eu", env="prod"} 142
	//            my_metric{db="main", region="us", env="prod"} 89
	ValueColumn string `yaml:"value_column,omitempty"`
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
	err = yaml.Unmarshal(data, &cfg)
	return cfg, err
}

func validateConfigDurations(cfg Config) error {
	for name, db := range cfg.Databases {
		if db.Driver == "" {
			return fmt.Errorf("database %q driver is required", name)
		}
		if db.URL == "" {
			return fmt.Errorf("database %q url is required", name)
		}

		// Проверяем корректность соотношения MaxIdleConns и MaxConns
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
