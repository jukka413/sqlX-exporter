package main

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Settings AppSettings `yaml:"settings"`

	// DefaultDB — локальная БД по умолчанию для запросов в этом файле.
	// Если не указан — используется parentDefaultDB от родителя или settings.default_db.
	DefaultDB string `yaml:"default_db,omitempty"`

	// IncludeDefaults — маппинг имени инклюд-файла к одной или нескольким БД по умолчанию.
	// Позволяет централизованно задать default_db для каждого файла с метриками
	// не дублируя его внутри самих файлов.
	//
	// Один файл — одна БД:
	//   include_defaults:
	//     oracle-metrics.yaml: "azdh_oracle"
	//
	// Один файл — несколько БД (запросы клонируются для каждой):
	//   include_defaults:
	//     oracle-metrics.yaml:
	//       - "azdh_oracle"
	//       - "ir_test_oracle"
	IncludeDefaults map[string]IncludeDefault `yaml:"include_defaults,omitempty"`

	// Includes — список путей к дополнительным конфиг файлам.
	Includes []string `yaml:"includes,omitempty"`

	Databases map[string]DBConfig    `yaml:"databases"`
	Queries   map[string]QueryConfig `yaml:"queries"`
}

// IncludeDefault поддерживает как строку так и список строк в YAML:
//
//	oracle-metrics.yaml: "azdh_oracle"          → ["azdh_oracle"]
//	oracle-metrics.yaml: ["azdh_oracle", "ir_test_oracle"] → ["azdh_oracle", "ir_test_oracle"]
type IncludeDefault []string

func (id *IncludeDefault) UnmarshalYAML(value *yaml.Node) error {
	// Пробуем как строку
	var single string
	if err := value.Decode(&single); err == nil {
		*id = IncludeDefault{single}
		return nil
	}
	// Пробуем как список
	var multi []string
	if err := value.Decode(&multi); err == nil {
		*id = IncludeDefault(multi)
		return nil
	}
	return fmt.Errorf("include_defaults value must be a string or list of strings")
}

// AppSettings — глобальные настройки приложения.
type AppSettings struct {
	DBReconnectInterval string `yaml:"db_reconnect_interval,omitempty"`
	DefaultDB           string `yaml:"default_db,omitempty"`
}

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

	// MetricName — имя метрики в Prometheus.
	// Заполняется автоматически при загрузке конфига — равно имени запроса.
	// При клонировании для нескольких БД ключ воркера становится составным
	// (name+db), но MetricName остаётся оригинальным именем запроса.
	// Это позволяет иметь одинаковое имя метрики для разных БД без суффиксов.
	MetricName string `yaml:"-"` // не читается из YAML, проставляется кодом
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
	// Предварительное чтение для получения глобального default_db
	var preview Config
	if data, err := os.ReadFile(path); err == nil {
		_ = yaml.Unmarshal(data, &preview)
	}
	globalDefault := preview.DefaultDB
	if globalDefault == "" {
		globalDefault = preview.Settings.DefaultDB
	}
	return loadConfigWithContext(path, 0, globalDefault, preview.IncludeDefaults)
}

const maxIncludeDepth = 10

// loadConfigWithContext загружает конфиг передавая контекст от родителя:
//   - parentDefaultDB — глобальный default_db от родителя
//   - parentIncludeDefaults — маппинг include_defaults от родителя
func loadConfigWithContext(path string, depth int, parentDefaultDB string, parentIncludeDefaults map[string]IncludeDefault) (Config, error) {
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

	expandURLs(&cfg)

	// Проставляем MetricName = имя запроса для каждого запроса.
	// Это нужно до клонирования — при клонировании ключ изменится но MetricName останется.
	for name, q := range cfg.Queries {
		if q.MetricName == "" {
			q.MetricName = name
			cfg.Queries[name] = q
		}
	}

	// Определяем эффективный default_db для этого файла
	effectiveDefaultDB := cfg.DefaultDB
	if effectiveDefaultDB == "" {
		effectiveDefaultDB = parentDefaultDB
	}

	// Применяем effectiveDefaultDB к запросам этого файла
	if effectiveDefaultDB != "" {
		applyDefaultDB(&cfg, effectiveDefaultDB)
	}

	// Мержим include_defaults — родительский + текущий файла
	// (текущий имеет приоритет)
	effectiveIncludeDefaults := mergeIncludeDefaults(parentIncludeDefaults, cfg.IncludeDefaults)

	// Обрабатываем инклюды
	if len(cfg.Includes) > 0 {
		baseDir := filepath.Dir(path)
		for _, includePath := range cfg.Includes {
			fullPath := filepath.Join(baseDir, includePath)
			baseName := filepath.Base(includePath)

			// Проверяем есть ли для этого файла маппинг в include_defaults
			if dbs, ok := effectiveIncludeDefaults[baseName]; ok {
				// Загружаем файл для каждой БД и мержим.
				// Если БД несколько — все запросы получают суффикс _dbname
				// чтобы воркеры были уникальны. Лейбл db в метрике всё равно
				// различает их в Prometheus.
				addSuffix := len(dbs) > 1
				for _, db := range dbs {
					inc, err := loadConfigWithContext(fullPath, depth+1, db, effectiveIncludeDefaults)
					if err != nil {
						return cfg, fmt.Errorf("include %q (db=%s): %w", fullPath, db, err)
					}
					if addSuffix {
						inc = cloneQueriesForDB(inc, db)
					}
					mergeConfig(&cfg, inc)
				}
			} else {
				// Нет маппинга — загружаем как обычно
				inc, err := loadConfigWithContext(fullPath, depth+1, effectiveDefaultDB, effectiveIncludeDefaults)
				if err != nil {
					return cfg, fmt.Errorf("include %q: %w", fullPath, err)
				}
				mergeConfig(&cfg, inc)
			}
		}
		cfg.Includes = nil
	}

	return cfg, nil
}

// mergeIncludeDefaults мержит два маппинга include_defaults.
// override имеет приоритет над base.
func mergeIncludeDefaults(base, override map[string]IncludeDefault) map[string]IncludeDefault {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	result := make(map[string]IncludeDefault, len(base)+len(override))
	for k, v := range base {
		result[k] = v
	}
	for k, v := range override {
		result[k] = v
	}
	return result
}

// applyDefaultDB подставляет defaultDB в запросы без явного db.
func applyDefaultDB(cfg *Config, defaultDB string) {
	for name, q := range cfg.Queries {
		if q.DB == "" {
			q.DB = defaultDB
			cfg.Queries[name] = q
		}
	}
}

// expandURLs подставляет переменные окружения только в поля url баз данных.
//
// Значения по умолчанию URL-кодируются чтобы спецсимволы в паролях
// (@ / + # % & : пробел и др.) не ломали парсинг URL.
//
// Исключение — значения похожие на TNS-дескриптор Oracle:
// "(DESCRIPTION=(ADDRESS=...)...)" — такие значения НЕ кодируются,
// иначе скобки и = превратятся в %28 %29 %3D и resolveDSN не сможет
// распознать и распарсить TNS-дескриптор в db.go.
func expandURLs(cfg *Config) {
	for name, db := range cfg.Databases {
		db.URL = os.Expand(db.URL, func(key string) string {
			val := os.Getenv(key)
			if val == "" {
				return ""
			}
			if looksLikeTNSDescriptor(val) {
				return val // подставляем как есть, без кодирования
			}
			return url.PathEscape(val)
		})
		cfg.Databases[name] = db
	}
}

// looksLikeTNSDescriptor определяет похоже ли значение на TNS-дескриптор Oracle.
// Признак: начинается с "(" и содержит "DESCRIPTION=" или "ADDRESS=" —
// этого достаточно чтобы отличить TNS от обычного пароля.
func looksLikeTNSDescriptor(val string) bool {
	trimmed := strings.TrimSpace(val)
	if !strings.HasPrefix(trimmed, "(") {
		return false
	}
	upper := strings.ToUpper(trimmed)
	return strings.Contains(upper, "DESCRIPTION=") || strings.Contains(upper, "ADDRESS=")
}

// cloneQueriesForDB возвращает копию конфига где ключи запросов становятся
// составными "name__db" для уникальности воркеров.
// MetricName при этом сохраняется оригинальным — имя метрики в Prometheus
// остаётся чистым без суффиксов.
func cloneQueriesForDB(cfg Config, db string) Config {
	if len(cfg.Queries) == 0 {
		return cfg
	}
	newQueries := make(map[string]QueryConfig, len(cfg.Queries))
	for name, q := range cfg.Queries {
		// Ключ составной — уникален для каждой БД
		// MetricName остаётся оригинальным именем запроса
		newQueries[name+"__"+db] = q
	}
	cfg.Queries = newQueries
	return cfg
}

// mergeConfig мержит src в dst.
// src (инклюд) имеет приоритет — перезаписывает существующие ключи.
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
