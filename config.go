package main

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
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

	// Includes — карта путей к дополнительным конфиг файлам.
	// Ключ — произвольное имя (для удобства в values файлах), значение — true.
	// Map вместо списка специально: при мерже нескольких Helm values файлов
	// Helm делает глубокий мерж map-полей, но ПОЛНОСТЬЮ заменяет списки.
	// Со списком второй values файл стирал includes первого; с map — оба
	// набора ключей объединяются автоматически.
	//
	// Пример в values:
	//   config:
	//     includes:
	//       "oracle.yaml": true
	//       "mssql.yaml": true
	Includes map[string]bool `yaml:"includes,omitempty"`

	Databases map[string]DBConfig    `yaml:"databases"`
	Queries   map[string]QueryConfig `yaml:"queries"`

	// skippedIncludes — список файлов из includes которые были проигнорированы
	// потому что для них не нашлось записи в include_defaults.
	// Не из YAML — заполняется кодом при загрузке, для логирования в app.go.
	skippedIncludes []string `yaml:"-"`
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
	// Предварительное чтение для получения глобального default_db и include_defaults
	// до того как они нужны дочерним инклюдам. Ошибки здесь намеренно не считаются
	// фатальными на этом шаге — тот же файл будет прочитан и провалидирован
	// по-настоящему внутри loadConfigWithContext, и там ошибка корректно вернётся
	// наружу. Если этот шаг тихо не сработал (плохой YAML, нет файла) —
	// globalDefault/IncludeDefaults останутся пустыми, что эквивалентно их отсутствию
	// в конфиге, и не маскирует реальную ошибку — она всплывёт чуть ниже.
	var preview Config
	data, err := os.ReadFile(path)
	if err == nil {
		// Ошибку Unmarshal здесь сознательно не пробрасываем — невалидный YAML
		// будет повторно обработан (и вернёт понятную ошибку с точным местом)
		// внутри loadConfigWithContext на той же строке кода.
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

	// Обрабатываем инклюды.
	// Сортируем ключи для детерминированного порядка обработки —
	// порядок итерации по map в Go не гарантирован, а порядок важен
	// для приоритета при mergeConfig (последний обработанный — побеждает).
	if len(cfg.Includes) > 0 {
		baseDir := filepath.Dir(path)
		includePaths := make([]string, 0, len(cfg.Includes))
		for p := range cfg.Includes {
			includePaths = append(includePaths, p)
		}
		sort.Strings(includePaths)

		for _, includePath := range includePaths {
			fullPath := filepath.Join(baseDir, includePath)
			baseName := filepath.Base(includePath)

			// Файл загружается ТОЛЬКО если для него есть запись в include_defaults.
			// Без записи — файл осознанно игнорируется (см. skippedIncludes ниже).
			// Это позволяет держать файлы метрик переиспользуемыми: добавление
			// файла в includes без include_defaults безопасно ничего не делает,
			// а не падает с ошибкой "db is required".
			dbs, ok := effectiveIncludeDefaults[baseName]
			if !ok {
				cfg.skippedIncludes = append(cfg.skippedIncludes,
					fmt.Sprintf("%q: no entry in include_defaults", includePath))
				continue
			}

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
				// Переносим skippedIncludes из дочернего конфига наверх
				cfg.skippedIncludes = append(cfg.skippedIncludes, inc.skippedIncludes...)
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
//
// ВНИМАНИЕ (известный риск, не исправлено): если два разных инклюд-файла
// (например из разных Git-репозиториев в multi-source ArgoCD) случайно
// объявляют запрос с одинаковым именем — один тихо перезапишет другой
// без какого-либо предупреждения, потому что mergeConfig не имеет доступа
// к логгеру и не сравнивает старое/новое значение перед записью.
// Симптом в проде: метрика которая должна обновляться от одной БД,
// внезапно начинает работать с другой, без единой строки в логах.
// TODO: либо протащить logger через loadConfigWithContext → mergeConfig,
// либо возвращать []string с предупреждениями как делает sanitizeQueries.
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

// sanitizeQueries проверяет каждый запрос независимо и удаляет невалидные
// из cfg.Queries вместо того чтобы валить весь конфиг одной ошибкой.
// Возвращает список причин по которым запросы были удалены — для логирования.
//
// Критичные проверки (драйверы, URL, durations баз данных) остаются
// в validateDatabasesAndSettings и продолжают валить весь reload — они означают
// что конфиг структурно сломан, а не что у одного запроса опечатка.
func sanitizeQueries(cfg *Config) []string {
	var removed []string

	for name, q := range cfg.Queries {
		if reason := validateSingleQuery(cfg, name, q); reason != "" {
			removed = append(removed, fmt.Sprintf("query %q skipped: %s", name, reason))
			delete(cfg.Queries, name)
		}
	}

	return removed
}

// validateSingleQuery проверяет один запрос и возвращает причину невалидности
// (пустая строка — запрос валиден).
func validateSingleQuery(cfg *Config, name string, q QueryConfig) string {
	if q.DB == "" && cfg.Settings.DefaultDB == "" {
		return "db is required (or set default_db / settings.default_db)"
	}
	if q.DB != "" {
		if _, ok := cfg.Databases[q.DB]; !ok {
			return fmt.Sprintf("db %q is not defined in databases", q.DB)
		}
	}
	if _, err := time.ParseDuration(q.Timeout); err != nil {
		return fmt.Sprintf("invalid timeout: %v", err)
	}
	if q.Schedule != nil {
		if err := validateSchedule(q.Schedule); err != nil {
			return fmt.Sprintf("invalid schedule: %v", err)
		}
	} else {
		if _, err := time.ParseDuration(q.Interval); err != nil {
			return fmt.Sprintf("invalid interval: %v", err)
		}
	}
	return ""
}

// validateDatabasesAndSettings проверяет критичные части конфига —
// настройки и описания БД. Ошибки здесь валят весь reload, так как
// означают структурно сломанный конфиг (а не опечатку в одном запросе).
func validateDatabasesAndSettings(cfg Config) error {
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
	return nil
}
