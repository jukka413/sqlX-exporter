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

	// missingEnvVars — список переменных окружения упомянутых в url через
	// ${VAR}, которые не были найдены в окружении при загрузке. Не из YAML —
	// заполняется кодом, для логирования в app.go. Без этого отсутствующая
	// переменная тихо превращалась в пустой пароль и проявлялась только как
	// загадочная ошибка аутентификации на стороне БД (например ORA-01017).
	missingEnvVars []string `yaml:"-"`

	// overwritten — список случаев когда инклюд перезаписал существующий
	// database/query с другим значением. Не из YAML — заполняется кодом,
	// для логирования в app.go. Без этого коллизия имён между двумя
	// независимыми инклюд-файлами (например из разных Git-репозиториев)
	// проходит абсолютно молча — метрика продолжает существовать под тем же
	// именем, но начинает собирать данные с другой БД или по другому SQL.
	overwritten []string `yaml:"-"`
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

	// Env — окружение этой БД (prod, test, dev и т.д.). Если задано,
	// автоматически добавляется как лейбл "env" ко всем метрикам,
	// использующим эту БД — не нужно прописывать labels: env вручную
	// в каждом запросе. Опционально: если не задано, лейбл env не добавляется.
	Env string `yaml:"env,omitempty"`

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

	// MaxRows — лимит строк для multi-row запросов (value_column задан).
	// Защита от unbounded cardinality: без лимита SELECT без GROUP BY/LIMIT
	// над большой таблицей может создать миллионы time series и привести к
	// OOM. Если не задано (0), используется defaultMaxRows (см. worker.go).
	// Игнорируется для single-value запросов.
	MaxRows int `yaml:"max_rows,omitempty"`

	// MetricName — имя метрики в Prometheus.
	// Заполняется автоматически при загрузке конфига — равно имени запроса.
	// При клонировании для нескольких БД ключ воркера становится составным
	// (name+db), но MetricName остаётся оригинальным именем запроса.
	// Это позволяет иметь одинаковое имя метрики для разных БД без суффиксов.
	MetricName string `yaml:"-"` // не читается из YAML, проставляется кодом

	// DBEnv — значение databases.<db>.env для той БД, к которой привязан
	// этот запрос. Не из YAML — проставляется в app.reconcileWorkers в момент
	// резолва q.DB → *dbPool, потому что именно там впервые известны и запрос,
	// и его пул одновременно. Пробрасывается в worker.go как лейбл "env".
	DBEnv string `yaml:"-"`
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
	var preview Config
	data, err := os.ReadFile(path)
	if err == nil {
		_ = yaml.Unmarshal(data, &preview)
	}
	globalDefault := preview.DefaultDB
	if globalDefault == "" {
		globalDefault = preview.Settings.DefaultDB
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		absPath = path
	}
	rootDir := filepath.Dir(absPath)

	return loadConfigWithContext(path, 0, globalDefault, preview.IncludeDefaults, rootDir)
}

const maxIncludeDepth = 10

// loadConfigWithContext загружает конфиг передавая контекст от родителя:
//   - parentDefaultDB — глобальный default_db от родителя
//   - parentIncludeDefaults — маппинг include_defaults от родителя
//   - rootDir — директория основного (корневого) конфига; все инклюды должны
//     резолвиться внутри неё, это защита от path traversal через includes
//     (например includes: {"../../../etc/something.yaml": true})
func loadConfigWithContext(path string, depth int, parentDefaultDB string, parentIncludeDefaults map[string]IncludeDefault, rootDir string) (Config, error) {
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

	cfg.missingEnvVars = expandURLs(&cfg)

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
			// includes — map[string]bool. false означает "временно выключен" —
			// пропускаем без загрузки, но и без включения в skippedIncludes
			// (это осознанное отключение автором конфига, а не "забыли настроить").
			if !cfg.Includes[includePath] {
				continue
			}

			fullPath := filepath.Join(baseDir, includePath)

			// Path traversal guard: резолвленный путь должен оставаться внутри
			// rootDir корневого конфига. Без этой проверки includes с "../.."
			// мог бы читать произвольные файлы с того же volume, к которым
			// у процесса есть доступ на чтение.
			absFullPath, err := filepath.Abs(fullPath)
			if err != nil {
				return cfg, fmt.Errorf("include %q: cannot resolve absolute path: %w", includePath, err)
			}
			rel, err := filepath.Rel(rootDir, absFullPath)
			if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				return cfg, fmt.Errorf("include %q resolves outside the config root directory %q", includePath, rootDir)
			}

			// Ищем запись в include_defaults сначала по полному include path
			// (например "team-a/metrics.yaml"), и только если такой записи нет —
			// по basename ("metrics.yaml") для обратной совместимости. Без этого
			// приоритета "team-a/metrics.yaml" и "team-b/metrics.yaml" искали бы
			// одну и ту же запись "metrics.yaml" и получили бы одинаковую БД.
			baseName := filepath.Base(includePath)
			dbs, ok := effectiveIncludeDefaults[includePath]
			if !ok {
				dbs, ok = effectiveIncludeDefaults[baseName]
			}
			if !ok {
				// Файл загружается ТОЛЬКО если для него есть запись в include_defaults.
				// Без записи — файл осознанно игнорируется. Это позволяет держать
				// файлы метрик переиспользуемыми: добавление файла в includes без
				// include_defaults безопасно ничего не делает, а не падает с
				// ошибкой "db is required".
				cfg.skippedIncludes = append(cfg.skippedIncludes,
					fmt.Sprintf("%q: no entry in include_defaults", includePath))
				continue
			}

			// Загружаем файл для каждой БД и мержим. Если БД несколько — все
			// запросы получают суффикс __db чтобы воркеры были уникальны.
			// Лейбл db в метрике всё равно различает их в Prometheus.
			addSuffix := len(dbs) > 1
			for _, db := range dbs {
				inc, err := loadConfigWithContext(fullPath, depth+1, db, effectiveIncludeDefaults, rootDir)
				if err != nil {
					return cfg, fmt.Errorf("include %q (db=%s): %w", fullPath, db, err)
				}
				if addSuffix {
					inc = cloneQueriesForDB(inc, db)
				}
				mergeConfig(&cfg, inc, includePath)
				// Переносим служебные списки из дочернего конфига наверх
				cfg.skippedIncludes = append(cfg.skippedIncludes, inc.skippedIncludes...)
				cfg.missingEnvVars = append(cfg.missingEnvVars, inc.missingEnvVars...)
				cfg.overwritten = append(cfg.overwritten, inc.overwritten...)
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
// (@ / + # % & : = пробел и др.) не ломали парсинг URL. Используется
// url.QueryEscape, так как он кодирует более широкий набор символов чем
// url.PathEscape (в частности "=", который PathEscape сознательно
// пропускает как разрешённый в path-сегменте по RFC 3986).
//
// Исключение — значения похожие на TNS-дескриптор Oracle:
// "(DESCRIPTION=(ADDRESS=...)...)" — такие значения НЕ кодируются,
// иначе скобки и = превратятся в %28 %29 %3D и resolveDSN не сможет
// распознать и распарсить TNS-дескриптор в db.go.
//
// ВАЖНО (исправленный баг): раньше отсутствующая переменная окружения
// (опечатка в имени, незапримонтированный Secret, неверный регистр —
// например ${password} в конфиге vs PASSWORD в env) тихо подставлялась
// как пустая строка через os.Getenv, который не различает "переменной нет
// вообще" и "переменная есть, но реально пустая". В результате URL
// собирался вида "oracle://user:@host:1521/service" — с пустым паролем —
// без единой ошибки на этапе загрузки конфига. Дальше Oracle совершенно
// ожидаемо отвечает ORA-01017 "invalid username/password" (соединение
// доходит до сервера — поэтому не "connection refused", просто пустой
// пароль действительно неверен), и по одному этому сообщению невозможно
// понять, что переменная окружения вовсе не была найдена.
//
// Теперь используется os.LookupEnv, который явно возвращает найдена ли
// переменная, и все ненайденные собираются в missingVars — для
// предупреждения в логах при reload (см. вызов в app.go).
func expandURLs(cfg *Config) (missingVars []string) {
	seen := make(map[string]struct{})

	for name, db := range cfg.Databases {
		db.URL = os.Expand(db.URL, func(key string) string {
			val, ok := os.LookupEnv(key)
			if !ok {
				warning := fmt.Sprintf("database %q: ${%s} is not set in environment", name, key)
				if _, dup := seen[warning]; !dup {
					seen[warning] = struct{}{}
					missingVars = append(missingVars, warning)
				}
				return ""
			}
			if val == "" {
				// Переменная явно задана как пустая строка — это может быть
				// осознанным выбором, не считаем ошибкой и не предупреждаем.
				return ""
			}
			if looksLikeTNSDescriptor(val) {
				return val // подставляем как есть, без кодирования
			}
			return url.QueryEscape(val)
		})
		cfg.Databases[name] = db
	}

	return missingVars
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

// mergeConfig мержит src в dst. src (инклюд из sourceLabel) имеет приоритет —
// перезаписывает существующие ключи. Коллизии (когда src перезаписывает
// ключ dst с ДРУГИМ значением) записываются в dst.overwritten для логирования
// в app.go — иначе такая коллизия проходит абсолютно молча: метрика
// продолжает существовать под тем же именем, но начинает собирать данные
// с другой БД или по другому SQL без единой строки в логах.
func mergeConfig(dst *Config, src Config, sourceLabel string) {
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
		if existing, exists := dst.Databases[name]; exists && existing != db {
			dst.overwritten = append(dst.overwritten,
				fmt.Sprintf("database %q overwritten by include %q", name, sourceLabel))
		}
		dst.Databases[name] = db
	}

	if dst.Queries == nil {
		dst.Queries = make(map[string]QueryConfig)
	}
	for name, q := range src.Queries {
		if existing, exists := dst.Queries[name]; exists && !sameQueryConfig(existing, q) {
			dst.overwritten = append(dst.overwritten,
				fmt.Sprintf("query %q overwritten by include %q", name, sourceLabel))
		}
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

	timeout, err := time.ParseDuration(q.Timeout)
	if err != nil {
		return fmt.Sprintf("invalid timeout: %v", err)
	}
	if timeout <= 0 {
		return "timeout must be positive"
	}

	if q.Schedule != nil && q.Interval != "" {
		return "interval and schedule are mutually exclusive"
	}
	if q.Schedule != nil {
		if err := validateSchedule(q.Schedule); err != nil {
			return fmt.Sprintf("invalid schedule: %v", err)
		}
	} else {
		interval, err := time.ParseDuration(q.Interval)
		if err != nil {
			return fmt.Sprintf("invalid interval: %v", err)
		}
		if interval <= 0 {
			return "interval must be positive"
		}
	}

	if !isValidPrometheusName(name) {
		return fmt.Sprintf("query name %q is not a valid Prometheus metric name", name)
	}
	for label := range q.Labels {
		if label == "db" || label == "env" {
			return fmt.Sprintf("static label %q is reserved (always set automatically) and cannot be overridden", label)
		}
		if !isValidPrometheusLabelName(label) {
			return fmt.Sprintf("static label %q is not a valid Prometheus label name", label)
		}
	}

	if q.MaxRows < 0 {
		return "max_rows must not be negative"
	}
	if q.MaxRows > 0 && q.ValueColumn == "" {
		return "max_rows only applies to multi-row queries (value_column must be set)"
	}

	return ""
}

// isValidPrometheusName проверяет валидность имени метрики по правилам Prometheus:
// [a-zA-Z_:][a-zA-Z0-9_:]*
func isValidPrometheusName(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		isLetter := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_' || r == ':'
		isDigit := r >= '0' && r <= '9'
		if i == 0 {
			if !isLetter {
				return false
			}
			continue
		}
		if !isLetter && !isDigit {
			return false
		}
	}
	return true
}

// isValidPrometheusLabelName проверяет валидность имени лейбла по правилам Prometheus:
// [a-zA-Z_][a-zA-Z0-9_]*, не начинается с "__" (зарезервировано для внутреннего использования).
func isValidPrometheusLabelName(name string) bool {
	if name == "" || strings.HasPrefix(name, "__") {
		return false
	}
	for i, r := range name {
		isLetter := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_'
		isDigit := r >= '0' && r <= '9'
		if i == 0 {
			if !isLetter {
				return false
			}
			continue
		}
		if !isLetter && !isDigit {
			return false
		}
	}
	return true
}

// validateDatabasesAndSettings проверяет критичные части конфига —
// настройки и описания БД. Ошибки здесь валят весь reload, так как
// означают структурно сломанный конфиг (а не опечатку в одном запросе).
func validateDatabasesAndSettings(cfg Config) error {
	if cfg.Settings.DBReconnectInterval != "" {
		d, err := time.ParseDuration(cfg.Settings.DBReconnectInterval)
		if err != nil {
			return fmt.Errorf("settings.db_reconnect_interval is invalid: %w", err)
		}
		if d <= 0 {
			return fmt.Errorf("settings.db_reconnect_interval must be positive")
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
			d, err := time.ParseDuration(db.MaxConnLifetime)
			if err != nil {
				return fmt.Errorf("database %q has invalid max_conn_lifetime: %w", name, err)
			}
			if d <= 0 {
				return fmt.Errorf("database %q: max_conn_lifetime must be positive", name)
			}
		}
		if db.MaxConnIdleTime != "" {
			d, err := time.ParseDuration(db.MaxConnIdleTime)
			if err != nil {
				return fmt.Errorf("database %q has invalid max_conn_idle_time: %w", name, err)
			}
			if d <= 0 {
				return fmt.Errorf("database %q: max_conn_idle_time must be positive", name)
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
