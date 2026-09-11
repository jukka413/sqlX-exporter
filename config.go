package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// unmarshalYAMLStrict парсит YAML с KnownFields(true) — опечатка в имени
// поля становится явной ошибкой, а не молча отброшенным значением.
// io.EOF от Decode() (пустой/закомментированный файл) не считается ошибкой.
//
// Multi-document YAML (несколько документов через "---" в одном файле,
// привычный синтаксис из Kubernetes-манифестов) явно не поддерживается —
// без этой проверки Decode() читает только первый документ, а всё после
// "---" молча теряется без единой ошибки: reload отчитается success,
// хотя часть файла даже не была прочитана.
func unmarshalYAMLStrict(data []byte, out any) error {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(out); err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("file contains more than one YAML document (separated by \"---\") — " +
				"multi-document files are not supported, only the first document would be used")
		}
		return err
	}
	return nil
}

type Config struct {
	Settings AppSettings `yaml:"settings"`

	// DefaultDB — default_db для запросов в этом файле; если пусто, берётся
	// от родителя или из settings.default_db.
	DefaultDB string `yaml:"default_db,omitempty"`

	// IncludeDefaults — БД по умолчанию для инклюд-файла (одна или несколько,
	// см. IncludeDefault). Позволяет не дублировать db: в каждом запросе.
	IncludeDefaults map[string]IncludeDefault `yaml:"include_defaults,omitempty"`

	// Includes — map, не список: Helm глубоко мержит map-поля между
	// несколькими values файлами, но полностью заменяет списки.
	Includes map[string]bool `yaml:"includes,omitempty"`

	Databases map[string]DBConfig    `yaml:"databases"`
	Queries   map[string]QueryConfig `yaml:"queries"`

	// Ниже — не из YAML, заполняется кодом при загрузке, читается в app.go
	// для логирования и построения списка watch-директорий.
	skippedIncludes []string `yaml:"-"` // инклюд без записи в include_defaults
	missingEnvVars  []string `yaml:"-"` // ${VAR} не найдена в окружении
	overwritten     []string `yaml:"-"` // database/query перезаписаны другим инклюдом
	failedIncludes  []string `yaml:"-"` // инклюд не удалось загрузить
	dependencies    []string `yaml:"-"` // абсолютные пути всех прочитанных файлов
}

// IncludeDefault поддерживает строку и список строк в YAML:
//
//	oracle-metrics.yaml: "azdh_oracle"
//	oracle-metrics.yaml: ["azdh_oracle", "ir_test_oracle"]
type IncludeDefault []string

func (id *IncludeDefault) UnmarshalYAML(value *yaml.Node) error {
	var single string
	if err := value.Decode(&single); err == nil {
		*id = IncludeDefault{single}
		return nil
	}
	var multi []string
	if err := value.Decode(&multi); err == nil {
		*id = IncludeDefault(multi)
		return nil
	}
	return fmt.Errorf("include_defaults value must be a string or list of strings")
}

type AppSettings struct {
	DBReconnectInterval string `yaml:"db_reconnect_interval,omitempty"`
	DefaultDB           string `yaml:"default_db,omitempty"`

	// Дефолты пула для databases: — применяются в applyDefaultPoolSettings,
	// когда БД не задаёт поле явно. Имена ключей совпадают с полями
	// DBConfig намеренно, для интуитивного переопределения.
	//
	// *int, не int — иначе нельзя было бы отличить "явно 0" от "не задано":
	// у Go нулевое значение int — тоже 0, и database/sql трактует
	// max_idle_conns=0 как значащее ("не держать простаивающие соединения",
	// а не дефолтные 2) — явный ноль на уровне конкретной БД должен уметь
	// переопределить ненулевой глобальный дефолт, а не потеряться в нём.
	DefaultMaxConns        *int   `yaml:"max_conns,omitempty"`
	DefaultMaxIdleConns    *int   `yaml:"max_idle_conns,omitempty"`
	DefaultMaxConnLifetime string `yaml:"max_conn_lifetime,omitempty"`
	DefaultMaxConnIdleTime string `yaml:"max_conn_idle_time,omitempty"`

	// DefaultTimezone — таймзона по умолчанию для запросов с schedule:,
	// у которых нет своего schedule.timezone. Применяется в
	// applyDefaultTimezone. Если не задано ни здесь, ни в самом запросе —
	// используется UTC (см. parseSchedule).
	DefaultTimezone string `yaml:"default_timezone,omitempty"`
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

	// Env — если задано, автоматически становится лейблом "env" на всех
	// метриках этой БД.
	Env string `yaml:"env,omitempty"`

	// *int, не int — см. комментарий у AppSettings.DefaultMaxConns.
	MaxConns     *int `yaml:"max_conns"`
	MaxIdleConns *int `yaml:"max_idle_conns"`

	MaxConnLifetime string `yaml:"max_conn_lifetime,omitempty"`
	MaxConnIdleTime string `yaml:"max_conn_idle_time,omitempty"`
}

type QueryConfig struct {
	DB       string `yaml:"db"`
	SQL      string `yaml:"sql"`
	Timeout  string `yaml:"timeout"`
	Interval string `yaml:"interval"`

	Schedule *ScheduleConfig `yaml:"schedule,omitempty"`

	Labels      map[string]string `yaml:"labels,omitempty"`
	ValueColumn string            `yaml:"value_column,omitempty"`

	// MaxRows — лимит строк для multi-row (защита от unbounded cardinality).
	// 0 — берётся defaultMaxRows из worker.go. Игнорируется для single-value.
	MaxRows int `yaml:"max_rows,omitempty"`

	// MetricName — имя запроса до клонирования на несколько БД (worker key
	// становится "name__db", MetricName остаётся чистым). Не из YAML.
	MetricName string `yaml:"-"`

	// DBEnv — Env той БД, к которой резолвится запрос. Проставляется в
	// workerManager.reconcile, не при загрузке — там впервые известна
	// связка запрос→пул. Не из YAML.
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

// loadConfig загружает конфиг из path и рекурсивно обрабатывает includes.
//
// Корневой файл читается ровно один раз, внутри loadConfigWithContext на
// depth==0 — раньше здесь был отдельный "preview"-проход, читающий тот же
// файл ещё раз только чтобы заранее вытащить default_db/include_defaults.
// Два раздельных os.ReadFile одного и того же файла не атомарны друг
// относительно друга: если содержимое на диске поменяется между ними
// (например Kubernetes переключил ..data ровно в этот момент), итоговый
// cfg мог оказаться собран из ДВУХ разных версий файла одновременно —
// в частности mergeIncludeDefaults(base, override) только перезаписывает
// совпадающие ключи, а не заменяет карту целиком, поэтому ключ, удалённый
// в новой версии, мог "воскреснуть" из значения preview-прохода, снятого
// со старой.
func loadConfig(path string) (Config, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		absPath = path
	}
	rootDir := filepath.Dir(absPath)

	cfg, err := loadConfigWithContext(path, 0, "", nil, rootDir, false)
	if err != nil {
		return cfg, err
	}
	normalizeDrivers(&cfg)
	applyDefaultPoolSettings(&cfg)
	applyDefaultTimezone(&cfg)
	return cfg, nil
}

// normalizeDrivers приводит driver каждой БД к нижнему регистру и обрезает
// пробелы. sql.Open требует точного совпадения регистра с именем, под
// которым драйвер зарегистрировал себя (см. drivers.go) — без этого
// "driver: PGX" проходил бы валидацию, но падал в рантайме.
func normalizeDrivers(cfg *Config) {
	for name, db := range cfg.Databases {
		db.Driver = strings.ToLower(strings.TrimSpace(db.Driver))
		cfg.Databases[name] = db
	}
}

// applyDefaultPoolSettings проставляет дефолты пула из settings: в БД без
// явного значения. Вызывается после полного мержа инклюдов — settings.*
// сам может быть переопределён более поздним инклюдом.
func applyDefaultPoolSettings(cfg *Config) {
	s := cfg.Settings
	for name, db := range cfg.Databases {
		if db.MaxConns == nil {
			db.MaxConns = s.DefaultMaxConns
		}
		if db.MaxIdleConns == nil {
			db.MaxIdleConns = s.DefaultMaxIdleConns
		}
		if db.MaxConnLifetime == "" {
			db.MaxConnLifetime = s.DefaultMaxConnLifetime
		}
		if db.MaxConnIdleTime == "" {
			db.MaxConnIdleTime = s.DefaultMaxConnIdleTime
		}
		cfg.Databases[name] = db
	}
}

// applyDefaultTimezone проставляет settings.default_timezone в schedule
// каждого запроса, у которого нет своего schedule.timezone. Явное значение
// в самом запросе всегда побеждает. Если не задано нигде — parseSchedule
// возьмёт UTC.
func applyDefaultTimezone(cfg *Config) {
	if cfg.Settings.DefaultTimezone == "" {
		return
	}
	for name, q := range cfg.Queries {
		if q.Schedule != nil && q.Schedule.Timezone == "" {
			q.Schedule.Timezone = cfg.Settings.DefaultTimezone
			cfg.Queries[name] = q
		}
	}
}

const maxIncludeDepth = 10

// loadConfigWithContext загружает один файл конфига и рекурсивно — его
// инклюды. rootDir — директория корневого конфига; все инклюды обязаны
// резолвиться внутри неё (защита от path traversal через "../..").
// rejectExplicitDB — true когда этот файл обрабатывается в рамках
// multi-DB include_defaults (один файл на несколько БД сразу): в этом
// режиме запросы не имеют права задавать db: явно (см. проверку ниже).
func loadConfigWithContext(path string, depth int, parentDefaultDB string, parentIncludeDefaults map[string]IncludeDefault, rootDir string, rejectExplicitDB bool) (Config, error) {
	var cfg Config

	if depth > maxIncludeDepth {
		return cfg, fmt.Errorf("include depth limit (%d) exceeded at %q — possible circular include", maxIncludeDepth, path)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}

	if err := unmarshalYAMLStrict(data, &cfg); err != nil {
		return cfg, fmt.Errorf("%s: %w", path, err)
	}

	if absSelf, err := filepath.Abs(path); err == nil {
		cfg.dependencies = append(cfg.dependencies, filepath.Clean(absSelf))
	}

	cfg.missingEnvVars = expandURLs(&cfg)

	// MetricName проставляется до клонирования на несколько БД.
	for name, q := range cfg.Queries {
		if q.MetricName == "" {
			q.MetricName = name
			cfg.Queries[name] = q
		}
	}

	// Проверяем ДО applyDefaultDB ниже — только тут ещё можно отличить
	// "пользователь сам написал db: в YAML" от "db: сейчас подставит
	// include_defaults". После applyDefaultDB оба случая неотличимы —
	// оба дают непустой q.DB, и проверка постфактум (как было раньше)
	// ложно срабатывала бы на КАЖДЫЙ запрос, которому include_defaults
	// только что законно проставил db:, а не на реально написанный вручную.
	if rejectExplicitDB {
		for name, q := range cfg.Queries {
			if q.DB != "" {
				return cfg, fmt.Errorf("query %q has an explicit db %q, but this include is used for "+
					"multiple databases via include_defaults — explicit db: is not allowed here, it would "+
					"run the same query against the same database multiple times", name, q.DB)
			}
		}
	}

	// settings.default_db как фолбэк — только для корневого файла (depth==0).
	// Раньше это вычислялось в loadConfig() из отдельного preview-чтения;
	// теперь берётся из этого же, единственного чтения cfg. Ограничение по
	// depth сохраняет прежнюю семантику: settings.default_db инклюда сам по
	// себе не становится "родительским" фолбэком для остальных файлов —
	// таким фолбэком был (и остаётся) только default_db/settings.default_db
	// корневого конфига.
	effectiveDefaultDB := cfg.DefaultDB
	if depth == 0 && effectiveDefaultDB == "" {
		effectiveDefaultDB = cfg.Settings.DefaultDB
	}
	if effectiveDefaultDB == "" {
		effectiveDefaultDB = parentDefaultDB
	}
	if effectiveDefaultDB != "" {
		applyDefaultDB(&cfg, effectiveDefaultDB)
	}

	effectiveIncludeDefaults := mergeIncludeDefaults(parentIncludeDefaults, cfg.IncludeDefaults)

	if len(cfg.Includes) > 0 {
		baseDir := filepath.Dir(path)
		includePaths := make([]string, 0, len(cfg.Includes))
		for p := range cfg.Includes {
			includePaths = append(includePaths, p)
		}
		sort.Strings(includePaths) // детерминированный порядок мержа

		for _, includePath := range includePaths {
			if !cfg.Includes[includePath] {
				continue // явно выключен
			}

			fullPath := filepath.Join(baseDir, includePath)

			absFullPath, err := filepath.Abs(fullPath)
			if err != nil {
				return cfg, fmt.Errorf("include %q: cannot resolve absolute path: %w", includePath, err)
			}
			rel, err := filepath.Rel(rootDir, absFullPath)
			if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				return cfg, fmt.Errorf("include %q resolves outside the config root directory %q", includePath, rootDir)
			}

			// Регистрируем как dependency ДО попытки загрузки, а не после —
			// если файл сейчас отсутствует или содержит невалидный YAML,
			// loadConfigWithContext ниже вернёт ошибку и никогда не дойдёт
			// до своего собственного самостоятельного добавления в
			// dependencies. Без этой строки watcher не знал бы, что нужно
			// следить за директорией сломанного инклюда — и когда его позже
			// починят, reload не сработал бы сам по себе, только по
			// какой-то другой, не связанной причине.
			cfg.dependencies = append(cfg.dependencies, absFullPath)

			// Сначала полный путь, потом basename — иначе team-a/x.yaml и
			// team-b/x.yaml делили бы одну запись в include_defaults.
			baseName := filepath.Base(includePath)
			dbs, ok := effectiveIncludeDefaults[includePath]
			if !ok {
				dbs, ok = effectiveIncludeDefaults[baseName]
			}
			if !ok {
				cfg.skippedIncludes = append(cfg.skippedIncludes,
					fmt.Sprintf("%q: no entry in include_defaults", includePath))
				continue
			}
			if len(dbs) == 0 {
				// Запись есть, но список пуст ("file.yaml: []") — цикл ниже
				// не выполнится ни разу; без этой проверки инклюд тихо
				// пропадал бы, не попадая никуда.
				cfg.skippedIncludes = append(cfg.skippedIncludes,
					fmt.Sprintf("%q: include_defaults entry is an empty list — no databases to apply this file to", includePath))
				continue
			}

			addSuffix := len(dbs) > 1
			for _, db := range dbs {
				// rejectExplicitDB || addSuffix — прилипает: если этот файл сам
				// обрабатывается под внешним multi-DB (rejectExplicitDB=true),
				// это ограничение должно распространяться и на его собственные
				// вложенные инклюды, а не только на его прямые queries — иначе
				// явный db: там ускользнул бы от проверки, а потом всё равно
				// попал бы под клонирование на внешнем уровне.
				inc, err := loadConfigWithContext(fullPath, depth+1, db, effectiveIncludeDefaults, rootDir, rejectExplicitDB || addSuffix)
				if err != nil {
					// Один сломанный инклюд не должен блокировать остальные —
					// его databases/queries просто отсутствуют в итоге.
					cfg.failedIncludes = append(cfg.failedIncludes,
						fmt.Sprintf("%q (db=%s): %v", fullPath, db, err))
					continue
				}
				if addSuffix {
					inc = cloneQueriesForDB(inc, db)
				}
				mergeConfig(&cfg, inc, includePath)
				cfg.skippedIncludes = append(cfg.skippedIncludes, inc.skippedIncludes...)
				cfg.missingEnvVars = append(cfg.missingEnvVars, inc.missingEnvVars...)
				cfg.overwritten = append(cfg.overwritten, inc.overwritten...)
				cfg.failedIncludes = append(cfg.failedIncludes, inc.failedIncludes...)
				cfg.dependencies = append(cfg.dependencies, inc.dependencies...)
			}
		}
		cfg.Includes = nil
	}

	return cfg, nil
}

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

func applyDefaultDB(cfg *Config, defaultDB string) {
	for name, q := range cfg.Queries {
		if q.DB == "" {
			q.DB = defaultDB
			cfg.Queries[name] = q
		}
	}
}

// expandURLs подставляет переменные окружения в url баз данных. Отсутствующая
// переменная (LookupEnv, не Getenv — различает "нет" от "пустая") собирается
// в missingVars вместо тихой подстановки пустой строки.
//
// Кодирование зависит от драйвера: MySQL DSN не кодируется вообще (это не
// URL, драйвер берёт пароль как есть — см. go-sql-driver/mysql docs), TNS-
// дескрипторы Oracle не кодируются (иначе ломается их грамматика), остальное
// — через escapeURLComponent.
// expandBracedOnly заменяет только строгий синтаксис ${VAR_NAME} — в
// отличие от os.Expand, НЕ трогает голый $VAR без фигурных скобок.
// os.Expand обрабатывает оба варианта; это опасно для литерального,
// зашитого в YAML пароля вроде "MyP@ss$word123" — голый "$word123" был
// бы воспринят как ссылка на переменную окружения "word123", и, не найдя
// такую, тихо заменён пустой строкой, испортив пароль ещё до попытки
// подключения. Символ "$" сам по себе (не начинающий "${") копируется
// в результат как есть.
func expandBracedOnly(s string, mapping func(string) string) string {
	var sb strings.Builder
	for i := 0; i < len(s); {
		if s[i] == '$' && i+1 < len(s) && s[i+1] == '{' {
			if end := strings.IndexByte(s[i+2:], '}'); end != -1 {
				sb.WriteString(mapping(s[i+2 : i+2+end]))
				i += 2 + end + 1
				continue
			}
		}
		sb.WriteByte(s[i])
		i++
	}
	return sb.String()
}

func expandURLs(cfg *Config) (missingVars []string) {
	seen := make(map[string]struct{})

	for name, db := range cfg.Databases {
		isMySQL := strings.EqualFold(strings.TrimSpace(db.Driver), "mysql")

		db.URL = expandBracedOnly(db.URL, func(key string) string {
			val, ok := os.LookupEnv(key)
			if !ok {
				warning := fmt.Sprintf("database %q: ${%s} is not set in environment", name, key)
				if _, dup := seen[warning]; !dup {
					seen[warning] = struct{}{}
					missingVars = append(missingVars, warning)
				}
				return ""
			}
			if val == "" || looksLikeTNSDescriptor(val) || isMySQL {
				return val
			}
			return escapeURLComponent(val)
		})
		cfg.Databases[name] = db
	}

	return missingVars
}

// escapeURLComponent percent-кодирует всё кроме unreserved (RFC 3986).
// Не url.QueryEscape (кодирует пробел как "+", а "+" в userinfo — литерал)
// и не url.PathEscape (оставляет "=" и "&" нетронутыми).
func escapeURLComponent(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		unreserved := (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') || c == '-' || c == '.' || c == '_' || c == '~'
		if unreserved {
			b.WriteByte(c)
			continue
		}
		const hex = "0123456789ABCDEF"
		b.WriteByte('%')
		b.WriteByte(hex[c>>4])
		b.WriteByte(hex[c&0x0F])
	}
	return b.String()
}

func looksLikeTNSDescriptor(val string) bool {
	trimmed := strings.TrimSpace(val)
	if !strings.HasPrefix(trimmed, "(") {
		return false
	}
	upper := strings.ToUpper(trimmed)
	return strings.Contains(upper, "DESCRIPTION=") || strings.Contains(upper, "ADDRESS=")
}

// cloneQueriesForDB даёт каждому ключу суффикс "__db" для уникальности
// воркеров; MetricName остаётся оригинальным (метрика в Prometheus без суффикса).
func cloneQueriesForDB(cfg Config, db string) Config {
	if len(cfg.Queries) == 0 {
		return cfg
	}
	newQueries := make(map[string]QueryConfig, len(cfg.Queries))
	for name, q := range cfg.Queries {
		newQueries[name+"__"+db] = q
	}
	cfg.Queries = newQueries
	return cfg
}

// mergeConfig мержит src в dst; src (инклюд sourceLabel) побеждает при
// коллизии ключей. Коллизия с ДРУГИМ значением логируется в dst.overwritten.
func mergeConfig(dst *Config, src Config, sourceLabel string) {
	if src.Settings.DBReconnectInterval != "" {
		dst.Settings.DBReconnectInterval = src.Settings.DBReconnectInterval
	}
	if src.Settings.DefaultDB != "" {
		dst.Settings.DefaultDB = src.Settings.DefaultDB
	}
	if src.Settings.DefaultTimezone != "" {
		dst.Settings.DefaultTimezone = src.Settings.DefaultTimezone
	}
	if src.Settings.DefaultMaxConns != nil {
		dst.Settings.DefaultMaxConns = src.Settings.DefaultMaxConns
	}
	if src.Settings.DefaultMaxIdleConns != nil {
		dst.Settings.DefaultMaxIdleConns = src.Settings.DefaultMaxIdleConns
	}
	if src.Settings.DefaultMaxConnLifetime != "" {
		dst.Settings.DefaultMaxConnLifetime = src.Settings.DefaultMaxConnLifetime
	}
	if src.Settings.DefaultMaxConnIdleTime != "" {
		dst.Settings.DefaultMaxConnIdleTime = src.Settings.DefaultMaxConnIdleTime
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

// sanitizeQueries проверяет каждый запрос независимо, удаляя невалидные из
// cfg.Queries вместо того чтобы валить весь конфиг одной ошибкой.
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

func validateSingleQuery(cfg *Config, name string, q QueryConfig) string {
	if strings.TrimSpace(q.SQL) == "" {
		return "sql is required"
	}
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

	// MetricName, не name — после cloneQueriesForDB name может быть
	// составным worker ID ("query__db"), а не именем метрики.
	metricName := q.MetricName
	if metricName == "" {
		metricName = name
	}
	if !isValidPrometheusName(metricName) {
		return fmt.Sprintf("metric name %q is not a valid Prometheus metric name", metricName)
	}
	if reserved := reservedMetricPrefix(metricName); reserved != "" {
		return fmt.Sprintf("metric name %q uses the reserved prefix %q — reserved for the exporter's "+
			"own internal metrics (app_) or Prometheus/Go runtime collectors (process_, go_, scrape_), "+
			"pick a different name to avoid colliding with an existing metric", metricName, reserved)
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

// isValidPrometheusName: [a-zA-Z_][a-zA-Z0-9_]*. Без ":" — формально
// Prometheus его допускает в именах метрик, но резервирует за recording
// rules и рекомендует экспортёрам не использовать; это только соглашение,
// нарушение которого ничего не ломает функционально, в отличие от
// reservedMetricPrefix ниже.
func isValidPrometheusName(name string) bool {
	if name == "" {
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

// reservedMetricPrefix возвращает непустой префикс, если metricName в него
// попадает — иначе "". В отличие от isValidPrometheusName (синтаксис),
// это про конкретный, функциональный риск: "app_" — метрики самого
// экспортёра (app_query_up и т.д.), "process_"/"go_" — встроенные
// коллекторы client_golang (ProcessCollector/GoCollector), "scrape_" —
// добавляется самим Prometheus-сервером при скрейпе любой цели. Запрос
// пользователя с таким именем либо получит явную ошибку регистрации
// (если лейблы не совпали с уже существующим коллектором), либо —
// что хуже — "усыновит" чужой коллектор через AlreadyRegisteredError и
// начнёт молча писать значения SQL-запроса в наш собственный внутренний
// сигнал здоровья.
func reservedMetricPrefix(metricName string) string {
	for _, prefix := range []string{"app_", "process_", "go_", "scrape_"} {
		if strings.HasPrefix(metricName, prefix) {
			return prefix
		}
	}
	return ""
}

// isValidPrometheusLabelName: [a-zA-Z_][a-zA-Z0-9_]*, без "__" в начале.
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

// supportedDrivers должен совпадать с тем, что реально регистрируется
// импортами в drivers.go.
var supportedDrivers = map[string]bool{
	"mysql":     true,
	"oracle":    true,
	"pgx":       true,
	"sqlserver": true,
}

// validateDatabasesAndSettings проверяет структурные части конфига.
// Ошибки здесь валят весь reload — в отличие от sanitizeQueries, они
// означают что конфиг сломан целиком, а не что у одного запроса опечатка.
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
	if cfg.Settings.DefaultTimezone != "" {
		if _, err := time.LoadLocation(cfg.Settings.DefaultTimezone); err != nil {
			return fmt.Errorf("settings.default_timezone is invalid: %w", err)
		}
	}
	// Валидируем сами дефолты пула отдельно — иначе ошибка вроде
	// settings.max_conns: -1 всплыла бы как ошибка каждой отдельной БД.
	if cfg.Settings.DefaultMaxConns != nil && *cfg.Settings.DefaultMaxConns < 0 {
		return fmt.Errorf("settings.max_conns must not be negative")
	}
	if cfg.Settings.DefaultMaxIdleConns != nil && *cfg.Settings.DefaultMaxIdleConns < 0 {
		return fmt.Errorf("settings.max_idle_conns must not be negative")
	}
	if cfg.Settings.DefaultMaxConnLifetime != "" {
		if _, err := time.ParseDuration(cfg.Settings.DefaultMaxConnLifetime); err != nil {
			return fmt.Errorf("settings.max_conn_lifetime is invalid: %w", err)
		}
	}
	if cfg.Settings.DefaultMaxConnIdleTime != "" {
		if _, err := time.ParseDuration(cfg.Settings.DefaultMaxConnIdleTime); err != nil {
			return fmt.Errorf("settings.max_conn_idle_time is invalid: %w", err)
		}
	}

	for name, db := range cfg.Databases {
		if db.Driver == "" {
			return fmt.Errorf("database %q: driver is required", name)
		}
		if !supportedDrivers[strings.ToLower(strings.TrimSpace(db.Driver))] {
			return fmt.Errorf("database %q: unknown driver %q (supported: mysql, oracle, pgx, sqlserver)",
				name, db.Driver)
		}
		if db.URL == "" {
			return fmt.Errorf("database %q: url is required", name)
		}
		// database/sql трактует отрицательные значения как "без ограничений".
		if db.MaxConns != nil && *db.MaxConns < 0 {
			return fmt.Errorf("database %q: max_conns must not be negative", name)
		}
		if db.MaxIdleConns != nil && *db.MaxIdleConns < 0 {
			return fmt.Errorf("database %q: max_idle_conns must not be negative", name)
		}
		if db.MaxIdleConns != nil && db.MaxConns != nil &&
			*db.MaxIdleConns > 0 && *db.MaxConns > 0 && *db.MaxIdleConns > *db.MaxConns {
			return fmt.Errorf("database %q: max_idle_conns (%d) must be <= max_conns (%d)",
				name, *db.MaxIdleConns, *db.MaxConns)
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
	}
	return nil
}
