package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeFiles создаёт файлы во временном каталоге (удаляется автоматически) и
// возвращает путь к нему. Имена могут содержать подкаталоги.
func writeFiles(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// mustLoad загружает config.yaml из набора файлов и падает, если loadConfig вернул ошибку.
func mustLoad(t *testing.T, files map[string]string) Config {
	t.Helper()
	dir := writeFiles(t, files)
	cfg, err := loadConfig(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	return cfg
}

func containsAny(list []string, sub string) bool {
	for _, s := range list {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func intp(n int) *int { return &n }

func intVal(t *testing.T, p *int) int {
	t.Helper()
	if p == nil {
		t.Fatal("ожидалось значение, получили nil")
	}
	return *p
}

// --- Строгий YAML -----------------------------------------------------------

func TestUnmarshalYAMLStrict(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantErr string // подстрока ошибки; "" — ошибки быть не должно
		fail    bool   // ошибка нужна, но текст не проверяем
	}{
		{name: "корректный файл", in: "databases:\n  a:\n    driver: pgx\n    url: x\n"},
		{name: "пустой файл", in: ""},
		{name: "только комментарий", in: "# ничего нет\n"},
		{name: "ведущий --- это один документ", in: "---\ndatabases: {}\n"},
		{name: "опечатка в поле верхнего уровня", in: "databases: {}\nqueriez: {}\n", wantErr: "queriez"},
		{name: "опечатка во вложенном поле", in: "queries:\n  q:\n    sqll: select 1\n", wantErr: "sqll"},
		{name: "несколько документов", in: "databases: {}\n---\nqueries: {}\n", wantErr: "more than one YAML document"},
		{name: "неверный тип", in: "databases: 5\n", fail: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg Config
			err := unmarshalYAMLStrict([]byte(tt.in), &cfg)
			switch {
			case tt.wantErr != "":
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("ожидали ошибку с %q, получили: %v", tt.wantErr, err)
				}
			case tt.fail:
				if err == nil {
					t.Fatal("ожидали ошибку, получили nil")
				}
			default:
				if err != nil {
					t.Fatalf("неожиданная ошибка: %v", err)
				}
			}
		})
	}
}

func TestLoadConfig_ErrorMentionsFileName(t *testing.T) {
	dir := writeFiles(t, map[string]string{"config.yaml": "databases: {}\n---\nqueries: {}\n"})
	_, err := loadConfig(filepath.Join(dir, "config.yaml"))
	if err == nil {
		t.Fatal("ожидали ошибку для multi-document файла")
	}
	if !strings.Contains(err.Error(), "config.yaml") || !strings.Contains(err.Error(), "more than one YAML document") {
		t.Fatalf("ошибка должна называть файл и причину, получили: %v", err)
	}
}

// --- Загрузка и default_db ---------------------------------------------------

func TestLoadConfig_DefaultDBFromSettings(t *testing.T) {
	cfg := mustLoad(t, map[string]string{"config.yaml": `
settings:
  default_db: main
databases:
  main: {driver: pgx, url: "postgres://u:p@h/db"}
queries:
  q1: {sql: "select 1", timeout: 5s, interval: 10s}
  q2: {db: other, sql: "select 2", timeout: 5s, interval: 10s}
`})
	if got := cfg.Queries["q1"].DB; got != "main" {
		t.Errorf("q1.DB = %q, ожидали main (из settings.default_db)", got)
	}
	if got := cfg.Queries["q2"].DB; got != "other" {
		t.Errorf("q2.DB = %q, явный db: не должен перезаписываться", got)
	}
	if got := cfg.Queries["q1"].MetricName; got != "q1" {
		t.Errorf("q1.MetricName = %q, ожидали q1", got)
	}
}

// --- Includes ---------------------------------------------------------------

const incQuery = `
queries:
  q:
    sql: select 1
    timeout: 5s
    interval: 10s
`

func TestLoadConfig_IncludeSingleDB(t *testing.T) {
	cfg := mustLoad(t, map[string]string{
		"config.yaml": `
include_defaults:
  a.yaml: db1
includes:
  a.yaml: true
databases:
  db1: {driver: pgx, url: "postgres://x"}
`,
		"a.yaml": incQuery,
	})
	if len(cfg.Queries) != 1 {
		t.Fatalf("ожидали 1 запрос, получили %d: %v", len(cfg.Queries), cfg.Queries)
	}
	q, ok := cfg.Queries["q"]
	if !ok || q.DB != "db1" {
		t.Fatalf("запрос q должен быть привязан к db1 без суффикса, получили %+v (ok=%v)", q, ok)
	}
	if len(cfg.failedIncludes) != 0 || len(cfg.skippedIncludes) != 0 {
		t.Errorf("failed=%v skipped=%v, ожидали пусто", cfg.failedIncludes, cfg.skippedIncludes)
	}
}

// Регрессия: один файл метрик на несколько БД без явного db: в YAML не должен
// отвергаться как "явный db:" (проверка обязана идти до подстановки default db).
func TestLoadConfig_IncludeMultiDB(t *testing.T) {
	cfg := mustLoad(t, map[string]string{
		"config.yaml": `
include_defaults:
  a.yaml: [db1, db2]
includes:
  a.yaml: true
databases:
  db1: {driver: pgx, url: "postgres://x"}
  db2: {driver: pgx, url: "postgres://y"}
`,
		"a.yaml": incQuery,
	})
	if len(cfg.failedIncludes) != 0 {
		t.Fatalf("include не должен падать: %v", cfg.failedIncludes)
	}
	if len(cfg.Queries) != 2 {
		t.Fatalf("ожидали 2 запроса (по одному на БД), получили %d: %v", len(cfg.Queries), cfg.Queries)
	}
	for _, db := range []string{"db1", "db2"} {
		q, ok := cfg.Queries["q__"+db]
		if !ok {
			t.Fatalf("нет запроса q__%s, есть: %v", db, cfg.Queries)
		}
		if q.DB != db {
			t.Errorf("q__%s.DB = %q, ожидали %q", db, q.DB, db)
		}
		if q.MetricName != "q" {
			t.Errorf("q__%s.MetricName = %q: имя метрики не должно получать суффикс", db, q.MetricName)
		}
	}
	if _, bad := cfg.Queries["q"]; bad {
		t.Error("запрос без суффикса не должен существовать при нескольких БД")
	}
}

func TestLoadConfig_MultiDBRejectsExplicitDB(t *testing.T) {
	cfg := mustLoad(t, map[string]string{
		"config.yaml": `
include_defaults:
  a.yaml: [db1, db2]
includes:
  a.yaml: true
databases:
  db1: {driver: pgx, url: "postgres://x"}
  db2: {driver: pgx, url: "postgres://y"}
`,
		"a.yaml": `
queries:
  q:
    db: db1
    sql: select 1
    timeout: 5s
    interval: 10s
`,
	})
	if !containsAny(cfg.failedIncludes, "explicit db") {
		t.Fatalf("ожидали ошибку про явный db: в failedIncludes, получили %v", cfg.failedIncludes)
	}
	if len(cfg.Queries) != 0 {
		t.Errorf("запросы отвергнутого include не должны попасть в конфиг: %v", cfg.Queries)
	}
}

// Запрет явного db: прилипает к вложенным include'ам multi-DB файла.
func TestLoadConfig_MultiDBRejectionIsStickyForNestedIncludes(t *testing.T) {
	cfg := mustLoad(t, map[string]string{
		"config.yaml": `
include_defaults:
  a.yaml: [db1, db2]
  b.yaml: db1
includes:
  a.yaml: true
databases:
  db1: {driver: pgx, url: "postgres://x"}
  db2: {driver: pgx, url: "postgres://y"}
`,
		"a.yaml": `
includes:
  b.yaml: true
`,
		"b.yaml": `
queries:
  q:
    db: db1
    sql: select 1
    timeout: 5s
    interval: 10s
`,
	})
	if !containsAny(cfg.failedIncludes, "explicit db") {
		t.Fatalf("вложенный include с явным db: должен быть отвергнут, failedIncludes=%v", cfg.failedIncludes)
	}
	if len(cfg.Queries) != 0 {
		t.Errorf("запросы не должны попасть в конфиг: %v", cfg.Queries)
	}
}

func TestLoadConfig_SingleDBIncludeAllowsExplicitDB(t *testing.T) {
	cfg := mustLoad(t, map[string]string{
		"config.yaml": `
include_defaults:
  a.yaml: db1
includes:
  a.yaml: true
`,
		"a.yaml": `
queries:
  q:
    db: other
    sql: select 1
    timeout: 5s
    interval: 10s
`,
	})
	if len(cfg.failedIncludes) != 0 {
		t.Fatalf("не ожидали ошибок: %v", cfg.failedIncludes)
	}
	if got := cfg.Queries["q"].DB; got != "other" {
		t.Errorf("q.DB = %q, явное значение должно сохраниться", got)
	}
}

func TestLoadConfig_SettingsIgnoredInInclude(t *testing.T) {
	cfg := mustLoad(t, map[string]string{
		"config.yaml": `
settings:
  default_db: db1
include_defaults:
  a.yaml: db1
includes:
  a.yaml: true
`,
		"a.yaml": `
settings:
  default_db: zzz
  max_conns: 99
`,
	})
	if !containsAny(cfg.ignoredSettings, "only honored in the root config") {
		t.Fatalf("ожидали предупреждение в ignoredSettings, получили %v", cfg.ignoredSettings)
	}
	if cfg.Settings.DefaultDB != "db1" {
		t.Errorf("settings.default_db = %q, значение из include не должно просачиваться", cfg.Settings.DefaultDB)
	}
	if cfg.Settings.DefaultMaxConns != nil {
		t.Errorf("settings.max_conns из include не должен применяться, получили %d", *cfg.Settings.DefaultMaxConns)
	}
}

func TestLoadConfig_SkippedAndDisabledIncludes(t *testing.T) {
	cfg := mustLoad(t, map[string]string{"config.yaml": `
include_defaults:
  b.yaml: []
includes:
  a.yaml: true
  b.yaml: true
  c.yaml: false
`})
	if !containsAny(cfg.skippedIncludes, "a.yaml") || !containsAny(cfg.skippedIncludes, "no entry in include_defaults") {
		t.Errorf("a.yaml без записи в include_defaults должен попасть в skipped: %v", cfg.skippedIncludes)
	}
	if !containsAny(cfg.skippedIncludes, "empty list") {
		t.Errorf("b.yaml с пустым списком БД должен попасть в skipped: %v", cfg.skippedIncludes)
	}
	if containsAny(cfg.skippedIncludes, "c.yaml") || containsAny(cfg.dependencies, "c.yaml") {
		t.Errorf("выключенный include (false) не должен ни пропускаться, ни считаться зависимостью")
	}
}

func TestLoadConfig_IncludeOutsideRootIsRejected(t *testing.T) {
	dir := writeFiles(t, map[string]string{"config.yaml": `
include_defaults:
  "../x.yaml": db1
includes:
  "../x.yaml": true
`})
	_, err := loadConfig(filepath.Join(dir, "config.yaml"))
	if err == nil || !strings.Contains(err.Error(), "outside the config root") {
		t.Fatalf("ожидали ошибку про выход за корень конфига, получили: %v", err)
	}
}

func TestLoadConfig_MissingIncludeIsSoftFailure(t *testing.T) {
	cfg := mustLoad(t, map[string]string{"config.yaml": `
include_defaults:
  missing.yaml: db1
includes:
  missing.yaml: true
`})
	if len(cfg.failedIncludes) != 1 {
		t.Fatalf("ожидали ровно один failed include, получили %v", cfg.failedIncludes)
	}
	// Файл должен быть в зависимостях, иначе watcher не заметит, когда его создадут.
	if !containsAny(cfg.dependencies, "missing.yaml") {
		t.Errorf("отсутствующий include должен быть в dependencies: %v", cfg.dependencies)
	}
}

func TestLoadConfig_CircularIncludeHitsDepthLimit(t *testing.T) {
	cfg := mustLoad(t, map[string]string{"config.yaml": `
include_defaults:
  config.yaml: db1
includes:
  config.yaml: true
`})
	if !containsAny(cfg.failedIncludes, "depth limit") {
		t.Fatalf("ожидали ошибку про глубину include, получили %v", cfg.failedIncludes)
	}
}

func TestLoadConfig_OverwriteDetection(t *testing.T) {
	root := `
include_defaults:
  a.yaml: d
  b.yaml: d
includes:
  a.yaml: true
  b.yaml: true
`
	db := func(url string) string {
		return "databases:\n  d: {driver: pgx, url: \"" + url + "\", max_conns: 10}\n"
	}

	t.Run("одинаковое определение не считается перезаписью", func(t *testing.T) {
		// Каждый файл декодируется отдельно, значит у max_conns разные *int —
		// сравнение должно идти по значению, а не по адресу.
		cfg := mustLoad(t, map[string]string{"config.yaml": root, "a.yaml": db("postgres://x"), "b.yaml": db("postgres://x")})
		if len(cfg.overwritten) != 0 {
			t.Fatalf("ложное предупреждение о перезаписи: %v", cfg.overwritten)
		}
	})
	t.Run("разные url — перезапись", func(t *testing.T) {
		cfg := mustLoad(t, map[string]string{"config.yaml": root, "a.yaml": db("postgres://x"), "b.yaml": db("postgres://y")})
		if !containsAny(cfg.overwritten, `database "d" overwritten by include`) {
			t.Fatalf("ожидали предупреждение о перезаписи, получили %v", cfg.overwritten)
		}
		if cfg.Databases["d"].URL != "postgres://y" {
			t.Errorf("побеждать должен более поздний include (b.yaml), получили %q", cfg.Databases["d"].URL)
		}
	})
}

// --- Переменные окружения -----------------------------------------------------

func TestExpandBracedOnly(t *testing.T) {
	m := func(key string) string { return "<" + key + ">" }
	tests := []struct{ in, want string }{
		{"a${X}b", "a<X>b"},
		{"${A}${B}", "<A><B>"},
		{"bare $word stays", "bare $word stays"},
		{"MyP@ss$word123", "MyP@ss$word123"},
		{"unterminated ${X", "unterminated ${X"},
		{"dollar at end $", "dollar at end $"},
		{"", ""},
	}
	for _, tt := range tests {
		if got := expandBracedOnly(tt.in, m); got != tt.want {
			t.Errorf("expandBracedOnly(%q) = %q, ожидали %q", tt.in, got, tt.want)
		}
	}
}

func TestEscapeURLComponent(t *testing.T) {
	tests := []struct{ in, want string }{
		{"abcXYZ019-._~", "abcXYZ019-._~"},
		{"a b+c/d=e&f", "a%20b%2Bc%2Fd%3De%26f"},
		{"p@ss:word", "p%40ss%3Aword"},
		{"ä", "%C3%A4"},
		{"", ""},
	}
	for _, tt := range tests {
		if got := escapeURLComponent(tt.in); got != tt.want {
			t.Errorf("escapeURLComponent(%q) = %q, ожидали %q", tt.in, got, tt.want)
		}
	}
}

func TestExpandURLs(t *testing.T) {
	t.Setenv("SQLX_T_PLAIN", "secret")
	t.Setenv("SQLX_T_SPECIAL", "p@ss w/rd:1")
	t.Setenv("SQLX_T_EMPTY", "")
	const tns = "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=h)(PORT=1521)))"
	t.Setenv("SQLX_T_TNS", tns)
	if err := os.Unsetenv("SQLX_T_MISSING"); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name, driver, url, want string
	}{
		{"обычное значение", "pgx", "postgres://u:${SQLX_T_PLAIN}@h/db", "postgres://u:secret@h/db"},
		{"спецсимволы кодируются", "pgx", "postgres://u:${SQLX_T_SPECIAL}@h/db", "postgres://u:p%40ss%20w%2Frd%3A1@h/db"},
		{"пустая переменная", "pgx", "postgres://u:${SQLX_T_EMPTY}@h/db", "postgres://u:@h/db"},
		{"голый $ не трогается", "pgx", "postgres://u:abc$word123@h/db", "postgres://u:abc$word123@h/db"},
		{"незакрытая ${ остаётся", "pgx", "postgres://u:${SQLX_T_PLAIN@h/db", "postgres://u:${SQLX_T_PLAIN@h/db"},
		{"MySQL DSN не кодируется", "mysql", "u:${SQLX_T_SPECIAL}@tcp(h:3306)/db", "u:p@ss w/rd:1@tcp(h:3306)/db"},
		{"TNS-дескриптор не кодируется", "oracle", "oracle+tns://u:x@${SQLX_T_TNS}", "oracle+tns://u:x@" + tns},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{Databases: map[string]DBConfig{"d": {Driver: tt.driver, URL: tt.url}}}
			if missing := expandURLs(cfg); len(missing) != 0 {
				t.Fatalf("неожиданные missing: %v", missing)
			}
			if got := cfg.Databases["d"].URL; got != tt.want {
				t.Errorf("url = %q, ожидали %q", got, tt.want)
			}
		})
	}

	t.Run("отсутствующая переменная", func(t *testing.T) {
		cfg := &Config{Databases: map[string]DBConfig{
			"d": {Driver: "pgx", URL: "postgres://${SQLX_T_MISSING}:${SQLX_T_MISSING}@h/db"},
		}}
		missing := expandURLs(cfg)
		if len(missing) != 1 || !strings.Contains(missing[0], "SQLX_T_MISSING") {
			t.Fatalf("ожидали одно (без дублей) предупреждение про SQLX_T_MISSING, получили %v", missing)
		}
		if got := cfg.Databases["d"].URL; got != "postgres://:@h/db" {
			t.Errorf("отсутствующая переменная должна подставляться пустой строкой, получили %q", got)
		}
	})
}

// --- Дефолты ------------------------------------------------------------------

func TestLoadConfig_PoolDefaults(t *testing.T) {
	cfg := mustLoad(t, map[string]string{"config.yaml": `
settings:
  max_conns: 10
  max_idle_conns: 2
  max_conn_lifetime: 1h
  max_conn_idle_time: 30m
databases:
  db_a: {driver: pgx, url: "postgres://a"}
  db_b: {driver: pgx, url: "postgres://b", max_conns: 3}
  db_c: {driver: pgx, url: "postgres://c", max_idle_conns: 0}
`})
	a, b, c := cfg.Databases["db_a"], cfg.Databases["db_b"], cfg.Databases["db_c"]

	if intVal(t, a.MaxConns) != 10 || intVal(t, a.MaxIdleConns) != 2 {
		t.Errorf("db_a должна взять оба значения из settings, получили %v/%v", a.MaxConns, a.MaxIdleConns)
	}
	if a.MaxConnLifetime != "1h" || a.MaxConnIdleTime != "30m" {
		t.Errorf("db_a: lifetime=%q idle_time=%q, ожидали 1h/30m", a.MaxConnLifetime, a.MaxConnIdleTime)
	}
	if intVal(t, b.MaxConns) != 3 || intVal(t, b.MaxIdleConns) != 2 {
		t.Errorf("db_b: явный max_conns=3 побеждает, max_idle_conns из settings; получили %v/%v", b.MaxConns, b.MaxIdleConns)
	}
	if intVal(t, c.MaxConns) != 10 {
		t.Errorf("db_c.max_conns должен прийти из settings, получили %v", c.MaxConns)
	}
	if intVal(t, c.MaxIdleConns) != 0 {
		t.Errorf("db_c: явный max_idle_conns=0 не должен теряться из-за settings.max_idle_conns=2, получили %v", *c.MaxIdleConns)
	}
}

func TestLoadConfig_NoPoolDefaultsLeavesNil(t *testing.T) {
	cfg := mustLoad(t, map[string]string{"config.yaml": `
databases:
  d: {driver: pgx, url: "postgres://a"}
`})
	if d := cfg.Databases["d"]; d.MaxConns != nil || d.MaxIdleConns != nil {
		t.Errorf("без настроек значения должны остаться не заданными (nil), получили %v/%v", d.MaxConns, d.MaxIdleConns)
	}
}

func TestLoadConfig_NormalizesDriverName(t *testing.T) {
	cfg := mustLoad(t, map[string]string{"config.yaml": `
databases:
  d: {driver: " PGX ", url: "postgres://a"}
`})
	if got := cfg.Databases["d"].Driver; got != "pgx" {
		t.Errorf("driver = %q, ожидали pgx", got)
	}
}

func TestLoadConfig_DefaultTimezone(t *testing.T) {
	cfg := mustLoad(t, map[string]string{"config.yaml": `
settings:
  default_timezone: Europe/Moscow
queries:
  q1:
    db: d
    sql: select 1
    timeout: 5s
    schedule:
      at:
        - {weekday: mon, time: "09:00"}
  q2:
    db: d
    sql: select 1
    timeout: 5s
    schedule:
      timezone: UTC
      at:
        - {weekday: mon, time: "09:00"}
`})
	if got := cfg.Queries["q1"].Schedule.Timezone; got != "Europe/Moscow" {
		t.Errorf("q1: timezone = %q, ожидали значение из settings", got)
	}
	if got := cfg.Queries["q2"].Schedule.Timezone; got != "UTC" {
		t.Errorf("q2: явный timezone должен победить, получили %q", got)
	}
}

// --- Сравнение конфигов ---------------------------------------------------------

func TestSameDBConfig(t *testing.T) {
	base := DBConfig{Driver: "pgx", URL: "postgres://x", MaxConns: intp(10), MaxIdleConns: intp(2)}

	if !sameDBConfig(base, DBConfig{Driver: "pgx", URL: "postgres://x", MaxConns: intp(10), MaxIdleConns: intp(2)}) {
		t.Error("одинаковые по значению конфиги с разными указателями должны считаться равными")
	}
	if sameDBConfig(base, DBConfig{Driver: "pgx", URL: "postgres://x", MaxConns: intp(11), MaxIdleConns: intp(2)}) {
		t.Error("разный max_conns — конфиги разные")
	}
	if sameDBConfig(base, DBConfig{Driver: "pgx", URL: "postgres://x", MaxConns: intp(10)}) {
		t.Error("max_idle_conns: 2 против не заданного — конфиги разные")
	}
	if sameDBConfig(DBConfig{Driver: "pgx", URL: "x", MaxIdleConns: intp(0)}, DBConfig{Driver: "pgx", URL: "x"}) {
		t.Error("явный 0 и не заданное значение — разные конфиги")
	}
	if sameDBConfig(base, DBConfig{Driver: "mysql", URL: "postgres://x", MaxConns: intp(10), MaxIdleConns: intp(2)}) {
		t.Error("разный driver — конфиги разные")
	}
}

// --- Валидация запроса ------------------------------------------------------------

func validQuery() QueryConfig {
	return QueryConfig{DB: "d", SQL: "select 1", Timeout: "5s", Interval: "10s", MetricName: "my_metric"}
}

func baseCfg() *Config {
	return &Config{Databases: map[string]DBConfig{"d": {Driver: "pgx", URL: "postgres://x"}}}
}

func TestValidateSingleQuery(t *testing.T) {
	validSchedule := &ScheduleConfig{At: []ScheduleAt{{Weekday: "mon", Time: "09:00"}}}

	tests := []struct {
		name   string
		mutate func(*QueryConfig)
		cfgMut func(*Config)
		want   string // подстрока причины; "" — запрос валиден
	}{
		{name: "валидный interval-запрос"},
		{name: "валидный schedule-запрос", mutate: func(q *QueryConfig) { q.Interval = ""; q.Schedule = validSchedule }},
		{name: "multi-row с max_rows", mutate: func(q *QueryConfig) { q.ValueColumn = "v"; q.MaxRows = 5 }},
		{name: "допустимые статические лейблы", mutate: func(q *QueryConfig) { q.Labels = map[string]string{"team": "a", "_x": "b"} }},
		{name: "db берётся из settings.default_db",
			mutate: func(q *QueryConfig) { q.DB = "" },
			cfgMut: func(c *Config) { c.Settings.DefaultDB = "d" }},

		{name: "пустой sql", mutate: func(q *QueryConfig) { q.SQL = "" }, want: "sql is required"},
		{name: "sql из пробелов", mutate: func(q *QueryConfig) { q.SQL = "  \n " }, want: "sql is required"},
		{name: "нет db", mutate: func(q *QueryConfig) { q.DB = "" }, want: "db is required"},
		{name: "неизвестная db", mutate: func(q *QueryConfig) { q.DB = "zzz" }, want: `db "zzz" is not defined`},

		{name: "кривой timeout", mutate: func(q *QueryConfig) { q.Timeout = "abc" }, want: "invalid timeout"},
		{name: "нулевой timeout", mutate: func(q *QueryConfig) { q.Timeout = "0s" }, want: "timeout must be positive"},
		{name: "отрицательный timeout", mutate: func(q *QueryConfig) { q.Timeout = "-1s" }, want: "timeout must be positive"},

		{name: "interval и schedule вместе", mutate: func(q *QueryConfig) { q.Schedule = validSchedule }, want: "mutually exclusive"},
		{name: "нет ни interval, ни schedule", mutate: func(q *QueryConfig) { q.Interval = "" }, want: "invalid interval"},
		{name: "нулевой interval", mutate: func(q *QueryConfig) { q.Interval = "0s" }, want: "interval must be positive"},
		{name: "кривой weekday в schedule", mutate: func(q *QueryConfig) {
			q.Interval = ""
			q.Schedule = &ScheduleConfig{At: []ScheduleAt{{Weekday: "someday", Time: "09:00"}}}
		}, want: "invalid schedule"},

		{name: "имя начинается с цифры", mutate: func(q *QueryConfig) { q.MetricName = "1abc" }, want: "not a valid Prometheus metric name"},
		{name: "дефис в имени", mutate: func(q *QueryConfig) { q.MetricName = "a-b" }, want: "not a valid Prometheus metric name"},
		{name: "двоеточие в имени", mutate: func(q *QueryConfig) { q.MetricName = "a:b" }, want: "not a valid Prometheus metric name"},
		{name: "префикс app_", mutate: func(q *QueryConfig) { q.MetricName = "app_x" }, want: "reserved prefix"},
		{name: "префикс go_", mutate: func(q *QueryConfig) { q.MetricName = "go_x" }, want: "reserved prefix"},
		{name: "префикс process_", mutate: func(q *QueryConfig) { q.MetricName = "process_x" }, want: "reserved prefix"},
		{name: "префикс scrape_", mutate: func(q *QueryConfig) { q.MetricName = "scrape_x" }, want: "reserved prefix"},

		{name: "лейбл db зарезервирован", mutate: func(q *QueryConfig) { q.Labels = map[string]string{"db": "x"} }, want: "reserved"},
		{name: "лейбл env зарезервирован", mutate: func(q *QueryConfig) { q.Labels = map[string]string{"env": "x"} }, want: "reserved"},
		{name: "лейбл с __", mutate: func(q *QueryConfig) { q.Labels = map[string]string{"__x": "1"} }, want: "not a valid Prometheus label name"},
		{name: "лейбл с цифры", mutate: func(q *QueryConfig) { q.Labels = map[string]string{"1a": "x"} }, want: "not a valid Prometheus label name"},
		{name: "лейбл с дефисом", mutate: func(q *QueryConfig) { q.Labels = map[string]string{"a-b": "x"} }, want: "not a valid Prometheus label name"},

		{name: "отрицательный max_rows", mutate: func(q *QueryConfig) { q.ValueColumn = "v"; q.MaxRows = -1 }, want: "must not be negative"},
		{name: "max_rows без value_column", mutate: func(q *QueryConfig) { q.MaxRows = 5 }, want: "only applies to multi-row"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := validQuery()
			if tt.mutate != nil {
				tt.mutate(&q)
			}
			cfg := baseCfg()
			if tt.cfgMut != nil {
				tt.cfgMut(cfg)
			}
			got := validateSingleQuery(cfg, "name", q)
			if tt.want == "" {
				if got != "" {
					t.Fatalf("запрос должен быть валиден, причина отказа: %q", got)
				}
				return
			}
			if !strings.Contains(got, tt.want) {
				t.Fatalf("причина = %q, ожидали подстроку %q", got, tt.want)
			}
		})
	}
}

func TestValidateSingleQuery_UsesMetricNameNotWorkerKey(t *testing.T) {
	// Ключ воркера после клонирования на несколько БД — "q__db1" (двойное
	// подчёркивание допустимо, но суть: проверяется именно MetricName).
	q := validQuery()
	q.MetricName = "good_name"
	if got := validateSingleQuery(baseCfg(), "bad-worker-key", q); got != "" {
		t.Fatalf("ключ воркера не должен валидироваться как имя метрики, получили %q", got)
	}
}

func TestSanitizeQueries(t *testing.T) {
	cfg := baseCfg()
	good := validQuery()
	bad := validQuery()
	bad.SQL = ""
	cfg.Queries = map[string]QueryConfig{"good": good, "bad": bad}

	removed := sanitizeQueries(cfg)

	if len(removed) != 1 || !strings.Contains(removed[0], `query "bad" skipped`) {
		t.Fatalf("ожидали одно сообщение про bad, получили %v", removed)
	}
	if _, ok := cfg.Queries["bad"]; ok {
		t.Error("невалидный запрос должен быть удалён из конфига")
	}
	if _, ok := cfg.Queries["good"]; !ok {
		t.Error("валидный запрос не должен пострадать")
	}
}

func TestIsValidPrometheusNames(t *testing.T) {
	metric := map[string]bool{
		"a": true, "A_1": true, "_x": true, "abc_def_9": true,
		"": false, "1a": false, "a-b": false, "a b": false, "a:b": false, "ä": false,
	}
	for name, want := range metric {
		if got := isValidPrometheusName(name); got != want {
			t.Errorf("isValidPrometheusName(%q) = %v, ожидали %v", name, got, want)
		}
	}
	label := map[string]bool{
		"a": true, "_x": true, "a1": true,
		"": false, "__x": false, "1a": false, "a-b": false, "a.b": false,
	}
	for name, want := range label {
		if got := isValidPrometheusLabelName(name); got != want {
			t.Errorf("isValidPrometheusLabelName(%q) = %v, ожидали %v", name, got, want)
		}
	}
}

// --- Валидация БД и settings ----------------------------------------------------------

func TestValidateDatabasesAndSettings(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string // подстрока ошибки; "" — конфиг валиден
	}{
		{name: "базовый валидный конфиг"},
		{name: "startup_spread 30s", mutate: func(c *Config) { c.Settings.StartupSpread = "30s" }},
		{name: "startup_spread 0s выключает", mutate: func(c *Config) { c.Settings.StartupSpread = "0s" }},
		{name: "драйвер в другом регистре", mutate: func(c *Config) { c.Databases["d"] = DBConfig{Driver: "PGX", URL: "x"} }},
		{name: "max_idle_conns при неограниченном max_conns",
			mutate: func(c *Config) {
				c.Databases["d"] = DBConfig{Driver: "pgx", URL: "x", MaxConns: intp(0), MaxIdleConns: intp(5)}
			}},
		{name: "default_db существует", mutate: func(c *Config) { c.Settings.DefaultDB = "d" }},
		{name: "часовой пояс существует", mutate: func(c *Config) { c.Settings.DefaultTimezone = "Europe/Moscow" }},

		{name: "db_reconnect_interval мусор", mutate: func(c *Config) { c.Settings.DBReconnectInterval = "abc" }, want: "db_reconnect_interval is invalid"},
		{name: "db_reconnect_interval ноль", mutate: func(c *Config) { c.Settings.DBReconnectInterval = "0s" }, want: "must be positive"},
		{name: "startup_spread мусор", mutate: func(c *Config) { c.Settings.StartupSpread = "abc" }, want: "startup_spread is invalid"},
		{name: "startup_spread отрицательный", mutate: func(c *Config) { c.Settings.StartupSpread = "-1s" }, want: "must not be negative"},
		{name: "default_db не определена", mutate: func(c *Config) { c.Settings.DefaultDB = "zzz" }, want: "is not defined in databases"},
		{name: "неизвестный часовой пояс", mutate: func(c *Config) { c.Settings.DefaultTimezone = "Not/AZone" }, want: "default_timezone is invalid"},
		{name: "settings.max_conns < 0", mutate: func(c *Config) { c.Settings.DefaultMaxConns = intp(-1) }, want: "settings.max_conns must not be negative"},
		{name: "settings.max_idle_conns < 0", mutate: func(c *Config) { c.Settings.DefaultMaxIdleConns = intp(-1) }, want: "settings.max_idle_conns must not be negative"},
		{name: "settings.max_conn_lifetime мусор", mutate: func(c *Config) { c.Settings.DefaultMaxConnLifetime = "abc" }, want: "max_conn_lifetime is invalid"},
		{name: "settings.max_conn_idle_time мусор", mutate: func(c *Config) { c.Settings.DefaultMaxConnIdleTime = "abc" }, want: "max_conn_idle_time is invalid"},

		{name: "нет driver", mutate: func(c *Config) { c.Databases["d"] = DBConfig{URL: "x"} }, want: "driver is required"},
		{name: "неизвестный driver", mutate: func(c *Config) { c.Databases["d"] = DBConfig{Driver: "mongo", URL: "x"} }, want: "unknown driver"},
		{name: "нет url", mutate: func(c *Config) { c.Databases["d"] = DBConfig{Driver: "pgx"} }, want: "url is required"},
		{name: "max_conns < 0", mutate: func(c *Config) { c.Databases["d"] = DBConfig{Driver: "pgx", URL: "x", MaxConns: intp(-1)} }, want: "max_conns must not be negative"},
		{name: "max_idle_conns < 0", mutate: func(c *Config) { c.Databases["d"] = DBConfig{Driver: "pgx", URL: "x", MaxIdleConns: intp(-1)} }, want: "max_idle_conns must not be negative"},
		{name: "max_idle_conns > max_conns",
			mutate: func(c *Config) {
				c.Databases["d"] = DBConfig{Driver: "pgx", URL: "x", MaxConns: intp(2), MaxIdleConns: intp(5)}
			},
			want: "must be <= max_conns"},
		{name: "max_conn_lifetime мусор", mutate: func(c *Config) { c.Databases["d"] = DBConfig{Driver: "pgx", URL: "x", MaxConnLifetime: "abc"} }, want: "invalid max_conn_lifetime"},
		{name: "max_conn_lifetime ноль", mutate: func(c *Config) { c.Databases["d"] = DBConfig{Driver: "pgx", URL: "x", MaxConnLifetime: "0s"} }, want: "max_conn_lifetime must be positive"},
		{name: "max_conn_idle_time отрицательный", mutate: func(c *Config) { c.Databases["d"] = DBConfig{Driver: "pgx", URL: "x", MaxConnIdleTime: "-1s"} }, want: "max_conn_idle_time must be positive"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := *baseCfg()
			if tt.mutate != nil {
				tt.mutate(&cfg)
			}
			err := validateDatabasesAndSettings(cfg)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("конфиг должен быть валиден, ошибка: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ожидали ошибку с %q, получили: %v", tt.want, err)
			}
		})
	}
}

func TestSettingsDurations(t *testing.T) {
	spread := map[string]time.Duration{
		"":    30 * time.Second, // не задано — дефолт
		"10s": 10 * time.Second,
		"0s":  0, // выключено
		"2m":  2 * time.Minute,
		"abc": 30 * time.Second, // мусор — дефолт (ошибку ловит validate)
		"-5s": 30 * time.Second,
	}
	for in, want := range spread {
		if got := (AppSettings{StartupSpread: in}).StartupSpreadDuration(); got != want {
			t.Errorf("StartupSpreadDuration(%q) = %v, ожидали %v", in, got, want)
		}
	}

	reconnect := map[string]time.Duration{
		"":    5 * time.Minute,
		"30s": 30 * time.Second,
		"abc": 5 * time.Minute,
	}
	for in, want := range reconnect {
		if got := (AppSettings{DBReconnectInterval: in}).DBReconnectIntervalDuration(); got != want {
			t.Errorf("DBReconnectIntervalDuration(%q) = %v, ожидали %v", in, got, want)
		}
	}
}
