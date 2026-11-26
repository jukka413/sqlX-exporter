package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// ===== Фейковый database/sql драйвер =====
//
// Настоящая БД не нужна: драйвер "sqlx_fake" отдаёт то, что вернёт handler,
// умеет задержку (для таймаутов) и считает вызовы / максимум одновременных
// запросов (для проверки "тики не накладываются").

type fakeResult struct {
	cols []string
	rows [][]driver.Value
}

type fakeDB struct {
	mu        sync.Mutex
	handler   func(query string) (fakeResult, error)
	delay     time.Duration
	calls     int
	active    int
	maxActive int
}

func (f *fakeDB) setHandler(h func(string) (fakeResult, error)) {
	f.mu.Lock()
	f.handler = h
	f.mu.Unlock()
}

func (f *fakeDB) setDelay(d time.Duration) {
	f.mu.Lock()
	f.delay = d
	f.mu.Unlock()
}

func (f *fakeDB) stats() (calls, maxActive int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls, f.maxActive
}

var (
	fakeMu  sync.Mutex
	fakeDBs = map[string]*fakeDB{}
	fakeSeq int
)

func init() { sql.Register("sqlx_fake", fakeDriver{}) }

type fakeDriver struct{}

func (fakeDriver) Open(name string) (driver.Conn, error) {
	fakeMu.Lock()
	f := fakeDBs[name]
	fakeMu.Unlock()
	if f == nil {
		return nil, fmt.Errorf("fake db %q not found", name)
	}
	return &fakeConn{f: f}, nil
}

type fakeConn struct{ f *fakeDB }

func (*fakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("fake driver: Prepare is not supported")
}
func (*fakeConn) Close() error { return nil }
func (*fakeConn) Begin() (driver.Tx, error) {
	return nil, errors.New("fake driver: transactions are not supported")
}

func (c *fakeConn) QueryContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	f := c.f
	f.mu.Lock()
	f.calls++
	f.active++
	if f.active > f.maxActive {
		f.maxActive = f.active
	}
	h, delay := f.handler, f.delay
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.active--
		f.mu.Unlock()
	}()

	if delay > 0 {
		t := time.NewTimer(delay)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.C:
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	res, err := h(query)
	if err != nil {
		return nil, err
	}
	return &fakeRows{res: res}, nil
}

type fakeRows struct {
	res fakeResult
	i   int
}

func (r *fakeRows) Columns() []string { return r.res.cols }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.i >= len(r.res.rows) {
		return io.EOF
	}
	copy(dest, r.res.rows[r.i])
	r.i++
	return nil
}

func newFakeDB(t *testing.T, h func(string) (fakeResult, error)) (*fakeDB, *sql.DB) {
	t.Helper()
	f := &fakeDB{handler: h}
	fakeMu.Lock()
	fakeSeq++
	dsn := fmt.Sprintf("%s#%d", t.Name(), fakeSeq)
	fakeDBs[dsn] = f
	fakeMu.Unlock()

	db, err := sql.Open("sqlx_fake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		fakeMu.Lock()
		delete(fakeDBs, dsn)
		fakeMu.Unlock()
	})
	return f, db
}

func constValue(v driver.Value) func(string) (fakeResult, error) {
	return func(string) (fakeResult, error) {
		return fakeResult{cols: []string{"v"}, rows: [][]driver.Value{{v}}}, nil
	}
}

func tableResult(cols []string, rows ...[]driver.Value) func(string) (fakeResult, error) {
	return func(string) (fakeResult, error) { return fakeResult{cols: cols, rows: rows}, nil }
}

func failWith(msg string) func(string) (fakeResult, error) {
	return func(string) (fakeResult, error) { return fakeResult{}, errors.New(msg) }
}

// ===== Чтение метрик из глобального реестра =====

type sample struct {
	labels map[string]string
	value  float64
}

func samples(t *testing.T, name string) []sample {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	var out []sample
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			s := sample{labels: map[string]string{}}
			for _, lp := range m.GetLabel() {
				s.labels[lp.GetName()] = lp.GetValue()
			}
			switch {
			case m.GetGauge() != nil:
				s.value = m.GetGauge().GetValue()
			case m.GetCounter() != nil:
				s.value = m.GetCounter().GetValue()
			case m.GetHistogram() != nil:
				s.value = float64(m.GetHistogram().GetSampleCount())
			}
			out = append(out, s)
		}
	}
	return out
}

func findSample(t *testing.T, name string, match map[string]string) (float64, bool) {
	t.Helper()
	for _, s := range samples(t, name) {
		ok := true
		for k, v := range match {
			if s.labels[k] != v {
				ok = false
				break
			}
		}
		if ok {
			return s.value, true
		}
	}
	return 0, false
}

func wantValue(t *testing.T, name string, match map[string]string, want float64) {
	t.Helper()
	got, ok := findSample(t, name, match)
	if !ok {
		t.Fatalf("метрика %s%v не найдена; есть: %v", name, match, samples(t, name))
	}
	if got != want {
		t.Fatalf("%s%v = %v, ожидали %v", name, match, got, want)
	}
}

func wantAbsent(t *testing.T, name string, match map[string]string) {
	t.Helper()
	if got, ok := findSample(t, name, match); ok {
		t.Fatalf("метрика %s%v не должна существовать, но равна %v", name, match, got)
	}
}

func cleanupMetric(t *testing.T, name string, dbs ...string) {
	t.Helper()
	t.Cleanup(func() {
		unregisterQueryMetric(name)
		for _, d := range dbs {
			deleteQueryHealthMetrics(name, d)
			configSchemaConflict.DeleteLabelValues(name, d)
			runtimeSchemaMismatch.DeleteLabelValues(name, d)
		}
	})
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("не дождались: %s", what)
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func doRun(t *testing.T, name string, cfg QueryConfig, db *sql.DB, timeout time.Duration, prev []prometheus.Labels) []prometheus.Labels {
	t.Helper()
	cfg.MetricName = name
	return runOnce(context.Background(), quietLogger(), name, cfg, db, timeout, prev)
}

// ===== runOnce =====

func TestRunOnce_SingleValue(t *testing.T) {
	const name = "sqlxtest_single_value"
	_, db := newFakeDB(t, constValue(int64(42)))
	cleanupMetric(t, name, "db1")

	doRun(t, name, QueryConfig{DB: "db1", SQL: "select 1", Labels: map[string]string{"team": "core"}}, db, 2*time.Second, nil)

	wantValue(t, name, map[string]string{"db": "db1", "team": "core"}, 42)
	wantValue(t, "app_query_up", map[string]string{"query": name, "db": "db1"}, 1)
	if _, ok := findSample(t, "app_query_last_success_timestamp_seconds", map[string]string{"query": name}); !ok {
		t.Error("не выставлен app_query_last_success_timestamp_seconds")
	}
}

func TestRunOnce_MultiRowAndStaleRemoval(t *testing.T) {
	const name = "sqlxtest_multi_row"
	f, db := newFakeDB(t, tableResult([]string{"wait_class", "value"},
		[]driver.Value{"Network", 1.5}, []driver.Value{"IO", int64(2)}))
	cleanupMetric(t, name, "db1")
	cfg := QueryConfig{DB: "db1", SQL: "select", ValueColumn: "value"}

	prev := doRun(t, name, cfg, db, 2*time.Second, nil)
	if len(prev) != 2 {
		t.Fatalf("prevLabels = %d, ожидали 2", len(prev))
	}
	wantValue(t, name, map[string]string{"wait_class": "Network"}, 1.5)
	wantValue(t, name, map[string]string{"wait_class": "IO"}, 2)

	// Network исчез из результата — его ряд должен быть удалён (иначе "вечная" серия).
	f.setHandler(tableResult([]string{"wait_class", "value"}, []driver.Value{"IO", int64(3)}))
	prev = doRun(t, name, cfg, db, 2*time.Second, prev)
	if len(prev) != 1 {
		t.Fatalf("prevLabels = %d, ожидали 1", len(prev))
	}
	wantAbsent(t, name, map[string]string{"wait_class": "Network"})
	wantValue(t, name, map[string]string{"wait_class": "IO"}, 3)
}

func TestRunOnce_ValueColumnIsCaseInsensitive(t *testing.T) {
	const name = "sqlxtest_case_insensitive"
	_, db := newFakeDB(t, tableResult([]string{"WAIT", "VALUE"}, []driver.Value{"x", int64(7)}))
	cleanupMetric(t, name, "db1")

	doRun(t, name, QueryConfig{DB: "db1", SQL: "select", ValueColumn: "value"}, db, 2*time.Second, nil)
	wantValue(t, name, map[string]string{"WAIT": "x"}, 7) // Oracle отдаёт имена в верхнем регистре
}

func TestRunOnce_DBErrorClearsSeriesAndCounts(t *testing.T) {
	const name = "sqlxtest_db_error"
	f, db := newFakeDB(t, tableResult([]string{"k", "value"}, []driver.Value{"a", int64(1)}))
	cleanupMetric(t, name, "db1")
	cfg := QueryConfig{DB: "db1", SQL: "select", ValueColumn: "value"}

	prev := doRun(t, name, cfg, db, 2*time.Second, nil)
	wantValue(t, name, map[string]string{"k": "a"}, 1)

	f.setHandler(failWith("ORA-00942: table or view does not exist"))
	next := doRun(t, name, cfg, db, 2*time.Second, prev)

	if next != nil {
		t.Errorf("после ошибки prevLabels должен быть nil, получили %v", next)
	}
	wantAbsent(t, name, map[string]string{"k": "a"}) // не показываем устаревшие данные
	wantValue(t, "app_query_up", map[string]string{"query": name, "db": "db1"}, 0)
	wantValue(t, "app_query_errors_total", map[string]string{"query": name, "reason": "db_error"}, 1)
}

func TestRunOnce_Timeout(t *testing.T) {
	const name = "sqlxtest_timeout"
	f, db := newFakeDB(t, constValue(int64(1)))
	f.setDelay(300 * time.Millisecond)
	cleanupMetric(t, name, "db1")

	doRun(t, name, QueryConfig{DB: "db1", SQL: "select 1"}, db, 30*time.Millisecond, nil)

	wantValue(t, "app_query_errors_total", map[string]string{"query": name, "reason": "timeout"}, 1)
	wantValue(t, "app_query_up", map[string]string{"query": name}, 0)
}

func TestRunOnce_CancelledIsNotAnError(t *testing.T) {
	const name = "sqlxtest_cancelled"
	_, db := newFakeDB(t, constValue(int64(1)))
	cleanupMetric(t, name, "db1")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	prev := []prometheus.Labels{{"db": "db1"}}
	got := runOnce(ctx, quietLogger(), name, QueryConfig{DB: "db1", SQL: "select 1", ValueColumn: "v", MetricName: name}, db, time.Second, prev)

	if len(got) != 1 {
		t.Errorf("при остановке воркера prevLabels должен сохраниться, получили %v", got)
	}
	// Остановка при reload/shutdown не должна выглядеть на дашборде как сбой запроса.
	wantAbsent(t, "app_query_errors_total", map[string]string{"query": name})
	wantAbsent(t, "app_query_up", map[string]string{"query": name})
}

func TestRunOnce_MaxRowsWritesNothing(t *testing.T) {
	const name = "sqlxtest_max_rows"
	_, db := newFakeDB(t, tableResult([]string{"k", "value"},
		[]driver.Value{"a", int64(1)}, []driver.Value{"b", int64(2)}, []driver.Value{"c", int64(3)}))
	cleanupMetric(t, name, "db1")

	doRun(t, name, QueryConfig{DB: "db1", SQL: "select", ValueColumn: "value", MaxRows: 2}, db, 2*time.Second, nil)

	if got := samples(t, name); len(got) != 0 {
		t.Errorf("при превышении max_rows не должно быть записано ни одной строки, есть: %v", got)
	}
	wantValue(t, "app_query_errors_total", map[string]string{"query": name, "reason": "db_error"}, 1)
}

func TestRunOnce_DuplicateLabelSetWritesNothing(t *testing.T) {
	const name = "sqlxtest_duplicate"
	_, db := newFakeDB(t, tableResult([]string{"k", "value"},
		[]driver.Value{"a", int64(1)}, []driver.Value{"a", int64(2)}))
	cleanupMetric(t, name, "db1")

	doRun(t, name, QueryConfig{DB: "db1", SQL: "select", ValueColumn: "value"}, db, 2*time.Second, nil)

	if got := samples(t, name); len(got) != 0 {
		t.Errorf("дубликат набора лейблов — результат отвергается целиком, есть: %v", got)
	}
	wantValue(t, "app_query_errors_total", map[string]string{"query": name, "reason": "db_error"}, 1)
}

func TestRunOnce_NonNumericRowsAreSkipped(t *testing.T) {
	const name = "sqlxtest_non_numeric"
	_, db := newFakeDB(t, tableResult([]string{"k", "value"},
		[]driver.Value{"a", "not-a-number"}, []driver.Value{"b", 2.0}))
	cleanupMetric(t, name, "db1")

	doRun(t, name, QueryConfig{DB: "db1", SQL: "select", ValueColumn: "value"}, db, 2*time.Second, nil)

	wantAbsent(t, name, map[string]string{"k": "a"})
	wantValue(t, name, map[string]string{"k": "b"}, 2)
	wantValue(t, "app_query_up", map[string]string{"query": name}, 1) // пропуск строки — не сбой запроса
}

func TestRunOnce_BadResultShapes(t *testing.T) {
	tests := []struct {
		name string
		cols []string
	}{
		{"value_column отсутствует", []string{"x", "y"}},
		{"столбец совпал с зарезервированным лейблом db", []string{"db", "value"}},
		{"столбец — не валидное имя лейбла", []string{"bad-name", "value"}},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name := fmt.Sprintf("sqlxtest_bad_shape_%d", i)
			_, db := newFakeDB(t, tableResult(tt.cols, []driver.Value{"a", int64(1)}))
			cleanupMetric(t, name, "db1")

			doRun(t, name, QueryConfig{DB: "db1", SQL: "select", ValueColumn: "value"}, db, 2*time.Second, nil)

			wantValue(t, "app_query_errors_total", map[string]string{"query": name, "reason": "db_error"}, 1)
			wantValue(t, "app_query_up", map[string]string{"query": name}, 0)
		})
	}
}

func TestRunOnce_RuntimeSchemaMismatchSetsAndClearsGauge(t *testing.T) {
	const name = "sqlxtest_schema_mismatch"
	f, db := newFakeDB(t, tableResult([]string{"a", "value"}, []driver.Value{"x", int64(1)}))
	cleanupMetric(t, name, "db1")
	cfg := QueryConfig{DB: "db1", SQL: "select", ValueColumn: "value"}
	match := map[string]string{"query": name, "db": "db1"}

	prev := doRun(t, name, cfg, db, 2*time.Second, nil)

	// Тот же запрос вернул другие столбцы — зарегистрированная схема лейблов не совпадает.
	f.setHandler(tableResult([]string{"b", "value"}, []driver.Value{"x", int64(1)}))
	prev = doRun(t, name, cfg, db, 2*time.Second, prev)
	wantValue(t, "app_query_errors_total", map[string]string{"query": name, "reason": "schema_mismatch"}, 1)
	wantValue(t, "app_query_runtime_schema_mismatch", match, 1)

	// Вернули прежние столбцы — сигнал должен пропасть сам.
	f.setHandler(tableResult([]string{"a", "value"}, []driver.Value{"x", int64(1)}))
	doRun(t, name, cfg, db, 2*time.Second, prev)
	wantAbsent(t, "app_query_runtime_schema_mismatch", match)
	wantValue(t, "app_query_up", match, 1)
}

// ===== startQueryWorker =====

// startTestWorker запускает воркер в горутине. stop отменяет контекст и ждёт
// завершения; done закрывается, когда воркер вышел сам.
func startTestWorker(t *testing.T, name string, cfg QueryConfig, db *sql.DB, spread time.Duration) (stop func(), done <-chan struct{}) {
	t.Helper()
	cfg.MetricName = name
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	prev := []prometheus.Labels{}
	go startQueryWorker(ctx, &wg, quietLogger(), name, cfg, db, &prev, spread)

	d := make(chan struct{})
	go func() { wg.Wait(); close(d) }()

	t.Cleanup(cancel)
	stop = func() {
		cancel()
		select {
		case <-d:
		case <-time.After(3 * time.Second):
			t.Fatal("воркер не остановился после отмены контекста")
		}
	}
	return stop, d
}

func TestStartQueryWorker_RunsPeriodicallyAndStops(t *testing.T) {
	const name = "sqlxtest_worker_periodic"
	f, db := newFakeDB(t, constValue(int64(5)))
	cleanupMetric(t, name, "db1")

	stop, _ := startTestWorker(t, name, QueryConfig{DB: "db1", SQL: "select 1", Timeout: "1s", Interval: "20ms"}, db, 0)

	eventually(t, "минимум 3 запуска запроса", func() bool { c, _ := f.stats(); return c >= 3 })
	wantValue(t, name, map[string]string{"db": "db1"}, 5)

	stop()
	before, _ := f.stats()
	time.Sleep(80 * time.Millisecond)
	if after, _ := f.stats(); after != before {
		t.Errorf("после остановки воркер продолжил ходить в БД: %d → %d запросов", before, after)
	}
}

func TestStartQueryWorker_TicksNeverOverlap(t *testing.T) {
	const name = "sqlxtest_worker_no_overlap"
	f, db := newFakeDB(t, constValue(int64(1)))
	f.setDelay(60 * time.Millisecond) // запрос заметно дольше interval
	cleanupMetric(t, name, "db1")

	stop, _ := startTestWorker(t, name, QueryConfig{DB: "db1", SQL: "select 1", Timeout: "1s", Interval: "10ms"}, db, 0)
	eventually(t, "минимум 3 запуска", func() bool { c, _ := f.stats(); return c >= 3 })
	stop()

	if _, maxActive := f.stats(); maxActive != 1 {
		t.Errorf("одновременно выполнялось %d запросов одного воркера, ожидали 1 (тики должны пропускаться)", maxActive)
	}
}

func TestStartQueryWorker_CancelDuringStartupDelay(t *testing.T) {
	const name = "sqlxtest_worker_startup_delay"
	f, db := newFakeDB(t, constValue(int64(1)))
	cleanupMetric(t, name, "db1")

	// interval и spread по часу: первый запуск отложен на хеш-смещение (почти наверняка >> 50мс).
	stop, _ := startTestWorker(t, name, QueryConfig{DB: "db1", SQL: "select 1", Timeout: "1s", Interval: "1h"}, db, time.Hour)
	time.Sleep(50 * time.Millisecond)
	if c, _ := f.stats(); c != 0 {
		t.Fatalf("запрос выполнился до истечения стартовой задержки (%d вызовов)", c)
	}
	stop() // должен выйти сразу, а не ждать конца задержки
}

func TestStartQueryWorker_InvalidConfigStopsWorker(t *testing.T) {
	tests := []struct {
		name string
		cfg  QueryConfig
	}{
		{"кривой timeout", QueryConfig{DB: "db1", SQL: "select 1", Timeout: "soon", Interval: "1s"}},
		{"кривой interval", QueryConfig{DB: "db1", SQL: "select 1", Timeout: "1s", Interval: "often"}},
		{"кривое расписание", QueryConfig{DB: "db1", SQL: "select 1", Timeout: "1s",
			Schedule: &ScheduleConfig{At: []ScheduleAt{{Weekday: "someday", Time: "09:00"}}}}},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, db := newFakeDB(t, constValue(int64(1)))
			_, done := startTestWorker(t, fmt.Sprintf("sqlxtest_worker_invalid_%d", i), tt.cfg, db, 0)
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("воркер с невалидным конфигом должен завершиться сам")
			}
			if c, _ := f.stats(); c != 0 {
				t.Errorf("воркер с невалидным конфигом сходил в БД %d раз", c)
			}
		})
	}
}

func TestStartQueryWorker_ScheduledWaitsAndStops(t *testing.T) {
	const name = "sqlxtest_worker_schedule"
	f, db := newFakeDB(t, constValue(int64(1)))
	cleanupMetric(t, name, "db1")

	// Каждый день, через 2 часа от текущего момента: ближайший запуск точно не наступит за время теста.
	at := time.Now().UTC().Add(2 * time.Hour).Format("15:04")
	cfg := QueryConfig{DB: "db1", SQL: "select 1", Timeout: "1s",
		Schedule: &ScheduleConfig{At: []ScheduleAt{{Weekday: "sun,mon,tue,wed,thu,fri,sat", Time: at}}}}

	stop, _ := startTestWorker(t, name, cfg, db, 0)
	time.Sleep(50 * time.Millisecond)
	if c, _ := f.stats(); c != 0 {
		t.Fatalf("запрос по расписанию сработал раньше времени (%d вызовов)", c)
	}
	stop()
}

// ===== workerManager.reconcile =====

func newTestManager(t *testing.T) *workerManager {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	wm := newWorkerManager(ctx, quietLogger())
	t.Cleanup(func() {
		cancel()
		wm.stopAll()
	})
	return wm
}

func testPools(env string, dbs map[string]*sql.DB) map[string]*dbPool {
	pools := make(map[string]*dbPool, len(dbs))
	for name, db := range dbs {
		pools[name] = &dbPool{cfg: DBConfig{Env: env}, db: db}
	}
	return pools
}

func testQuery(db, metric string) QueryConfig {
	return QueryConfig{DB: db, SQL: "select 1", Timeout: "1s", Interval: "20ms", MetricName: metric}
}

func workerCount(wm *workerManager) int {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	return len(wm.workers)
}

func TestReconcile_StartChangeRemove(t *testing.T) {
	const name = "sqlxtest_reconcile_lifecycle"
	_, db := newFakeDB(t, func(q string) (fakeResult, error) {
		v := int64(1)
		if q == "select 2" {
			v = 2
		}
		return fakeResult{cols: []string{"v"}, rows: [][]driver.Value{{v}}}, nil
	})
	cleanupMetric(t, name, "db1")
	wm := newTestManager(t)
	pools := testPools("prod", map[string]*sql.DB{"db1": db})
	match := map[string]string{"db": "db1", "env": "prod"}

	q := testQuery("db1", name)
	if got := wm.reconcile(1, map[string]QueryConfig{name: q}, pools, nil, ""); got != (ReconcileStats{Started: 1}) {
		t.Fatalf("первый reconcile: %+v", got)
	}
	eventually(t, "появилась метрика со значением 1", func() bool {
		v, ok := findSample(t, name, match)
		return ok && v == 1
	})

	if got := wm.reconcile(2, map[string]QueryConfig{name: q}, pools, nil, ""); got != (ReconcileStats{Unchanged: 1}) {
		t.Fatalf("тот же конфиг: %+v, воркер не должен перезапускаться", got)
	}

	q.SQL = "select 2"
	if got := wm.reconcile(3, map[string]QueryConfig{name: q}, pools, nil, ""); got != (ReconcileStats{Restarted: 1}) {
		t.Fatalf("изменили SQL: %+v", got)
	}
	eventually(t, "воркер подхватил новый SQL", func() bool {
		v, ok := findSample(t, name, match)
		return ok && v == 2
	})

	if got := wm.reconcile(4, map[string]QueryConfig{}, pools, nil, ""); got != (ReconcileStats{Stopped: 1}) {
		t.Fatalf("удалили запрос: %+v", got)
	}
	if workerCount(wm) != 0 {
		t.Error("воркер остался после удаления запроса")
	}
	if _, ok := lookupQueryMetric(name); ok {
		t.Error("метрика запроса осталась зарегистрированной после удаления запроса")
	}
	wantAbsent(t, "app_query_up", map[string]string{"query": name})
}

func TestReconcile_StaleRevisionIsIgnored(t *testing.T) {
	const name = "sqlxtest_reconcile_stale"
	_, db := newFakeDB(t, constValue(int64(1)))
	cleanupMetric(t, name, "db1")
	wm := newTestManager(t)
	pools := testPools("", map[string]*sql.DB{"db1": db})

	wm.reconcile(5, map[string]QueryConfig{name: testQuery("db1", name)}, pools, nil, "")
	// Запоздавший reconcile со старой ревизией не должен откатить уже применённое.
	if got := wm.reconcile(4, map[string]QueryConfig{}, pools, nil, ""); got != (ReconcileStats{}) {
		t.Errorf("устаревшая ревизия должна быть пропущена, получили %+v", got)
	}
	if workerCount(wm) != 1 {
		t.Error("устаревший reconcile остановил воркер")
	}
}

func TestReconcile_DBUnavailableDoesNotStart(t *testing.T) {
	const name = "sqlxtest_reconcile_nodb"
	wm := newTestManager(t)

	got := wm.reconcile(1, map[string]QueryConfig{name: testQuery("missing", name)}, map[string]*dbPool{}, nil, "")
	if got.Started != 0 || workerCount(wm) != 0 {
		t.Errorf("запрос к недоступной БД не должен стартовать: %+v", got)
	}
}

func TestReconcile_PendingDBDoesNotStartNewQuery(t *testing.T) {
	const name = "sqlxtest_reconcile_pending"
	f, db := newFakeDB(t, constValue(int64(1)))
	wm := newTestManager(t)
	pools := testPools("", map[string]*sql.DB{"db1": db})

	got := wm.reconcile(1, map[string]QueryConfig{name: testQuery("db1", name)}, pools,
		map[string]struct{}{"db1": {}}, "")
	if got != (ReconcileStats{Pending: 1}) || workerCount(wm) != 0 {
		t.Errorf("новый запрос на pending-БД не должен стартовать на старом пуле: %+v", got)
	}
	if c, _ := f.stats(); c != 0 {
		t.Errorf("к pending-БД были обращения: %d", c)
	}
}

func TestReconcile_DefaultDB(t *testing.T) {
	const name = "sqlxtest_reconcile_default_db"
	_, db := newFakeDB(t, constValue(int64(1)))
	cleanupMetric(t, name, "main")
	wm := newTestManager(t)
	pools := testPools("", map[string]*sql.DB{"main": db})

	q := testQuery("", name) // db не указан
	if got := wm.reconcile(1, map[string]QueryConfig{name: q}, pools, nil, "main"); got.Started != 1 {
		t.Fatalf("запрос без db должен уйти на default_db: %+v", got)
	}
	eventually(t, "метрика с db=main", func() bool {
		_, ok := findSample(t, name, map[string]string{"db": "main"})
		return ok
	})

	// Без default_db запрос без db стартовать не должен.
	wm2 := newTestManager(t)
	if got := wm2.reconcile(1, map[string]QueryConfig{name + "_x": testQuery("", name+"_x")}, pools, nil, ""); got.Started != 0 {
		t.Errorf("запрос без db и без default_db не должен стартовать: %+v", got)
	}
}

func TestReconcile_LabelSchemaChangeFreezesQuery(t *testing.T) {
	const name = "sqlxtest_reconcile_frozen"
	_, db := newFakeDB(t, constValue(int64(1)))
	cleanupMetric(t, name, "db1")
	wm := newTestManager(t)
	pools := testPools("", map[string]*sql.DB{"db1": db})

	q := testQuery("db1", name)
	q.Labels = map[string]string{"team": "a"}
	wm.reconcile(1, map[string]QueryConfig{name: q}, pools, nil, "")
	eventually(t, "метрика появилась", func() bool {
		_, ok := findSample(t, name, map[string]string{"team": "a"})
		return ok
	})

	// Меняем набор ключей лейблов и SQL: Prometheus не умеет менять схему без рестарта.
	changed := testQuery("db1", name)
	changed.SQL = "select 999"
	changed.Labels = map[string]string{"owner": "a"}
	got := wm.reconcile(2, map[string]QueryConfig{name: changed}, pools, nil, "")
	if got.SchemaConflicts != 1 || got.Restarted != 0 {
		t.Fatalf("ожидали 1 конфликт схемы и ни одного перезапуска: %+v", got)
	}

	wm.mu.Lock()
	frozenSQL := wm.workers[name].cfg.SQL
	wm.mu.Unlock()
	if frozenSQL != "select 1" {
		t.Errorf("конфиг должен остаться замороженным на последнем рабочем состоянии, SQL = %q", frozenSQL)
	}
	wantValue(t, "app_query_config_schema_conflict", map[string]string{"query": name, "db": "db1"}, 1)
}

func TestReconcile_SharedMetricAcrossDatabases(t *testing.T) {
	const name = "sqlxtest_reconcile_shared"
	_, db1 := newFakeDB(t, constValue(int64(1)))
	_, db2 := newFakeDB(t, constValue(int64(1)))
	cleanupMetric(t, name, "db1", "db2")
	wm := newTestManager(t)
	pools := testPools("", map[string]*sql.DB{"db1": db1, "db2": db2})

	k1, k2 := name+"__db1", name+"__db2"
	both := map[string]QueryConfig{k1: testQuery("db1", name), k2: testQuery("db2", name)}
	if got := wm.reconcile(1, both, pools, nil, ""); got.Started != 2 {
		t.Fatalf("ожидали 2 запущенных воркера: %+v", got)
	}
	eventually(t, "обе серии на месте", func() bool {
		_, a := findSample(t, name, map[string]string{"db": "db1"})
		_, b := findSample(t, name, map[string]string{"db": "db2"})
		return a && b
	})

	// Убрали клон для db2: общая метрика обязана остаться — ею пользуется клон db1.
	if got := wm.reconcile(2, map[string]QueryConfig{k1: both[k1]}, pools, nil, ""); got.Stopped != 1 {
		t.Fatalf("ожидали остановку одного воркера: %+v", got)
	}
	if _, ok := lookupQueryMetric(name); !ok {
		t.Fatal("метрика снята с регистрации, хотя её ещё использует соседний клон")
	}
	wantAbsent(t, name, map[string]string{"db": "db2"})
	wantValue(t, name, map[string]string{"db": "db1"}, 1)

	// Убрали последний — теперь метрика снимается целиком.
	wm.reconcile(3, map[string]QueryConfig{}, pools, nil, "")
	if _, ok := lookupQueryMetric(name); ok {
		t.Error("метрика осталась зарегистрированной после удаления всех клонов")
	}
}

func TestWorkerManager_StopAll(t *testing.T) {
	const name = "sqlxtest_stop_all"
	f, db := newFakeDB(t, constValue(int64(1)))
	cleanupMetric(t, name, "db1")
	wm := newTestManager(t)
	pools := testPools("", map[string]*sql.DB{"db1": db})

	wm.reconcile(1, map[string]QueryConfig{name: testQuery("db1", name)}, pools, nil, "")
	eventually(t, "воркер сделал хотя бы один запрос", func() bool { c, _ := f.stats(); return c >= 1 })

	wm.stopAll()
	before, _ := f.stats()
	time.Sleep(80 * time.Millisecond)
	if after, _ := f.stats(); after != before {
		t.Errorf("stopAll вернулся, но воркер продолжил работать: %d → %d запросов", before, after)
	}
}
