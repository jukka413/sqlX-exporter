package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestStartupOffset(t *testing.T) {
	const spread = 30 * time.Second

	t.Run("детерминирован", func(t *testing.T) {
		a := startupOffset("db", "query", 10*time.Second, spread)
		b := startupOffset("db", "query", 10*time.Second, spread)
		if a != b {
			t.Fatalf("смещение должно быть одинаковым между вызовами: %v и %v", a, b)
		}
	})

	t.Run("не выходит за окно min(interval, spread)", func(t *testing.T) {
		cases := []struct{ interval, spread time.Duration }{
			{10 * time.Second, spread}, // окно = interval
			{5 * time.Minute, spread},  // окно = spread
			{time.Second, spread},
			{10 * time.Second, 3 * time.Second},
		}
		for _, c := range cases {
			window := min(c.interval, c.spread)
			for i := 0; i < 2000; i++ {
				got := startupOffset("db", fmt.Sprintf("q%d", i), c.interval, c.spread)
				if got < 0 || got >= window {
					t.Fatalf("interval=%v spread=%v: смещение %v вне [0, %v)", c.interval, c.spread, got, window)
				}
			}
		}
	})

	t.Run("spread=0 выключает разнесение", func(t *testing.T) {
		if got := startupOffset("db", "q", 10*time.Second, 0); got != 0 {
			t.Errorf("получили %v, ожидали 0", got)
		}
	})

	t.Run("нулевой или отрицательный interval", func(t *testing.T) {
		if got := startupOffset("db", "q", 0, spread); got != 0 {
			t.Errorf("interval=0: получили %v", got)
		}
		if got := startupOffset("db", "q", -time.Second, spread); got != 0 {
			t.Errorf("interval<0: получили %v", got)
		}
	})

	t.Run("запросы действительно разносятся", func(t *testing.T) {
		seen := make(map[time.Duration]struct{})
		for i := 0; i < 100; i++ {
			seen[startupOffset("db", fmt.Sprintf("query_%d", i), 10*time.Second, spread)] = struct{}{}
		}
		if len(seen) < 90 {
			t.Errorf("100 запросов дали всего %d разных смещений — хеш не разносит", len(seen))
		}
	})

	t.Run("зависит от БД", func(t *testing.T) {
		a := startupOffset("db_a", "same_query", 10*time.Second, spread)
		b := startupOffset("db_b", "same_query", 10*time.Second, spread)
		if a == b {
			t.Errorf("один и тот же запрос на разных БД получил одинаковое смещение %v", a)
		}
	})

	t.Run("разделитель защищает от склейки имён", func(t *testing.T) {
		a := startupOffset("ab", "c", 10*time.Second, spread)
		b := startupOffset("a", "bc", 10*time.Second, spread)
		if a == b {
			t.Errorf(`("ab","c") и ("a","bc") дали одинаковое смещение %v`, a)
		}
	})
}

func TestClassifyError(t *testing.T) {
	bg := context.Background()

	expired, cancelExpired := context.WithTimeout(bg, time.Millisecond)
	defer cancelExpired()
	<-expired.Done()

	cancelled, cancelNow := context.WithCancel(bg)
	cancelNow()

	tests := []struct {
		name      string
		workerCtx context.Context
		queryCtx  context.Context
		err       error
		want      string
	}{
		{"обычная ошибка БД", bg, bg, errors.New("boom"), "db_error"},
		{"таймаут запроса", bg, expired, context.DeadlineExceeded, "timeout"},
		{"несовместимая схема", bg, bg, fmt.Errorf("query x: %w", errSchemaMismatch), "schema_mismatch"},
		{"воркер остановлен", cancelled, bg, context.Canceled, "cancelled"},
		{"остановка важнее таймаута", cancelled, expired, context.DeadlineExceeded, "cancelled"},
		{"таймаут важнее схемы", bg, expired, errSchemaMismatch, "timeout"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyError(tt.workerCtx, tt.queryCtx, tt.err); got != tt.want {
				t.Errorf("classifyError = %q, ожидали %q", got, tt.want)
			}
		})
	}
}

func TestLabelsKey(t *testing.T) {
	a := prometheus.Labels{"x": "1", "y": "2"}
	b := prometheus.Labels{"y": "2", "x": "1"}
	if labelsKey(a) != labelsKey(b) {
		t.Error("ключ не должен зависеть от порядка добавления лейблов")
	}

	distinct := []prometheus.Labels{
		{"a": "bc"},
		{"ab": "c"},
		{"a": "b", "c": ""},
		{"a": "1"},
		{"a": "10"},
		{},
	}
	seen := make(map[string]int)
	for i, l := range distinct {
		k := labelsKey(l)
		if j, dup := seen[k]; dup {
			t.Errorf("лейблы #%d и #%d дали один ключ %q — разные ряды склеились бы", j, i, k)
		}
		seen[k] = i
	}
}

func TestToFloat64(t *testing.T) {
	ok := []struct {
		in   any
		want float64
	}{
		{3, 3}, {int32(4), 4}, {int64(5), 5}, {float32(1.5), 1.5}, {2.25, 2.25},
		{[]byte("2.5"), 2.5}, {"7", 7}, {"-1.5e2", -150},
	}
	for _, tt := range ok {
		got, isOK := toFloat64(tt.in)
		if !isOK || got != tt.want {
			t.Errorf("toFloat64(%#v) = %v, %v; ожидали %v, true", tt.in, got, isOK, tt.want)
		}
	}
	for _, in := range []any{nil, true, "abc", "", []byte(""), []byte("x1"), struct{}{}} {
		if got, isOK := toFloat64(in); isOK {
			t.Errorf("toFloat64(%#v) = %v, true; ожидали false", in, got)
		}
	}
}

func TestAnyToString(t *testing.T) {
	tests := []struct {
		in   any
		want string
	}{
		{nil, ""},
		{"s", "s"},
		{[]byte("bytes"), "bytes"},
		{42, "42"},
		{1.5, "1.5"},
		{true, "true"},
	}
	for _, tt := range tests {
		if got := anyToString(tt.in); got != tt.want {
			t.Errorf("anyToString(%#v) = %q, ожидали %q", tt.in, got, tt.want)
		}
	}
}
