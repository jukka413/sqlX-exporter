package main

import (
	"strings"
	"testing"
	"time"
	_ "time/tzdata" // тесты не должны зависеть от наличия tzdata в системе
)

func mustSpecs(t *testing.T, expr string) []scheduleSpec {
	t.Helper()
	specs, err := parseScheduleExpr(expr)
	if err != nil {
		t.Fatalf("parseScheduleExpr(%q): %v", expr, err)
	}
	return specs
}

func weekdaySet(s scheduleSpec) string {
	var b strings.Builder
	for _, d := range []time.Weekday{time.Monday, time.Tuesday, time.Wednesday, time.Thursday, time.Friday, time.Saturday, time.Sunday} {
		if s.wdays[d] {
			b.WriteString(d.String()[:2])
		}
	}
	return b.String()
}

func TestParseScheduleExpr_Window(t *testing.T) {
	tests := []struct {
		expr      string
		wantCount int // число моментов внутри суток
		first     int // секунды от полуночи
		last      int
		days      string // подходящие дни недели
	}{
		{"wd1-5 09:00-19:00/5m", 121, 9 * 3600, 19 * 3600, "MoTuWeThFr"},
		{"09:00-19:00/1h", 11, 9 * 3600, 19 * 3600, "MoTuWeThFrSaSu"},
		{"9:00-19:00/1h", 11, 9 * 3600, 19 * 3600, "MoTuWeThFrSaSu"}, // час без ведущего нуля
		{"wd6,7 10:00-12:00/30m", 5, 10 * 3600, 12 * 3600, "SaSu"},
		{"wd1,3,5 10:00-10:00/1h", 1, 10 * 3600, 10 * 3600, "MoWeFr"},
		{"09:00-09:00/5m", 1, 9 * 3600, 9 * 3600, "MoTuWeThFrSaSu"}, // окно из одной точки
		{"09:00-09:01/30s", 3, 9 * 3600, 9*3600 + 60, "MoTuWeThFrSaSu"},
		{"09:00-19:00/7m", 86, 9 * 3600, 9*3600 + 85*7*60, "MoTuWeThFrSaSu"}, // сетка от начала окна; конец не на сетке не включается
		{"00:00-23:59/1h", 24, 0, 23 * 3600, "MoTuWeThFrSaSu"},
		{"md1,15 06:00-06:30/10m", 4, 6 * 3600, 6*3600 + 30*60, "MoTuWeThFrSaSu"},
		{"  wd1-5   09:00-19:00/5m  ", 121, 9 * 3600, 19 * 3600, "MoTuWeThFr"}, // лишние пробелы
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			specs := mustSpecs(t, tt.expr)
			if len(specs) != 1 {
				t.Fatalf("ожидали 1 окно, получили %d", len(specs))
			}
			s := specs[0]
			if len(s.times) != tt.wantCount || s.times[0] != tt.first || s.times[len(s.times)-1] != tt.last {
				t.Errorf("times: n=%d first=%d last=%d, ожидали n=%d first=%d last=%d",
					len(s.times), s.times[0], s.times[len(s.times)-1], tt.wantCount, tt.first, tt.last)
			}
			if got := weekdaySet(s); got != tt.days {
				t.Errorf("дни недели %s, ожидали %s", got, tt.days)
			}
		})
	}
}

func TestParseScheduleExpr_DaysOfMonth(t *testing.T) {
	s := mustSpecs(t, "md1,15,28-31 03:00-03:00/1h")[0]
	for d := 1; d <= 31; d++ {
		want := d == 1 || d == 15 || d >= 28
		if s.days[d] != want {
			t.Errorf("день %d: %v, ожидали %v", d, s.days[d], want)
		}
	}
}

func TestParseScheduleExpr_MultipleWindows(t *testing.T) {
	specs := mustSpecs(t, "wd1-5 09:00-19:00/5m;wd6 10:00-14:00/30m")
	if len(specs) != 2 {
		t.Fatalf("ожидали 2 окна, получили %d", len(specs))
	}
	if weekdaySet(specs[0]) != "MoTuWeThFr" || weekdaySet(specs[1]) != "Sa" {
		t.Errorf("дни: %s и %s", weekdaySet(specs[0]), weekdaySet(specs[1]))
	}
	if len(specs[1].times) != 9 { // 10:00, 10:30 … 14:00
		t.Errorf("во втором окне %d моментов, ожидали 9", len(specs[1].times))
	}
}

func TestParseScheduleExpr_Errors(t *testing.T) {
	tests := []struct{ expr, want string }{
		{"", "empty"},
		{"   ", "empty"},
		{"09:00-19:00/5m;;wd1 10:00-11:00/5m", "stray"},
		{"wd1-5 09:00-19:00/5m;", "stray"},

		// нет шага / окно записано неправильно
		{"09:00-19:00", "needs a step"},
		{"wd1-5 09:00-19:00", "needs a step"},
		{"09:00/5m", "expected HH:MM-HH:MM"},
		{"19:00-09:00/5m", "cross midnight"},

		// время
		{"09:00-24:00/5m", "between 00:00 and 23:59"},
		{"09:60-10:00/5m", "between 00:00 and 23:59"},
		{"9-10:00/5m", "expected HH:MM"},

		// шаг
		{"09:00-10:00/5", "expected a number with a unit"},
		{"09:00-10:00/m", "expected a number with a unit"},
		{"09:00-10:00/0m", "must be a positive number"},
		{"09:00-10:00/5d", "expected a number with a unit"},

		// дни
		{"h9 09:00-19:00/5m", "only md and wd"},
		{"wd1-5/2 09:00-19:00/5m", "only md and wd"},
		{"wd 09:00-19:00/5m", "no value"},
		{"wd0 09:00-19:00/5m", "out of range"},
		{"wd9 09:00-19:00/5m", "out of range"},
		{"md32 09:00-19:00/5m", "out of range"},
		{"wd5-1 09:00-19:00/5m", "reversed"},
		{"wd1,,3 09:00-19:00/5m", "empty value"},
		{"wd1-x 09:00-19:00/5m", "not a number"},
		{"wd1-5 extra 09:00-19:00/5m", "expected"},

		// старая запись в стиле Zabbix больше не поддерживается — ошибка должна подсказать формат
		{"wd1-5h9", "no time window found"},
		{"1m", "no time window found"},
		{"m0-59/5", "no time window found"},
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			_, err := parseScheduleExpr(tt.expr)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ожидали ошибку с %q, получили: %v", tt.want, err)
			}
		})
	}
}

func TestNextRunExpr_Window(t *testing.T) {
	// 2026-11-25 — среда, 11-27 — пятница, 11-28/29 — сб/вс, 11-30 — понедельник.
	specs := mustSpecs(t, "wd1-5 09:00-19:00/5m")
	utc := func(d, h, m int) time.Time { return time.Date(2026, 11, d, h, m, 0, 0, time.UTC) }

	tests := []struct {
		name      string
		now, want time.Time
	}{
		{"внутри окна — следующая отметка", utc(25, 10, 2), utc(25, 10, 5)},
		{"ровно на отметке — следующая (строго после)", utc(25, 10, 5), utc(25, 10, 10)},
		{"до начала окна", utc(25, 8, 0), utc(25, 9, 0)},
		{"последний запуск окна ровно в 19:00", utc(25, 18, 58), utc(25, 19, 0)},
		{"после 19:00 — завтра в 09:00", utc(25, 19, 0), utc(26, 9, 0)},
		{"пятница после окна — понедельник", utc(27, 19, 1), utc(30, 9, 0)},
		{"суббота — понедельник", utc(28, 12, 0), utc(30, 9, 0)},
		{"воскресенье — понедельник", utc(29, 23, 59), utc(30, 9, 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := nextRunExpr(tt.now, time.UTC, specs); !got.Equal(tt.want) {
				t.Errorf("nextRunExpr(%v) = %v, ожидали %v", tt.now, got, tt.want)
			}
		})
	}
}

func TestNextRunExpr_SubSecondNow(t *testing.T) {
	specs := mustSpecs(t, "09:00-09:00/1h")
	now := time.Date(2026, 11, 25, 9, 0, 0, 500_000_000, time.UTC) // 09:00:00.5 — запуск этой секунды уже прошёл
	want := time.Date(2026, 11, 26, 9, 0, 0, 0, time.UTC)
	if got := nextRunExpr(now, time.UTC, specs); !got.Equal(want) {
		t.Errorf("got %v, ожидали %v", got, want)
	}
}

// Сетка шага идёт от начала окна, а не от начала часа.
func TestNextRunExpr_GridAnchoredToWindowStart(t *testing.T) {
	specs := mustSpecs(t, "09:00-10:00/25m") // 09:00, 09:25, 09:50
	now := time.Date(2026, 11, 25, 9, 30, 0, 0, time.UTC)
	if got, want := nextRunExpr(now, time.UTC, specs), time.Date(2026, 11, 25, 9, 50, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("got %v, ожидали %v", got, want)
	}
	now = time.Date(2026, 11, 25, 9, 50, 0, 0, time.UTC)
	if got, want := nextRunExpr(now, time.UTC, specs), time.Date(2026, 11, 26, 9, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("после последней отметки окна — завтра: got %v, ожидали %v", got, want)
	}
}

func TestNextRunExpr_MultipleWindows(t *testing.T) {
	specs := mustSpecs(t, "wd1-5 09:00-19:00/1h;wd6 10:00-10:00/1h")
	// Пятница 19:30 → следующая — суббота 10:00 (второе окно).
	now := time.Date(2026, 11, 27, 19, 30, 0, 0, time.UTC)
	if got, want := nextRunExpr(now, time.UTC, specs), time.Date(2026, 11, 28, 10, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("got %v, ожидали %v", got, want)
	}
}

func TestNextRunExpr_DaysOfMonth(t *testing.T) {
	utc := func(y int, m time.Month, d, h int) time.Time { return time.Date(y, m, d, h, 0, 0, 0, time.UTC) }

	// В ноябре 30 дней: 31-е — только в декабре.
	if got, want := nextRunExpr(utc(2026, 11, 25, 10), time.UTC, mustSpecs(t, "md31 00:00-00:00/1h")), utc(2026, 12, 31, 0); !got.Equal(want) {
		t.Errorf("md31: got %v, ожидали %v", got, want)
	}
	// В феврале 30-го числа нет — перескакивает на март.
	if got, want := nextRunExpr(utc(2027, 1, 31, 10), time.UTC, mustSpecs(t, "md30 00:00-00:00/1h")), utc(2027, 3, 30, 0); !got.Equal(want) {
		t.Errorf("md30: got %v, ожидали %v", got, want)
	}
	// md и wd вместе — день должен подходить под оба условия: 13-е и пятница.
	got := nextRunExpr(utc(2026, 11, 25, 10), time.UTC, mustSpecs(t, "md13wd5 00:00-00:00/1h"))
	if want := utc(2027, 8, 13, 0); !got.Equal(want) { // ближайшая пятница 13-е после 25.11.2026
		t.Errorf("md13wd5: got %v, ожидали %v", got, want)
	}
}

func TestNextRunExpr_Timezone(t *testing.T) {
	msk, err := time.LoadLocation("Europe/Moscow") // UTC+3 круглый год
	if err != nil {
		t.Fatal(err)
	}
	specs := mustSpecs(t, "wd1-5 09:00-09:00/1h")

	// Пятница 23:30 по Москве (20:30 UTC) — следующий запуск в понедельник 09:00 МСК.
	now := time.Date(2026, 11, 27, 20, 30, 0, 0, time.UTC)
	got := nextRunExpr(now, msk, specs)
	if want := time.Date(2026, 11, 30, 9, 0, 0, 0, msk); !got.Equal(want) {
		t.Errorf("got %v, ожидали %v", got, want)
	}

	// 22:30 UTC в воскресенье — уже понедельник 01:30 МСК, 09:00 МСК ещё впереди.
	now = time.Date(2026, 11, 29, 22, 30, 0, 0, time.UTC)
	got = nextRunExpr(now, msk, specs)
	if want := time.Date(2026, 11, 30, 9, 0, 0, 0, msk); !got.Equal(want) {
		t.Errorf("день недели должен считаться в поясе расписания: got %v, ожидали %v", got, want)
	}
}

func TestBuildScheduleNext(t *testing.T) {
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC) // среда

	t.Run("at и expr вместе — берётся ближайший", func(t *testing.T) {
		next, err := buildScheduleNext(&ScheduleConfig{
			At:   []ScheduleAt{{Weekday: "mon", Time: "09:00"}},
			Expr: "12:00-12:00/1h",
		})
		if err != nil {
			t.Fatal(err)
		}
		if got, want := next(now), time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC); !got.Equal(want) {
			t.Errorf("got %v, ожидали %v", got, want)
		}
	})

	t.Run("только at работает как раньше", func(t *testing.T) {
		next, err := buildScheduleNext(&ScheduleConfig{At: []ScheduleAt{{Weekday: "mon", Time: "09:00"}}})
		if err != nil {
			t.Fatal(err)
		}
		if got, want := next(now), time.Date(2026, 11, 30, 9, 0, 0, 0, time.UTC); !got.Equal(want) {
			t.Errorf("got %v, ожидали %v", got, want)
		}
	})

	t.Run("только expr с часовым поясом", func(t *testing.T) {
		next, err := buildScheduleNext(&ScheduleConfig{Timezone: "Europe/Moscow", Expr: "15:00-15:00/1h"})
		if err != nil {
			t.Fatal(err)
		}
		// 10:00 UTC = 13:00 МСК → ближайшие 15:00 МСК = 12:00 UTC.
		if got, want := next(now), time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC); !got.Equal(want) {
			t.Errorf("got %v, ожидали %v", got, want)
		}
	})

	t.Run("ошибки", func(t *testing.T) {
		if _, err := buildScheduleNext(&ScheduleConfig{}); err == nil {
			t.Error("пустое расписание должно быть ошибкой")
		}
		if _, err := buildScheduleNext(&ScheduleConfig{Expr: "09:00-19:00"}); err == nil || !strings.Contains(err.Error(), "schedule.expr") {
			t.Errorf("ожидали ошибку с указанием schedule.expr, получили: %v", err)
		}
		if _, err := buildScheduleNext(&ScheduleConfig{Timezone: "Not/AZone", Expr: "09:00-10:00/5m"}); err == nil {
			t.Error("неизвестный часовой пояс должен быть ошибкой")
		}
	})
}

func TestValidateSchedule_Expr(t *testing.T) {
	if err := validateSchedule(&ScheduleConfig{Expr: "wd1-5 09:00-19:00/5m"}); err != nil {
		t.Errorf("валидный expr без at отвергнут: %v", err)
	}
	if err := validateSchedule(&ScheduleConfig{Expr: "wd1-5 09:00-19:00"}); err == nil || !strings.Contains(err.Error(), "schedule.expr") {
		t.Errorf("невалидный expr: получили %v", err)
	}
	if err := validateSchedule(&ScheduleConfig{Expr: "  "}); err == nil {
		t.Error("пустой expr без at должен быть ошибкой")
	}
	if err := validateSchedule(&ScheduleConfig{Timezone: "Not/AZone", Expr: "09:00-10:00/5m"}); err == nil {
		t.Error("неизвестный пояс при expr должен быть ошибкой")
	}
}

func TestSameSchedule_Expr(t *testing.T) {
	a := &ScheduleConfig{Expr: "09:00-19:00/5m"}
	if !sameSchedule(a, &ScheduleConfig{Expr: "09:00-19:00/5m"}) {
		t.Error("одинаковые expr должны быть равны")
	}
	if sameSchedule(a, &ScheduleConfig{Expr: "09:00-19:00/10m"}) {
		t.Error("смена expr должна перезапускать воркер")
	}
}
