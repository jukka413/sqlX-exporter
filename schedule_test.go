package main

import (
	"strings"
	"testing"
	"time"
	_ "time/tzdata" // тесты не должны зависеть от наличия tzdata в системе
)

func TestSplitComma(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		{"mon", []string{"mon"}},
		{"mon, wed", []string{"mon", "wed"}},
		{" mon ,wed,  fri ", []string{"mon", "wed", "fri"}},
		{"", []string{}},
		{" , ,", []string{}},
	}
	for _, tt := range tests {
		got := splitComma(tt.in)
		if strings.Join(got, "|") != strings.Join(tt.want, "|") {
			t.Errorf("splitComma(%q) = %q, ожидали %q", tt.in, got, tt.want)
		}
	}
}

func TestParseWeekday(t *testing.T) {
	ok := map[string]time.Weekday{
		"sun": time.Sunday, "Monday": time.Monday, "TUE": time.Tuesday, "tues": time.Tuesday,
		"wed": time.Wednesday, "thu": time.Thursday, "thurs": time.Thursday,
		"fri": time.Friday, " sat ": time.Saturday,
	}
	for in, want := range ok {
		got, err := parseWeekday(in)
		if err != nil || got != want {
			t.Errorf("parseWeekday(%q) = %v, %v; ожидали %v", in, got, err, want)
		}
	}
	for _, in := range []string{"", "someday", "mo"} {
		if _, err := parseWeekday(in); err == nil {
			t.Errorf("parseWeekday(%q) должен вернуть ошибку", in)
		}
	}
}

func TestParseHHMM(t *testing.T) {
	h, m, err := parseHHMM(" 09:05 ")
	if err != nil || h != 9 || m != 5 {
		t.Errorf("parseHHMM(09:05) = %d:%d, %v", h, m, err)
	}
	for _, in := range []string{"", "25:00", "12:60", "noon", "12"} {
		if _, _, err := parseHHMM(in); err == nil {
			t.Errorf("parseHHMM(%q) должен вернуть ошибку", in)
		}
	}
}

func TestParseSchedule(t *testing.T) {
	t.Run("комбинации weekday x time", func(t *testing.T) {
		loc, entries, err := parseSchedule(&ScheduleConfig{
			At: []ScheduleAt{{Weekday: "mon, wed", Time: "09:00,18:30"}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 4 {
			t.Fatalf("ожидали 2 дня x 2 времени = 4 записи, получили %d", len(entries))
		}
		if loc != time.UTC {
			t.Errorf("без timezone должен быть UTC (а не локальный пояс контейнера), получили %v", loc)
		}
	})

	t.Run("несколько блоков at", func(t *testing.T) {
		_, entries, err := parseSchedule(&ScheduleConfig{
			At: []ScheduleAt{{Weekday: "mon", Time: "09:00"}, {Weekday: "fri", Time: "17:00"}},
		})
		if err != nil || len(entries) != 2 {
			t.Fatalf("ожидали 2 записи, получили %d, ошибка: %v", len(entries), err)
		}
	})

	t.Run("часовой пояс", func(t *testing.T) {
		loc, _, err := parseSchedule(&ScheduleConfig{
			Timezone: "Europe/Moscow",
			At:       []ScheduleAt{{Weekday: "mon", Time: "09:00"}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if loc.String() != "Europe/Moscow" {
			t.Errorf("loc = %v", loc)
		}
	})

	errCases := []struct {
		name string
		cfg  ScheduleConfig
		want string
	}{
		{"пустой at", ScheduleConfig{}, "schedule.at is empty"},
		{"неизвестный день", ScheduleConfig{At: []ScheduleAt{{Weekday: "someday", Time: "09:00"}}}, "invalid weekday"},
		{"кривое время", ScheduleConfig{At: []ScheduleAt{{Weekday: "mon", Time: "25:00"}}}, ""},
		{"неизвестный пояс", ScheduleConfig{Timezone: "Not/AZone", At: []ScheduleAt{{Weekday: "mon", Time: "09:00"}}}, ""},
	}
	for _, tt := range errCases {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg
			_, _, err := parseSchedule(&cfg)
			if err == nil {
				t.Fatal("ожидали ошибку")
			}
			if tt.want != "" && !strings.Contains(err.Error(), tt.want) {
				t.Errorf("ошибка %q не содержит %q", err, tt.want)
			}
		})
	}
}

func TestValidateSchedule(t *testing.T) {
	if err := validateSchedule(nil); err == nil {
		t.Error("nil schedule должен быть ошибкой")
	}
	if err := validateSchedule(&ScheduleConfig{}); err == nil {
		t.Error("пустой at должен быть ошибкой")
	}
	if err := validateSchedule(&ScheduleConfig{At: []ScheduleAt{{Weekday: " , ", Time: "09:00"}}}); err == nil ||
		!strings.Contains(err.Error(), "no valid entries") {
		t.Errorf("weekday из одних запятых: получили %v", err)
	}
	if err := validateSchedule(&ScheduleConfig{At: []ScheduleAt{{Weekday: "mon", Time: ""}}}); err == nil ||
		!strings.Contains(err.Error(), "no valid entries") {
		t.Errorf("пустое время: получили %v", err)
	}
	if err := validateSchedule(&ScheduleConfig{Timezone: "Not/AZone", At: []ScheduleAt{{Weekday: "mon", Time: "09:00"}}}); err == nil {
		t.Error("неизвестный часовой пояс должен быть ошибкой")
	}
	if err := validateSchedule(&ScheduleConfig{Timezone: "Europe/Moscow", At: []ScheduleAt{{Weekday: "mon,fri", Time: "09:00,18:00"}}}); err != nil {
		t.Errorf("валидное расписание отвергнуто: %v", err)
	}
}

// entriesFor собирает записи расписания тем же путём, что и воркер.
func entriesFor(t *testing.T, tz string, at ...ScheduleAt) (*time.Location, []scheduleEntry) {
	t.Helper()
	loc, entries, err := parseSchedule(&ScheduleConfig{Timezone: tz, At: at})
	if err != nil {
		t.Fatal(err)
	}
	return loc, entries
}

func TestNextRun_UTC(t *testing.T) {
	// 2026-11-25 — среда.
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		at   []ScheduleAt
		want time.Time
	}{
		{"сегодня, позже текущего времени",
			[]ScheduleAt{{Weekday: "wed", Time: "10:01"}}, time.Date(2026, 11, 25, 10, 1, 0, 0, time.UTC)},
		{"ровно сейчас — следующая неделя (строго после now)",
			[]ScheduleAt{{Weekday: "wed", Time: "10:00"}}, time.Date(2026, 12, 2, 10, 0, 0, 0, time.UTC)},
		{"сегодня, но уже прошло — следующая неделя",
			[]ScheduleAt{{Weekday: "wed", Time: "09:59"}}, time.Date(2026, 12, 2, 9, 59, 0, 0, time.UTC)},
		{"понедельник — через 5 дней",
			[]ScheduleAt{{Weekday: "mon", Time: "09:00"}}, time.Date(2026, 11, 30, 9, 0, 0, 0, time.UTC)},
		{"воскресенье",
			[]ScheduleAt{{Weekday: "sun", Time: "23:59"}}, time.Date(2026, 11, 29, 23, 59, 0, 0, time.UTC)},
		{"из нескольких записей берётся ближайшая",
			[]ScheduleAt{{Weekday: "fri", Time: "12:00"}, {Weekday: "thu", Time: "08:00"}},
			time.Date(2026, 11, 26, 8, 0, 0, 0, time.UTC)},
		{"несколько дней и времён в одной записи",
			[]ScheduleAt{{Weekday: "wed, fri", Time: "09:00, 15:00"}},
			time.Date(2026, 11, 25, 15, 0, 0, 0, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loc, entries := entriesFor(t, "", tt.at...)
			got := nextRun(now, loc, entries)
			if !got.Equal(tt.want) {
				t.Errorf("nextRun = %v, ожидали %v", got, tt.want)
			}
			if !got.After(now) {
				t.Errorf("следующий запуск %v должен быть строго позже now %v", got, now)
			}
		})
	}
}

func TestNextRun_Timezone(t *testing.T) {
	msk, err := time.LoadLocation("Europe/Moscow") // UTC+3 круглый год
	if err != nil {
		t.Fatal(err)
	}
	// 20:30 UTC = 23:30 MSK, среда 2026-11-25.
	now := time.Date(2026, 11, 25, 20, 30, 0, 0, time.UTC)

	t.Run("через полночь в поясе расписания", func(t *testing.T) {
		loc, entries := entriesFor(t, "Europe/Moscow", ScheduleAt{Weekday: "thu", Time: "00:15"})
		got := nextRun(now, loc, entries)
		want := time.Date(2026, 11, 26, 0, 15, 0, 0, msk)
		if !got.Equal(want) {
			t.Errorf("nextRun = %v, ожидали %v", got, want)
		}
	})

	t.Run("день недели считается в поясе расписания, а не в UTC", func(t *testing.T) {
		// В UTC сейчас ещё среда 20:30, в Москве — тоже среда 23:30, но 23:45 MSK
		// (20:45 UTC) ещё впереди.
		loc, entries := entriesFor(t, "Europe/Moscow", ScheduleAt{Weekday: "wed", Time: "23:45"})
		got := nextRun(now, loc, entries)
		want := time.Date(2026, 11, 25, 23, 45, 0, 0, msk)
		if !got.Equal(want) {
			t.Errorf("nextRun = %v, ожидали %v", got, want)
		}
	})
}

func TestSameLabelsAndSchedule(t *testing.T) {
	a := map[string]string{"x": "1", "y": "2"}
	b := map[string]string{"y": "2", "x": "1"}
	c := map[string]string{"x": "1", "y": "3"}
	d := map[string]string{"x": "1", "z": "2"}

	if !sameLabelKeys(a, b) || !sameLabelValues(a, b) {
		t.Error("одинаковые карты должны быть равны")
	}
	if !sameLabelKeys(a, c) || sameLabelValues(a, c) {
		t.Error("тот же набор ключей, другое значение: ключи равны, значения нет")
	}
	if sameLabelKeys(a, d) {
		t.Error("разные имена лейблов — разная схема")
	}
	if !sameLabelKeys(nil, map[string]string{}) || !sameLabelValues(nil, map[string]string{}) {
		t.Error("nil и пустая карта равны")
	}

	s1 := &ScheduleConfig{Timezone: "UTC", At: []ScheduleAt{{Weekday: "mon", Time: "09:00"}}}
	s2 := &ScheduleConfig{Timezone: "UTC", At: []ScheduleAt{{Weekday: "mon", Time: "09:00"}}}
	s3 := &ScheduleConfig{Timezone: "UTC", At: []ScheduleAt{{Weekday: "mon", Time: "09:01"}}}
	s4 := &ScheduleConfig{Timezone: "Europe/Moscow", At: []ScheduleAt{{Weekday: "mon", Time: "09:00"}}}
	switch {
	case !sameSchedule(nil, nil):
		t.Error("nil и nil равны")
	case sameSchedule(s1, nil) || sameSchedule(nil, s1):
		t.Error("nil и непустое расписание различаются")
	case !sameSchedule(s1, s2):
		t.Error("одинаковые расписания равны")
	case sameSchedule(s1, s3):
		t.Error("разное время — разные расписания")
	case sameSchedule(s1, s4):
		t.Error("разный пояс — разные расписания")
	}
}

func TestSameQueryConfig(t *testing.T) {
	base := func() QueryConfig {
		return QueryConfig{
			DB: "d", SQL: "select 1", Timeout: "5s", Interval: "10s",
			ValueColumn: "v", MaxRows: 100, Labels: map[string]string{"team": "a"},
		}
	}
	if !sameQueryConfig(base(), base()) {
		t.Fatal("одинаковые конфиги должны считаться равными")
	}

	changes := map[string]func(*QueryConfig){
		"db":           func(q *QueryConfig) { q.DB = "other" },
		"sql":          func(q *QueryConfig) { q.SQL = "select 2" },
		"timeout":      func(q *QueryConfig) { q.Timeout = "6s" },
		"interval":     func(q *QueryConfig) { q.Interval = "11s" },
		"value_column": func(q *QueryConfig) { q.ValueColumn = "w" },
		"max_rows":     func(q *QueryConfig) { q.MaxRows = 200 },
		"label value":  func(q *QueryConfig) { q.Labels = map[string]string{"team": "b"} },
		"label name":   func(q *QueryConfig) { q.Labels = map[string]string{"owner": "a"} },
		"schedule": func(q *QueryConfig) {
			q.Schedule = &ScheduleConfig{At: []ScheduleAt{{Weekday: "mon", Time: "09:00"}}}
		},
	}
	for name, mutate := range changes {
		q := base()
		mutate(&q)
		if sameQueryConfig(base(), q) {
			t.Errorf("изменение %q должно делать конфиги разными (иначе воркер не перезапустится)", name)
		}
	}
}
