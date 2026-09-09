package main

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type scheduleEntry struct {
	weekday time.Weekday
	hour    int
	min     int
}

func validateSchedule(s *ScheduleConfig) error {
	if s == nil {
		return errors.New("schedule is nil")
	}
	if s.Timezone != "" {
		if _, err := time.LoadLocation(s.Timezone); err != nil {
			return err
		}
	}
	if len(s.At) == 0 {
		return errors.New("schedule.at is empty")
	}
	for _, a := range s.At {
		weekdays := splitComma(a.Weekday)
		if len(weekdays) == 0 {
			return fmt.Errorf("schedule.at: weekday %q has no valid entries", a.Weekday)
		}
		for _, wd := range weekdays {
			if _, err := parseWeekday(wd); err != nil {
				return err
			}
		}

		times := splitComma(a.Time)
		if len(times) == 0 {
			return fmt.Errorf("schedule.at: time %q has no valid entries", a.Time)
		}
		for _, t := range times {
			if _, _, err := parseHHMM(t); err != nil {
				return err
			}
		}
	}
	return nil
}

// sameQueryConfig решает нужен ли рестарт воркера при изменении конфига.
func sameQueryConfig(a, b QueryConfig) bool {
	return a.DB == b.DB &&
		a.SQL == b.SQL &&
		a.Timeout == b.Timeout &&
		a.Interval == b.Interval &&
		a.ValueColumn == b.ValueColumn &&
		a.MaxRows == b.MaxRows &&
		sameLabelKeys(a.Labels, b.Labels) &&
		sameLabelValues(a.Labels, b.Labels) &&
		sameSchedule(a.Schedule, b.Schedule)
}

// sameLabelKeys сравнивает только имена лейблов — это определяет схему
// GaugeVec. Смена значения при том же наборе имён — не смена схемы.
func sameLabelKeys(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

// sameLabelValues сравнивает значения (набор ключей уже проверен sameLabelKeys) —
// отвечает на вопрос "изменилась ли identity time series".
func sameLabelValues(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

func sameSchedule(a, b *ScheduleConfig) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.Timezone != b.Timezone {
		return false
	}
	if len(a.At) != len(b.At) {
		return false
	}
	for i := range a.At {
		if a.At[i].Weekday != b.At[i].Weekday || a.At[i].Time != b.At[i].Time {
			return false
		}
	}
	return true
}

func parseSchedule(cfg *ScheduleConfig) (*time.Location, []scheduleEntry, error) {
	// UTC, не time.Local — иначе время срабатывания schedule: зависело бы
	// от TZ окружения контейнера/базового образа, а не только от текста
	// конфига. Одинаковый конфиг в разных подах мог бы сработать в разное
	// время просто из-за разницы в сборке образа.
	loc := time.UTC
	var err error
	if cfg.Timezone != "" {
		loc, err = time.LoadLocation(cfg.Timezone)
		if err != nil {
			return nil, nil, err
		}
	}

	var out []scheduleEntry

	for _, a := range cfg.At {
		weekdays := splitComma(a.Weekday)
		times := splitComma(a.Time)

		// Генерируем все комбинации weekday × time
		for _, wdStr := range weekdays {
			wd, err := parseWeekday(wdStr)
			if err != nil {
				return nil, nil, err
			}
			for _, tStr := range times {
				h, m, err := parseHHMM(tStr)
				if err != nil {
					return nil, nil, err
				}
				out = append(out, scheduleEntry{weekday: wd, hour: h, min: m})
			}
		}
	}

	if len(out) == 0 {
		return nil, nil, errors.New("schedule.at is empty")
	}

	return loc, out, nil
}

func nextRun(now time.Time, loc *time.Location, entries []scheduleEntry) time.Time {
	n := now.In(loc)

	var best time.Time
	for _, e := range entries {
		c := time.Date(n.Year(), n.Month(), n.Day(), e.hour, e.min, 0, 0, loc)
		dayDiff := (int(e.weekday) - int(n.Weekday()) + 7) % 7
		c = c.AddDate(0, 0, dayDiff)
		if !c.After(n) {
			c = c.AddDate(0, 0, 7)
		}
		if best.IsZero() || c.Before(best) {
			best = c
		}
	}
	return best
}

// splitComma разбивает строку по запятой и триммирует пробелы.
// "mon, wed" → ["mon", "wed"]
// "mon"      → ["mon"]
func splitComma(s string) []string {
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if v := strings.TrimSpace(p); v != "" {
			result = append(result, v)
		}
	}
	return result
}

func parseHHMM(s string) (int, int, error) {
	t, err := time.Parse("15:04", strings.TrimSpace(s))
	if err != nil {
		return 0, 0, err
	}
	return t.Hour(), t.Minute(), nil
}

func parseWeekday(s string) (time.Weekday, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "sun", "sunday":
		return time.Sunday, nil
	case "mon", "monday":
		return time.Monday, nil
	case "tue", "tues", "tuesday":
		return time.Tuesday, nil
	case "wed", "wednesday":
		return time.Wednesday, nil
	case "thu", "thurs", "thursday":
		return time.Thursday, nil
	case "fri", "friday":
		return time.Friday, nil
	case "sat", "saturday":
		return time.Saturday, nil
	default:
		return 0, errors.New("invalid weekday: " + s)
	}
}
