package main

import (
	"errors"
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
		// Валидируем каждый элемент через splitComma — поддерживаем "mon,wed" и "03:00,14:39"
		for _, wd := range splitComma(a.Weekday) {
			if _, err := parseWeekday(wd); err != nil {
				return err
			}
		}
		for _, t := range splitComma(a.Time) {
			if _, _, err := parseHHMM(t); err != nil {
				return err
			}
		}
	}
	return nil
}

func sameQueryConfig(a, b QueryConfig) bool {
	return a.DB == b.DB &&
		a.SQL == b.SQL &&
		a.Timeout == b.Timeout &&
		a.Interval == b.Interval &&
		sameSchedule(a.Schedule, b.Schedule)
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
	loc := time.Local
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
