package main

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// schedule.expr — одно или несколько окон через ";":
//
//	expr   := window (";" window)*
//	window := [days] HH:MM-HH:MM "/" STEP
//	days   := ["md" list] ["wd" list]     // необязательно; без дней — каждый день
//	list   := item ("," item)*
//	item   := N | N "-" M
//	STEP   := N ("s" | "m" | "h")
//
// Пример: "wd1-5 09:00-19:00/5m" — пн-пт с 09:00 до 19:00 каждые 5 минут.
//
// md — день месяца 1-31, wd — день недели 1-7 (1 = понедельник, 7 = воскресенье).
// Если указаны оба, день должен подходить под оба условия.
// Окно включает оба края; сетка шага отсчитывается от начала окна.

// scheduleSpec — один разобранный window из expr.
type scheduleSpec struct {
	days  [32]bool // 1..31; индекс 0 не используется
	wdays [7]bool  // time.Weekday: 0 = воскресенье
	// times — подходящие моменты внутри суток, секунды от полуночи, по возрастанию.
	times []int
}

type exprUnit struct {
	name     string
	min, max int
}

var (
	exprDayUnits = []exprUnit{{"md", 1, 31}, {"wd", 1, 7}}
	exprStepRe   = regexp.MustCompile(`^([0-9]+)([smh])$`)
)

// maxExprLookaheadDays — как далеко вперёд ищем подходящий день. Пять лет
// покрывают любые комбинации md/wd (например, пятница 13-го); дальше — "никогда".
const maxExprLookaheadDays = 366 * 5

const exprFormatHint = `expected "[days] HH:MM-HH:MM/STEP", e.g. "wd1-5 09:00-19:00/5m"`

func parseScheduleExpr(expr string) ([]scheduleSpec, error) {
	if strings.TrimSpace(expr) == "" {
		return nil, errors.New("expr is empty")
	}
	var specs []scheduleSpec
	for _, part := range strings.Split(expr, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("%q: empty window (stray ';')", expr)
		}
		spec, err := parseExprWindow(part)
		if err != nil {
			return nil, fmt.Errorf("%q: %w", part, err)
		}
		specs = append(specs, spec)
	}
	return specs, nil
}

// parseExprWindow разбирает "[дни] HH:MM-HH:MM/ШАГ", например "wd1-5 09:00-19:00/5m".
// Сетка шага отсчитывается от начала окна, конец включается, если попадает на сетку.
func parseExprWindow(s string) (scheduleSpec, error) {
	fields := strings.Fields(s)
	if len(fields) == 0 || len(fields) > 2 {
		return scheduleSpec{}, errors.New(exprFormatHint)
	}
	windowText := fields[len(fields)-1]
	if !strings.Contains(windowText, ":") {
		return scheduleSpec{}, fmt.Errorf("no time window found: %s", exprFormatHint)
	}

	var spec scheduleSpec
	for d := 1; d <= 31; d++ {
		spec.days[d] = true
	}
	for w := 0; w < 7; w++ {
		spec.wdays[w] = true
	}
	if len(fields) == 2 {
		if err := parseExprDays(fields[0], &spec); err != nil {
			return scheduleSpec{}, err
		}
	}

	rangeText, stepText, hasStep := strings.Cut(windowText, "/")
	if !hasStep {
		return scheduleSpec{}, fmt.Errorf("window %q needs a step, e.g. %s/5m", windowText, windowText)
	}
	fromText, toText, isRange := strings.Cut(rangeText, "-")
	if !isRange {
		return scheduleSpec{}, fmt.Errorf("window %q: expected HH:MM-HH:MM", rangeText)
	}
	from, err := parseExprClock(fromText)
	if err != nil {
		return scheduleSpec{}, err
	}
	to, err := parseExprClock(toText)
	if err != nil {
		return scheduleSpec{}, err
	}
	if from > to {
		return scheduleSpec{}, fmt.Errorf("window %q: the end is before the start; a window cannot cross midnight, "+
			"split it into two (e.g. 22:00-23:59/5m;00:00-02:00/5m)", rangeText)
	}

	m := exprStepRe.FindStringSubmatch(stepText)
	if m == nil {
		return scheduleSpec{}, fmt.Errorf("step %q: expected a number with a unit s, m or h (e.g. 5m)", stepText)
	}
	n, err := strconv.Atoi(m[1])
	if err != nil || n < 1 {
		return scheduleSpec{}, fmt.Errorf("step %q must be a positive number", stepText)
	}
	step := n
	switch m[2] {
	case "m":
		step = n * 60
	case "h":
		step = n * 3600
	}

	for t := from; t <= to; t += step {
		spec.times = append(spec.times, t)
	}
	return spec, nil
}

// parseExprDays разбирает часть с днями ("wd1-5", "md1,15", "md1wd1") и
// сужает spec.days / spec.wdays.
func parseExprDays(text string, spec *scheduleSpec) error {
	rest := text
	for _, unit := range exprDayUnits {
		if !strings.HasPrefix(rest, unit.name) {
			continue
		}
		rest = rest[len(unit.name):]
		j := 0
		for j < len(rest) && (rest[j] >= '0' && rest[j] <= '9' || rest[j] == ',' || rest[j] == '-') {
			j++
		}
		if j == 0 {
			return fmt.Errorf("days %q: %q has no value", text, unit.name)
		}
		vals, err := parseExprValues(unit, rest[:j])
		if err != nil {
			return err
		}
		rest = rest[j:]

		if unit.name == "md" {
			spec.days = [32]bool{}
			for _, d := range vals {
				spec.days[d] = true
			}
		} else {
			spec.wdays = [7]bool{}
			for _, w := range vals {
				spec.wdays[w%7] = true // 7 (воскресенье) → time.Sunday = 0
			}
		}
	}
	if rest != "" {
		return fmt.Errorf("days %q: unexpected %q — only md and wd are allowed before the time window (e.g. wd1-5)", text, rest)
	}
	return nil
}

func parseExprValues(unit exprUnit, text string) ([]int, error) {
	var out []int
	for _, item := range strings.Split(text, ",") {
		if item == "" {
			return nil, fmt.Errorf("%q: empty value in list %q", unit.name, text)
		}
		if a, b, isRange := strings.Cut(item, "-"); isRange {
			lo, err := parseExprNumber(unit, a)
			if err != nil {
				return nil, err
			}
			hi, err := parseExprNumber(unit, b)
			if err != nil {
				return nil, err
			}
			if lo > hi {
				return nil, fmt.Errorf("%q: range %q is reversed", unit.name, item)
			}
			for x := lo; x <= hi; x++ {
				out = append(out, x)
			}
			continue
		}
		n, err := parseExprNumber(unit, item)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}

func parseExprNumber(unit exprUnit, s string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("%q: %q is not a number", unit.name, s)
	}
	if n < unit.min || n > unit.max {
		return 0, fmt.Errorf("%q: %d is out of range %d-%d", unit.name, n, unit.min, unit.max)
	}
	return n, nil
}

// parseExprClock: "HH:MM" → секунды от полуночи. Допустимо 00:00–23:59.
func parseExprClock(s string) (int, error) {
	hh, mm, ok := strings.Cut(s, ":")
	if !ok || len(hh) == 0 || len(hh) > 2 || len(mm) != 2 {
		return 0, fmt.Errorf("time %q: expected HH:MM", s)
	}
	h, err1 := strconv.Atoi(hh)
	m, err2 := strconv.Atoi(mm)
	if err1 != nil || err2 != nil || h < 0 || h > 23 || m < 0 || m > 59 {
		return 0, fmt.Errorf("time %q: expected HH:MM between 00:00 and 23:59", s)
	}
	return h*3600 + m*60, nil
}

// nextRunExpr — ближайший момент строго после now по любому из specs;
// нулевое время, если подходящего нет в пределах maxExprLookaheadDays.
func nextRunExpr(now time.Time, loc *time.Location, specs []scheduleSpec) time.Time {
	n := now.In(loc)
	nowSec := n.Hour()*3600 + n.Minute()*60 + n.Second()

	var best time.Time
	for i := range specs {
		spec := &specs[i]
		if len(spec.times) == 0 {
			continue
		}
		for off := 0; off < maxExprLookaheadDays; off++ {
			day := time.Date(n.Year(), n.Month(), n.Day()+off, 12, 0, 0, 0, loc) // 12:00 — вдали от границ перехода на летнее время
			if !spec.days[day.Day()] || !spec.wdays[day.Weekday()] {
				continue
			}
			after := -1
			if off == 0 {
				after = nowSec
			}
			k := sort.SearchInts(spec.times, after+1)
			if k == len(spec.times) {
				continue
			}
			t := spec.times[k]
			cand := time.Date(day.Year(), day.Month(), day.Day(), t/3600, t%3600/60, t%60, 0, loc)
			if !cand.After(n) { // страховка на случай сдвига при переходе на летнее время
				continue
			}
			if best.IsZero() || cand.Before(best) {
				best = cand
			}
			break
		}
	}
	return best
}

// buildScheduleNext собирает функцию "когда следующий запуск" для schedule:
// объединяет at и expr. Возвращает нулевое время, если запусков больше нет.
func buildScheduleNext(cfg *ScheduleConfig) (func(now time.Time) time.Time, error) {
	var nexts []func(time.Time) time.Time

	if len(cfg.At) > 0 {
		loc, entries, err := parseSchedule(cfg)
		if err != nil {
			return nil, err
		}
		nexts = append(nexts, func(now time.Time) time.Time { return nextRun(now, loc, entries) })
	}

	if strings.TrimSpace(cfg.Expr) != "" {
		loc := time.UTC
		if cfg.Timezone != "" {
			var err error
			if loc, err = time.LoadLocation(cfg.Timezone); err != nil {
				return nil, err
			}
		}
		specs, err := parseScheduleExpr(cfg.Expr)
		if err != nil {
			return nil, fmt.Errorf("schedule.expr: %w", err)
		}
		nexts = append(nexts, func(now time.Time) time.Time { return nextRunExpr(now, loc, specs) })
	}

	if len(nexts) == 0 {
		return nil, errors.New("schedule.at is empty")
	}
	return func(now time.Time) time.Time {
		var best time.Time
		for _, f := range nexts {
			if t := f(now); !t.IsZero() && (best.IsZero() || t.Before(best)) {
				best = t
			}
		}
		return best
	}, nil
}
