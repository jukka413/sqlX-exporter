package main

import (
	"errors"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Databases map[string]DBConfig    `yaml:"databases"`
	Queries   map[string]QueryConfig `yaml:"queries"`
}

type DBConfig struct {
	URL      string `yaml:"url"`
	MaxConns int    `yaml:"max_conns"`
	MinConns int    `yaml:"min_conns"`
}

type QueryConfig struct {
	DB       string `yaml:"db"`
	SQL      string `yaml:"sql"`
	Timeout  string `yaml:"timeout"`
	Interval string `yaml:"interval"`

	Schedule *ScheduleConfig `yaml:"schedule,omitempty"`
}

type ScheduleConfig struct {
	Timezone string       `yaml:"timezone,omitempty"`
	At       []ScheduleAt `yaml:"at"`
}

type ScheduleAt struct {
	Weekday string `yaml:"weekday"`
	Time    string `yaml:"time"`
}

func loadConfig(path string) (Config, error) {
	var cfg Config
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	err = yaml.Unmarshal(data, &cfg)
	return cfg, err
}

func validateConfigDurations(cfg Config) error {
	for name, q := range cfg.Queries {
		if _, err := time.ParseDuration(q.Timeout); err != nil {
			return errors.New("query " + name + " has invalid timeout: " + err.Error())
		}

		if q.Schedule != nil {
			if err := validateSchedule(q.Schedule); err != nil {
				return errors.New("query " + name + " has invalid schedule: " + err.Error())
			}
		} else {
			if _, err := time.ParseDuration(q.Interval); err != nil {
				return errors.New("query " + name + " has invalid interval: " + err.Error())
			}
		}
	}
	return nil
}
