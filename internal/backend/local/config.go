package local

import (
	"errors"
	"strings"
)

type Config struct {
	Path   string
	Layout string `option:"layout" help:"use this backend directory layout (default: auto-detect) (deprecated)"`

	Connections uint `option:"connections" help:"set a limit for the number of concurrent operations (default: 2)"`
}

func NewConfig() Config {
	return Config{
		Connections: 2,
	}
}

func ParseConfig(s string) (*Config, error) {
	if !strings.HasPrefix(s, "local:") {
		return nil, errors.New(`invalid format, prefix "local" not found`)
	}

	cfg := NewConfig()
	cfg.Path = s[6:]
	return &cfg, nil
}
