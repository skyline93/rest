package rest

import (
	"context"

	"github.com/pkg/errors"
	"github.com/restic/chunker"
	log "github.com/sirupsen/logrus"
)

type Config struct {
	Version           uint        `json:"version"`
	ID                string      `json:"id"`
	ChunkerPolynomial chunker.Pol `json:"chunker_polynomial"`
}

const MinRepoVersion = 1
const MaxRepoVersion = 2

func CreateConfig(version uint) (Config, error) {
	var (
		err error
		cfg Config
	)

	cfg.ChunkerPolynomial, err = chunker.RandomPolynomial()
	if err != nil {
		return Config{}, errors.Wrap(err, "chunker.RandomPolynomial")
	}

	cfg.ID = NewRandomID().String()
	cfg.Version = version

	log.Infof("New config: %#v", cfg)
	return cfg, nil
}

func SaveConfig(ctx context.Context, r SaverUnpacked, cfg Config) error {
	_, err := SaveJSONUnpacked(ctx, r, ConfigFile, cfg)
	return err
}

var checkPolynomial = true

// LoadConfig returns loads, checks and returns the config for a repository.
func LoadConfig(ctx context.Context, r LoaderUnpacked) (Config, error) {
	var (
		cfg Config
	)

	err := LoadJSONUnpacked(ctx, r, ConfigFile, ID{}, &cfg)
	if err != nil {
		return Config{}, err
	}

	if cfg.Version < MinRepoVersion || cfg.Version > MaxRepoVersion {
		return Config{}, errors.Errorf("unsupported repository version %v", cfg.Version)
	}

	if checkPolynomial {
		if !cfg.ChunkerPolynomial.Irreducible() {
			return Config{}, errors.New("invalid chunker polynomial")
		}
	}

	return cfg, nil
}
