package rest

import (
	"context"
	"log"

	"github.com/pkg/errors"
	"github.com/restic/chunker"
)

const MinRepoVersion = 1
const MaxRepoVersion = 2

// Config contains the configuration for a repository.
type Config struct {
	Version           uint        `json:"version"`
	ID                string      `json:"id"`
	ChunkerPolynomial chunker.Pol `json:"chunker_polynomial"`
}

// CreateConfig creates a config file with a randomly selected polynomial and
// ID.
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

	log.Printf("New config: %#v", cfg)
	return cfg, nil
}

func SaveConfig(ctx context.Context, r SaverUnpacked, cfg Config) error {
	_, err := SaveJSONUnpacked(ctx, r, ConfigFile, cfg)
	return err
}
