package rest

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// SaveJSONUnpacked serialises item as JSON and encrypts and saves it in the
// backend as type t, without a pack. It returns the storage hash.
func SaveJSONUnpacked(ctx context.Context, repo SaverUnpacked, t FileType, item interface{}) (ID, error) {
	log.Infof("save new blob %v", t)
	plaintext, err := json.Marshal(item)
	if err != nil {
		return ID{}, errors.Wrap(err, "json.Marshal")
	}

	return repo.SaveUnpacked(ctx, t, plaintext)
}
