package repository

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/user"
	"time"

	"github.com/pkg/errors"
	"github.com/skyline93/rest/internal/crypto"
	"github.com/skyline93/rest/internal/rest"
)

// Key represents an encrypted master key for a repository.
type Key struct {
	Created  time.Time `json:"created"`
	Username string    `json:"username"`
	Hostname string    `json:"hostname"`

	KDF  string `json:"kdf"`
	N    int    `json:"N"`
	R    int    `json:"r"`
	P    int    `json:"p"`
	Salt []byte `json:"salt"`
	Data []byte `json:"data"`

	user   *crypto.Key
	master *crypto.Key

	id rest.ID
}

// Params tracks the parameters used for the KDF. If not set, it will be
// calibrated on the first run of AddKey().
var Params *crypto.Params

var (
	// KDFTimeout specifies the maximum runtime for the KDF.
	KDFTimeout = 500 * time.Millisecond

	// KDFMemory limits the memory the KDF is allowed to use.
	KDFMemory = 60
)

// createMasterKey creates a new master key in the given backend and encrypts
// it with the password.
func createMasterKey(ctx context.Context, s *Repository, password string) (*Key, error) {
	return AddKey(ctx, s, password, "", "", nil)
}

// AddKey adds a new key to an already existing repository.
func AddKey(ctx context.Context, s *Repository, password, username, hostname string, template *crypto.Key) (*Key, error) {
	// make sure we have valid KDF parameters
	if Params == nil {
		p, err := crypto.Calibrate(KDFTimeout, KDFMemory)
		if err != nil {
			return nil, errors.Wrap(err, "Calibrate")
		}

		Params = &p
		log.Printf("calibrated KDF parameters are %v", p)
	}

	// fill meta data about key
	newkey := &Key{
		Created:  time.Now(),
		Username: username,
		Hostname: hostname,

		KDF: "scrypt",
		N:   Params.N,
		R:   Params.R,
		P:   Params.P,
	}

	if newkey.Hostname == "" {
		newkey.Hostname, _ = os.Hostname()
	}

	if newkey.Username == "" {
		usr, err := user.Current()
		if err == nil {
			newkey.Username = usr.Username
		}
	}

	// generate random salt
	var err error
	newkey.Salt, err = crypto.NewSalt()
	if err != nil {
		panic("unable to read enough random bytes for salt: " + err.Error())
	}

	// call KDF to derive user key
	newkey.user, err = crypto.KDF(*Params, newkey.Salt, password)
	if err != nil {
		return nil, err
	}

	if template == nil {
		// generate new random master keys
		newkey.master = crypto.NewRandomKey()
	} else {
		// copy master keys from old key
		newkey.master = template
	}

	// encrypt master keys (as json) with user key
	buf, err := json.Marshal(newkey.master)
	if err != nil {
		return nil, errors.Wrap(err, "Marshal")
	}

	nonce := crypto.NewRandomNonce()
	ciphertext := make([]byte, 0, crypto.CiphertextLength(len(buf)))
	ciphertext = append(ciphertext, nonce...)
	ciphertext = newkey.user.Seal(ciphertext, nonce, buf, nil)
	newkey.Data = ciphertext

	// dump as json
	buf, err = json.Marshal(newkey)
	if err != nil {
		return nil, errors.Wrap(err, "Marshal")
	}

	id := rest.Hash(buf)
	// store in repository and return
	h := rest.Handle{
		Type: rest.KeyFile,
		Name: id.String(),
	}

	err = s.be.Save(ctx, h, rest.NewByteReader(buf, s.be.Hasher()))
	if err != nil {
		return nil, err
	}

	newkey.id = id

	return newkey, nil
}

// ID returns an identifier for the key.
func (k Key) ID() rest.ID {
	return k.id
}
