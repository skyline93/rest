package rest

import (
	"context"
	"os/user"
	"path/filepath"
	"time"
)

// Snapshot is the state of a resource at one point in time.
type Snapshot struct {
	Time     time.Time `json:"time"`
	Parent   *ID       `json:"parent,omitempty"`
	Tree     *ID       `json:"tree"`
	Paths    []string  `json:"paths"`
	Hostname string    `json:"hostname,omitempty"`
	Username string    `json:"username,omitempty"`
	UID      uint32    `json:"uid,omitempty"`
	GID      uint32    `json:"gid,omitempty"`
	Excludes []string  `json:"excludes,omitempty"`
	Tags     []string  `json:"tags,omitempty"`
	Original *ID       `json:"original,omitempty"`

	ProgramVersion string `json:"program_version,omitempty"`

	id *ID // plaintext ID, used during restore
}

// NewSnapshot returns an initialized snapshot struct for the current user and
// time.
func NewSnapshot(paths []string, tags []string, hostname string, time time.Time) (*Snapshot, error) {
	absPaths := make([]string, 0, len(paths))
	for _, path := range paths {
		p, err := filepath.Abs(path)
		if err == nil {
			absPaths = append(absPaths, p)
		} else {
			absPaths = append(absPaths, path)
		}
	}

	sn := &Snapshot{
		Paths:    absPaths,
		Time:     time,
		Tags:     tags,
		Hostname: hostname,
	}

	err := sn.fillUserInfo()
	if err != nil {
		return nil, err
	}

	return sn, nil
}

func (sn *Snapshot) fillUserInfo() error {
	usr, err := user.Current()
	if err != nil {
		return nil
	}
	sn.Username = usr.Username

	// set userid and groupid
	sn.UID, sn.GID, err = uidGidInt(usr)
	return err
}

// SaveSnapshot saves the snapshot sn and returns its ID.
func SaveSnapshot(ctx context.Context, repo SaverUnpacked, sn *Snapshot) (ID, error) {
	return SaveJSONUnpacked(ctx, repo, SnapshotFile, sn)
}
