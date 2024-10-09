package rest

import (
	"context"
	"fmt"
	"os/user"
	"path/filepath"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
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

	ProgramVersion string           `json:"program_version,omitempty"`
	Summary        *SnapshotSummary `json:"summary,omitempty"`

	id *ID // plaintext ID, used during restore
}

type SnapshotSummary struct {
	BackupStart time.Time `json:"backup_start"`
	BackupEnd   time.Time `json:"backup_end"`

	// statistics from the backup json output
	FilesNew            uint   `json:"files_new"`
	FilesChanged        uint   `json:"files_changed"`
	FilesUnmodified     uint   `json:"files_unmodified"`
	DirsNew             uint   `json:"dirs_new"`
	DirsChanged         uint   `json:"dirs_changed"`
	DirsUnmodified      uint   `json:"dirs_unmodified"`
	DataBlobs           int    `json:"data_blobs"`
	TreeBlobs           int    `json:"tree_blobs"`
	DataAdded           uint64 `json:"data_added"`
	DataAddedPacked     uint64 `json:"data_added_packed"`
	TotalFilesProcessed uint   `json:"total_files_processed"`
	TotalBytesProcessed uint64 `json:"total_bytes_processed"`
}

// ForAllSnapshots reads all snapshots in parallel and calls the
// given function. It is guaranteed that the function is not run concurrently.
// If the called function returns an error, this function is cancelled and
// also returns this error.
// If a snapshot ID is in excludeIDs, it will be ignored.
func ForAllSnapshots(ctx context.Context, be Lister, loader LoaderUnpacked, excludeIDs IDSet, fn func(ID, *Snapshot, error) error) error {
	var m sync.Mutex

	// For most snapshots decoding is nearly for free, thus just assume were only limited by IO
	return ParallelList(ctx, be, SnapshotFile, loader.Connections(), func(ctx context.Context, id ID, _ int64) error {
		if excludeIDs.Has(id) {
			return nil
		}

		sn, err := LoadSnapshot(ctx, loader, id)
		m.Lock()
		defer m.Unlock()
		return fn(id, sn, err)
	})
}

// LoadSnapshot loads the snapshot with the id and returns it.
func LoadSnapshot(ctx context.Context, loader LoaderUnpacked, id ID) (*Snapshot, error) {
	sn := &Snapshot{id: &id}
	err := LoadJSONUnpacked(ctx, loader, SnapshotFile, id, sn)
	if err != nil {
		return nil, fmt.Errorf("failed to load snapshot %v: %w", id.Str(), err)
	}

	return sn, nil
}

// HasHostname returns true if either
// - the snapshot hostname is in the list of the given hostnames, or
// - the list of given hostnames is empty
func (sn *Snapshot) HasHostname(hostnames []string) bool {
	if len(hostnames) == 0 {
		return true
	}

	for _, hostname := range hostnames {
		if sn.Hostname == hostname {
			return true
		}
	}

	return false
}

// HasTagList returns true if either
//   - the snapshot satisfies at least one TagList, so there is a TagList in l
//     for which all tags are included in sn, or
//   - l is empty
func (sn *Snapshot) HasTagList(l []TagList) bool {
	log.Infof("testing snapshot with tags %v against list: %v", sn.Tags, l)

	if len(l) == 0 {
		return true
	}

	for _, tags := range l {
		if sn.HasTags(tags) {
			log.Infof("  snapshot satisfies %v %v", tags, l)
			return true
		}
	}

	return false
}

// HasTags returns true if the snapshot has all the tags in l.
func (sn *Snapshot) HasTags(l []string) bool {
	for _, tag := range l {
		if tag == "" && len(sn.Tags) == 0 {
			return true
		}
		if !sn.hasTag(tag) {
			return false
		}
	}

	return true
}

func (sn *Snapshot) hasTag(tag string) bool {
	for _, snTag := range sn.Tags {
		if tag == snTag {
			return true
		}
	}
	return false
}

// HasPaths returns true if the snapshot has all of the paths.
func (sn *Snapshot) HasPaths(paths []string) bool {
	m := make(map[string]struct{}, len(sn.Paths))
	for _, snPath := range sn.Paths {
		m[snPath] = struct{}{}
	}
	for _, path := range paths {
		if _, ok := m[path]; !ok {
			return false
		}
	}

	return true
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

// ID returns the snapshot's ID.
func (sn Snapshot) ID() *ID {
	return sn.id
}

// SaveSnapshot saves the snapshot sn and returns its ID.
func SaveSnapshot(ctx context.Context, repo SaverUnpacked, sn *Snapshot) (ID, error) {
	return SaveJSONUnpacked(ctx, repo, SnapshotFile, sn)
}
