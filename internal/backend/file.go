package backend

type FileType uint8

const (
	PackFile FileType = 1 + iota
	KeyFile
	LockFile
	SnapshotFile
	IndexFile
	ConfigFile
)

func (t FileType) String() string {
	s := "invalid"
	switch t {
	case PackFile:
		// Spelled "data" instead of "pack" for historical reasons.
		s = "data"
	case KeyFile:
		s = "key"
	case LockFile:
		s = "lock"
	case SnapshotFile:
		s = "snapshot"
	case IndexFile:
		s = "index"
	case ConfigFile:
		s = "config"
	}
	return s
}

type Handle struct {
	Type       FileType
	IsMetadata bool
	Name       string
}
