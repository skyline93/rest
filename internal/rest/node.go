package rest

import (
	"bytes"
	"encoding/json"
	"os"
	"os/user"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/skyline93/rest/internal/errors"
	"github.com/skyline93/rest/internal/fs"
)

// ExtendedAttribute is a tuple storing the xattr name and value for various filesystems.
type ExtendedAttribute struct {
	Name  string `json:"name"`
	Value []byte `json:"value"`
}

// GenericAttributeType can be used for OS specific functionalities by defining specific types
// in node.go to be used by the specific node_xx files.
// OS specific attribute types should follow the convention <OS>Attributes.
// GenericAttributeTypes should follow the convention <OS specific attribute type>.<attribute name>
// The attributes in OS specific attribute types must be pointers as we want to distinguish nil values
// and not create GenericAttributes for them.
type GenericAttributeType string

// Node is a file, directory or other item in a backup.
type Node struct {
	Name       string      `json:"name"`
	Type       string      `json:"type"`
	Mode       os.FileMode `json:"mode,omitempty"`
	ModTime    time.Time   `json:"mtime,omitempty"`
	AccessTime time.Time   `json:"atime,omitempty"`
	ChangeTime time.Time   `json:"ctime,omitempty"`
	UID        uint32      `json:"uid"`
	GID        uint32      `json:"gid"`
	User       string      `json:"user,omitempty"`
	Group      string      `json:"group,omitempty"`
	Inode      uint64      `json:"inode,omitempty"`
	DeviceID   uint64      `json:"device_id,omitempty"` // device id of the file, stat.st_dev, only stored for hardlinks
	Size       uint64      `json:"size,omitempty"`
	Links      uint64      `json:"links,omitempty"`
	LinkTarget string      `json:"linktarget,omitempty"`
	// implicitly base64-encoded field. Only used while encoding, `linktarget_raw` will overwrite LinkTarget if present.
	// This allows storing arbitrary byte-sequences, which are possible as symlink targets on unix systems,
	// as LinkTarget without breaking backwards-compatibility.
	// Must only be set of the linktarget cannot be encoded as valid utf8.
	LinkTargetRaw      []byte                                   `json:"linktarget_raw,omitempty"`
	ExtendedAttributes []ExtendedAttribute                      `json:"extended_attributes,omitempty"`
	GenericAttributes  map[GenericAttributeType]json.RawMessage `json:"generic_attributes,omitempty"`
	Device             uint64                                   `json:"device,omitempty"` // in case of Type == "dev", stat.st_rdev
	Content            IDs                                      `json:"content"`
	Subtree            *ID                                      `json:"subtree,omitempty"`

	Error string `json:"error,omitempty"`

	Path string `json:"-"`
}

// NodeFromFileInfo returns a new node from the given path and FileInfo. It
// returns the first error that is encountered, together with a node.
func NodeFromFileInfo(path string, fi os.FileInfo, ignoreXattrListError bool) (*Node, error) {
	mask := os.ModePerm | os.ModeType | os.ModeSetuid | os.ModeSetgid | os.ModeSticky
	node := &Node{
		Path:    path,
		Name:    fi.Name(),
		Mode:    fi.Mode() & mask,
		ModTime: fi.ModTime(),
	}

	node.Type = nodeTypeFromFileInfo(fi)
	if node.Type == "file" {
		node.Size = uint64(fi.Size())
	}

	err := node.fillExtra(path, fi, ignoreXattrListError)
	return node, err
}

func nodeTypeFromFileInfo(fi os.FileInfo) string {
	switch fi.Mode() & os.ModeType {
	case 0:
		return "file"
	case os.ModeDir:
		return "dir"
	case os.ModeSymlink:
		return "symlink"
	case os.ModeDevice | os.ModeCharDevice:
		return "chardev"
	case os.ModeDevice:
		return "dev"
	case os.ModeNamedPipe:
		return "fifo"
	case os.ModeSocket:
		return "socket"
	case os.ModeIrregular:
		return "irregular"
	}

	return ""
}

func (node *Node) fillExtra(path string, fi os.FileInfo, ignoreXattrListError bool) error {
	stat, ok := toStatT(fi.Sys())
	if !ok {
		// fill minimal info with current values for uid, gid
		node.UID = uint32(os.Getuid())
		node.GID = uint32(os.Getgid())
		node.ChangeTime = node.ModTime
		return nil
	}

	node.Inode = uint64(stat.ino())
	node.DeviceID = uint64(stat.dev())

	node.fillTimes(stat)

	node.fillUser(stat)

	switch node.Type {
	case "file":
		node.Size = uint64(stat.size())
		node.Links = uint64(stat.nlink())
	case "dir":
	case "symlink":
		var err error
		node.LinkTarget, err = fs.Readlink(path)
		node.Links = uint64(stat.nlink())
		if err != nil {
			return errors.WithStack(err)
		}
	case "dev":
		node.Device = uint64(stat.rdev())
		node.Links = uint64(stat.nlink())
	case "chardev":
		node.Device = uint64(stat.rdev())
		node.Links = uint64(stat.nlink())
	case "fifo":
	case "socket":
	default:
		return errors.Errorf("unsupported file type %q", node.Type)
	}

	allowExtended, err := node.fillGenericAttributes(path, fi, stat)
	if allowExtended {
		// Skip processing ExtendedAttributes if allowExtended is false.
		err = errors.CombineErrors(err, node.fillExtendedAttributes(path, ignoreXattrListError))
	}
	return err
}

func (node *Node) fillTimes(stat *statT) {
	ctim := stat.ctim()
	atim := stat.atim()
	node.ChangeTime = time.Unix(ctim.Unix())
	node.AccessTime = time.Unix(atim.Unix())
}

func (node *Node) fillUser(stat *statT) {
	uid, gid := stat.uid(), stat.gid()
	node.UID, node.GID = uid, gid
	node.User = lookupUsername(uid)
	node.Group = lookupGroup(gid)
}

var (
	uidLookupCache      = make(map[uint32]string)
	uidLookupCacheMutex = sync.RWMutex{}
)

// Cached user name lookup by uid. Returns "" when no name can be found.
func lookupUsername(uid uint32) string {
	uidLookupCacheMutex.RLock()
	username, ok := uidLookupCache[uid]
	uidLookupCacheMutex.RUnlock()

	if ok {
		return username
	}

	u, err := user.LookupId(strconv.Itoa(int(uid)))
	if err == nil {
		username = u.Username
	}

	uidLookupCacheMutex.Lock()
	uidLookupCache[uid] = username
	uidLookupCacheMutex.Unlock()

	return username
}

var (
	gidLookupCache      = make(map[uint32]string)
	gidLookupCacheMutex = sync.RWMutex{}
)

// Cached group name lookup by gid. Returns "" when no name can be found.
func lookupGroup(gid uint32) string {
	gidLookupCacheMutex.RLock()
	group, ok := gidLookupCache[gid]
	gidLookupCacheMutex.RUnlock()

	if ok {
		return group
	}

	g, err := user.LookupGroupId(strconv.Itoa(int(gid)))
	if err == nil {
		group = g.Name
	}

	gidLookupCacheMutex.Lock()
	gidLookupCache[gid] = group
	gidLookupCacheMutex.Unlock()

	return group
}

func (node Node) Equals(other Node) bool {
	if node.Name != other.Name {
		return false
	}
	if node.Type != other.Type {
		return false
	}
	if node.Mode != other.Mode {
		return false
	}
	if !node.ModTime.Equal(other.ModTime) {
		return false
	}
	if !node.AccessTime.Equal(other.AccessTime) {
		return false
	}
	if !node.ChangeTime.Equal(other.ChangeTime) {
		return false
	}
	if node.UID != other.UID {
		return false
	}
	if node.GID != other.GID {
		return false
	}
	if node.User != other.User {
		return false
	}
	if node.Group != other.Group {
		return false
	}
	if node.Inode != other.Inode {
		return false
	}
	if node.DeviceID != other.DeviceID {
		return false
	}
	if node.Size != other.Size {
		return false
	}
	if node.Links != other.Links {
		return false
	}
	if node.LinkTarget != other.LinkTarget {
		return false
	}
	if node.Device != other.Device {
		return false
	}
	if !node.sameContent(other) {
		return false
	}
	if !node.sameExtendedAttributes(other) {
		return false
	}
	if !node.sameGenericAttributes(other) {
		return false
	}
	if node.Subtree != nil {
		if other.Subtree == nil {
			return false
		}

		if !node.Subtree.Equal(*other.Subtree) {
			return false
		}
	} else {
		if other.Subtree != nil {
			return false
		}
	}
	if node.Error != other.Error {
		return false
	}

	return true
}

func (node Node) sameContent(other Node) bool {
	if node.Content == nil {
		return other.Content == nil
	}

	if other.Content == nil {
		return false
	}

	if len(node.Content) != len(other.Content) {
		return false
	}

	for i := 0; i < len(node.Content); i++ {
		if !node.Content[i].Equal(other.Content[i]) {
			return false
		}
	}
	return true
}

func (node Node) sameExtendedAttributes(other Node) bool {
	ln := len(node.ExtendedAttributes)
	lo := len(other.ExtendedAttributes)
	if ln != lo {
		return false
	} else if ln == 0 {
		// This means lo is also of length 0
		return true
	}

	// build a set of all attributes that node has
	type mapvalue struct {
		value   []byte
		present bool
	}
	attributes := make(map[string]mapvalue)
	for _, attr := range node.ExtendedAttributes {
		attributes[attr.Name] = mapvalue{value: attr.Value}
	}

	for _, attr := range other.ExtendedAttributes {
		v, ok := attributes[attr.Name]
		if !ok {
			// extended attribute is not set for node
			log.Debugf("other node has attribute %v, which is not present in node", attr.Name)
			return false

		}

		if !bytes.Equal(v.value, attr.Value) {
			// attribute has different value
			log.Debugf("attribute %v has different value", attr.Name)
			return false
		}

		// remember that this attribute is present in other.
		v.present = true
		attributes[attr.Name] = v
	}

	// check for attributes that are not present in other
	for name, v := range attributes {
		if !v.present {
			log.Debugf("attribute %v not present in other node", name)
			return false
		}
	}

	return true
}

func (node Node) sameGenericAttributes(other Node) bool {
	return deepEqual(node.GenericAttributes, other.GenericAttributes)
}

func deepEqual(map1, map2 map[GenericAttributeType]json.RawMessage) bool {
	// Check if the maps have the same number of keys
	if len(map1) != len(map2) {
		return false
	}

	// Iterate over each key-value pair in map1
	for key, value1 := range map1 {
		// Check if the key exists in map2
		value2, ok := map2[key]
		if !ok {
			return false
		}

		// Check if the JSON.RawMessage values are equal byte by byte
		if !bytes.Equal(value1, value2) {
			return false
		}
	}

	return true
}
