package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type BlobSaver interface {
	SaveBlob(context.Context, BlobType, []byte, ID, bool) (ID, bool, int, error)
}

type TreeJSONBuilder struct {
	buf      bytes.Buffer
	lastName string
}

func NewTreeJSONBuilder() *TreeJSONBuilder {
	tb := &TreeJSONBuilder{}
	_, _ = tb.buf.WriteString(`{"nodes":[`)
	return tb
}

var ErrTreeNotOrdered = errors.New("nodes are not ordered or duplicate")

func (builder *TreeJSONBuilder) AddNode(node *Node) error {
	if node.Name <= builder.lastName {
		return fmt.Errorf("node %q, last%q: %w", node.Name, builder.lastName, ErrTreeNotOrdered)
	}
	if builder.lastName != "" {
		_ = builder.buf.WriteByte(',')
	}
	builder.lastName = node.Name

	val, err := json.Marshal(node)
	if err != nil {
		return err
	}
	_, _ = builder.buf.Write(val)
	return nil
}

func (builder *TreeJSONBuilder) Finalize() ([]byte, error) {
	// append a newline so that the data is always consistent (json.Encoder
	// adds a newline after each object)
	_, _ = builder.buf.WriteString("]}\n")
	buf := builder.buf.Bytes()
	// drop reference to buffer
	builder.buf = bytes.Buffer{}
	return buf, nil
}

// Tree is an ordered list of nodes.
type Tree struct {
	Nodes []*Node `json:"nodes"`
}

// Find returns a node with the given name, or nil if none could be found.
func (t *Tree) Find(name string) *Node {
	if t == nil {
		return nil
	}

	_, node := t.find(name)
	return node
}

func (t *Tree) find(name string) (int, *Node) {
	pos := sort.Search(len(t.Nodes), func(i int) bool {
		return t.Nodes[i].Name >= name
	})

	if pos < len(t.Nodes) && t.Nodes[pos].Name == name {
		return pos, t.Nodes[pos]
	}

	return pos, nil
}

type BlobLoader interface {
	LoadBlob(context.Context, BlobType, ID, []byte) ([]byte, error)
}

// LoadTree loads a tree from the repository.
func LoadTree(ctx context.Context, r BlobLoader, id ID) (*Tree, error) {
	log.Debugf("load tree %v", id)

	buf, err := r.LoadBlob(ctx, TreeBlob, id, nil)
	if err != nil {
		return nil, err
	}

	t := &Tree{}
	err = json.Unmarshal(buf, t)
	if err != nil {
		return nil, err
	}

	return t, nil
}
