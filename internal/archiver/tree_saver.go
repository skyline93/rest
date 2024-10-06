package archiver

import (
	"context"

	log "github.com/sirupsen/logrus"
	"github.com/skyline93/rest/internal/errors"
	"github.com/skyline93/rest/internal/rest"
	"golang.org/x/sync/errgroup"
)

// TreeSaver concurrently saves incoming trees to the repo.
type TreeSaver struct {
	saveBlob SaveBlobFn
	errFn    ErrorFunc

	ch chan<- saveTreeJob
}

// FutureNode holds a reference to a channel that returns a FutureNodeResult
// or a reference to an already existing result. If the result is available
// immediately, then storing a reference directly requires less memory than
// using the indirection via a channel.
type FutureNode struct {
	ch  <-chan futureNodeResult
	res *futureNodeResult
}

type saveTreeJob struct {
	snPath   string
	target   string
	node     *rest.Node
	nodes    []FutureNode
	ch       chan<- futureNodeResult
	complete CompleteFunc
}

func (s *TreeSaver) TriggerShutdown() {
	close(s.ch)
}

// NewTreeSaver returns a new tree saver. A worker pool with treeWorkers is
// started, it is stopped when ctx is cancelled.
func NewTreeSaver(ctx context.Context, wg *errgroup.Group, treeWorkers uint, saveBlob SaveBlobFn, errFn ErrorFunc) *TreeSaver {
	ch := make(chan saveTreeJob)

	s := &TreeSaver{
		ch:       ch,
		saveBlob: saveBlob,
		errFn:    errFn,
	}

	for i := uint(0); i < treeWorkers; i++ {
		wg.Go(func() error {
			return s.worker(ctx, ch)
		})
	}

	return s
}

func (s *TreeSaver) worker(ctx context.Context, jobs <-chan saveTreeJob) error {
	for {
		var job saveTreeJob
		var ok bool
		select {
		case <-ctx.Done():
			return nil
		case job, ok = <-jobs:
			if !ok {
				return nil
			}
		}

		node, stats, err := s.save(ctx, &job)
		if err != nil {
			log.Debugf("error saving tree blob: %v", err)
			close(job.ch)
			return err
		}

		if job.complete != nil {
			job.complete(node, stats)
		}
		job.ch <- futureNodeResult{
			snPath: job.snPath,
			target: job.target,
			node:   node,
			stats:  stats,
		}
		close(job.ch)
	}
}

// save stores the nodes as a tree in the repo.
func (s *TreeSaver) save(ctx context.Context, job *saveTreeJob) (*rest.Node, ItemStats, error) {
	var stats ItemStats
	node := job.node
	nodes := job.nodes
	// allow GC of nodes array once the loop is finished
	job.nodes = nil

	builder := rest.NewTreeJSONBuilder()
	var lastNode *rest.Node

	for i, fn := range nodes {
		// fn is a copy, so clear the original value explicitly
		nodes[i] = FutureNode{}
		fnr := fn.take(ctx)

		// return the error if it wasn't ignored
		if fnr.err != nil {
			log.Debugf("err for %v: %v", fnr.snPath, fnr.err)
			if fnr.err == context.Canceled {
				return nil, stats, fnr.err
			}

			fnr.err = s.errFn(fnr.target, fnr.err)
			if fnr.err == nil {
				// ignore error
				continue
			}

			return nil, stats, fnr.err
		}

		// when the error is ignored, the node could not be saved, so ignore it
		if fnr.node == nil {
			log.Debugf("%v excluded: %v", fnr.snPath, fnr.target)
			continue
		}

		err := builder.AddNode(fnr.node)
		if err != nil && errors.Is(err, rest.ErrTreeNotOrdered) && lastNode != nil && fnr.node.Equals(*lastNode) {
			log.Debugf("insert %v failed: %v", fnr.node.Name, err)
			// ignore error if an _identical_ node already exists, but nevertheless issue a warning
			_ = s.errFn(fnr.target, err)
			err = nil
		}
		if err != nil {
			log.Debugf("insert %v failed: %v", fnr.node.Name, err)
			return nil, stats, err
		}
		lastNode = fnr.node
	}

	buf, err := builder.Finalize()
	if err != nil {
		return nil, stats, err
	}

	b := &Buffer{Data: buf}
	ch := make(chan SaveBlobResponse, 1)
	s.saveBlob(ctx, rest.TreeBlob, b, job.target, func(res SaveBlobResponse) {
		ch <- res
	})

	select {
	case sbr := <-ch:
		if !sbr.known {
			stats.TreeBlobs++
			stats.TreeSize += uint64(sbr.length)
			stats.TreeSizeInRepo += uint64(sbr.sizeInRepo)
		}

		node.Subtree = &sbr.id
		return node, stats, nil
	case <-ctx.Done():
		return nil, stats, ctx.Err()
	}
}

func newFutureNode() (FutureNode, chan<- futureNodeResult) {
	ch := make(chan futureNodeResult, 1)
	return FutureNode{ch: ch}, ch
}

// Save stores the dir d and returns the data once it has been completed.
func (s *TreeSaver) Save(ctx context.Context, snPath string, target string, node *rest.Node, nodes []FutureNode, complete CompleteFunc) FutureNode {
	fn, ch := newFutureNode()
	job := saveTreeJob{
		snPath:   snPath,
		target:   target,
		node:     node,
		nodes:    nodes,
		ch:       ch,
		complete: complete,
	}
	select {
	case s.ch <- job:
	case <-ctx.Done():
		log.Debugf("not saving tree, context is cancelled")
		close(ch)
	}

	return fn
}
