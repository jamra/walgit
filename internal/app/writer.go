package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"walgit/internal/wal"
)

const writerSocketEnvironment = "WALGIT_WRITER_SOCKET"

type WriterStats struct {
	CommitRequests uint64  `json:"commit_requests"`
	CommitBatches  uint64  `json:"commit_batches"`
	MaximumBatch   uint64  `json:"maximum_batch"`
	MeanBatch      float64 `json:"mean_batch"`
}

type writerCounters struct {
	requests atomic.Uint64
	batches  atomic.Uint64
	maximum  atomic.Uint64
}

type WriterHandle struct {
	socket     string
	listener   net.Listener
	cancel     context.CancelFunc
	acceptDone chan struct{}
	handlers   sync.WaitGroup
	loopDone   chan struct{}
	counters   *writerCounters
	closeOnce  sync.Once
	closeErr   error
}

type writerCoordinator struct {
	repositoryID string
	backend      wal.Backend
	batchWindow  time.Duration
	maximumBatch int
	commits      chan *writerCommitRequest
	operationMu  sync.Mutex
	counters     *writerCounters
}

type writerRequest struct {
	Operation string          `json:"operation"`
	ObjectDir string          `json:"object_dir,omitempty"`
	Updates   []wal.RefUpdate `json:"updates,omitempty"`
}

type writerResponse struct {
	Manifest wal.Manifest `json:"manifest,omitempty"`
	Error    string       `json:"error,omitempty"`
}

type writerCommitRequest struct {
	updates []wal.RefUpdate
	done    chan writerResponse
}

func StartWriter(socket, store, repositoryID string, batchWindow time.Duration, maximumBatch int) (*WriterHandle, error) {
	if socket == "" || store == "" || repositoryID == "" {
		return nil, errors.New("writer requires socket, store, and repository ID")
	}
	if !filepath.IsAbs(socket) {
		return nil, errors.New("writer socket path must be absolute")
	}
	if batchWindow < 0 {
		return nil, errors.New("writer batch window cannot be negative")
	}
	if maximumBatch < 1 || maximumBatch > 1024 {
		return nil, errors.New("writer maximum batch must be between 1 and 1024")
	}
	if _, err := os.Lstat(socket); err == nil {
		return nil, fmt.Errorf("writer socket already exists: %s", socket)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	backend, err := wal.Open(store)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(socket, 0o600); err != nil {
		listener.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	counters := &writerCounters{}
	coordinator := &writerCoordinator{
		repositoryID: repositoryID, backend: backend, batchWindow: batchWindow,
		maximumBatch: maximumBatch, commits: make(chan *writerCommitRequest, maximumBatch*2), counters: counters,
	}
	handle := &WriterHandle{
		socket: socket, listener: listener, cancel: cancel, acceptDone: make(chan struct{}),
		loopDone: make(chan struct{}), counters: counters,
	}
	go func() {
		defer close(handle.loopDone)
		coordinator.commitLoop(ctx)
	}()
	go func() {
		defer close(handle.acceptDone)
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			handle.handlers.Add(1)
			go func() {
				defer handle.handlers.Done()
				coordinator.handle(ctx, connection)
			}()
		}
	}()
	return handle, nil
}

func ServeWriter(socket, store, repositoryID string, batchWindow time.Duration, maximumBatch int) error {
	handle, err := StartWriter(socket, store, repositoryID, batchWindow, maximumBatch)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	return handle.Close()
}

func (h *WriterHandle) Close() error {
	h.closeOnce.Do(func() {
		h.closeErr = h.listener.Close()
		<-h.acceptDone
		h.handlers.Wait()
		h.cancel()
		<-h.loopDone
		if err := os.Remove(h.socket); err != nil && !errors.Is(err, os.ErrNotExist) && h.closeErr == nil {
			h.closeErr = err
		}
		if errors.Is(h.closeErr, net.ErrClosed) {
			h.closeErr = nil
		}
	})
	return h.closeErr
}

func (h *WriterHandle) Stats() WriterStats {
	requests := h.counters.requests.Load()
	batches := h.counters.batches.Load()
	stats := WriterStats{CommitRequests: requests, CommitBatches: batches, MaximumBatch: h.counters.maximum.Load()}
	if batches > 0 {
		stats.MeanBatch = float64(requests) / float64(batches)
	}
	return stats
}

func (c *writerCoordinator) handle(ctx context.Context, connection net.Conn) {
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(2 * time.Minute))
	var request writerRequest
	if err := json.NewDecoder(io.LimitReader(connection, 1<<20)).Decode(&request); err != nil {
		_ = json.NewEncoder(connection).Encode(writerResponse{Error: "decode request: " + err.Error()})
		return
	}
	var response writerResponse
	switch request.Operation {
	case "stage":
		if !filepath.IsAbs(request.ObjectDir) {
			response.Error = "stage object directory must be absolute"
			break
		}
		if err := c.backend.Stage(c.repositoryID, request.ObjectDir, request.Updates); err != nil {
			response.Error = err.Error()
		}
	case "commit":
		commit := &writerCommitRequest{updates: request.Updates, done: make(chan writerResponse, 1)}
		select {
		case c.commits <- commit:
			select {
			case response = <-commit.done:
			case <-ctx.Done():
				response.Error = "writer is shutting down"
			}
		case <-ctx.Done():
			response.Error = "writer is shutting down"
		}
	case "finalize":
		response = c.mutateAndLoad(func() error { return c.backend.Finalize(c.repositoryID, request.Updates) })
	case "abort":
		response = c.mutateAndLoad(func() error { return c.backend.Abort(c.repositoryID, request.Updates) })
	case "load":
		c.operationMu.Lock()
		response.Manifest, response.Error = loadWriterManifest(c.backend, c.repositoryID)
		c.operationMu.Unlock()
	default:
		response.Error = fmt.Sprintf("unknown writer operation %q", request.Operation)
	}
	_ = json.NewEncoder(connection).Encode(response)
}

func (c *writerCoordinator) mutateAndLoad(operation func() error) writerResponse {
	c.operationMu.Lock()
	defer c.operationMu.Unlock()
	if err := operation(); err != nil {
		return writerResponse{Error: err.Error()}
	}
	manifest, message := loadWriterManifest(c.backend, c.repositoryID)
	return writerResponse{Manifest: manifest, Error: message}
}

func loadWriterManifest(backend wal.Backend, repositoryID string) (wal.Manifest, string) {
	manifest, err := backend.Load(repositoryID)
	if err != nil {
		return wal.Manifest{}, err.Error()
	}
	return manifest, ""
}

func (c *writerCoordinator) commitLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case first := <-c.commits:
			batch := []*writerCommitRequest{first}
			timer := time.NewTimer(c.batchWindow)
		collect:
			for len(batch) < c.maximumBatch {
				select {
				case request := <-c.commits:
					batch = append(batch, request)
				case <-timer.C:
					break collect
				case <-ctx.Done():
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					for _, request := range batch {
						request.done <- writerResponse{Error: "writer is shutting down"}
					}
					return
				}
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			c.commit(batch)
		}
	}
}

func (c *writerCoordinator) commit(batch []*writerCommitRequest) {
	c.counters.requests.Add(uint64(len(batch)))
	c.counters.batches.Add(1)
	for {
		maximum := c.counters.maximum.Load()
		if uint64(len(batch)) <= maximum || c.counters.maximum.CompareAndSwap(maximum, uint64(len(batch))) {
			break
		}
	}
	updates := make([][]wal.RefUpdate, len(batch))
	for i, request := range batch {
		updates[i] = request.updates
	}
	c.operationMu.Lock()
	var manifest wal.Manifest
	var err error
	if batchBackend, ok := c.backend.(wal.BatchCommitter); ok {
		_, manifest, err = batchBackend.CommitBatch(c.repositoryID, updates)
	} else {
		for _, requestUpdates := range updates {
			_, manifest, err = c.backend.Commit(c.repositoryID, requestUpdates)
			if err != nil {
				break
			}
		}
	}
	c.operationMu.Unlock()
	response := writerResponse{Manifest: manifest}
	if err != nil {
		response.Error = err.Error()
	}
	for _, request := range batch {
		request.done <- response
	}
}

func callWriter(socket string, request writerRequest) (writerResponse, error) {
	connection, err := net.DialTimeout("unix", socket, 5*time.Second)
	if err != nil {
		return writerResponse{}, err
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(2 * time.Minute))
	if err := json.NewEncoder(connection).Encode(request); err != nil {
		return writerResponse{}, err
	}
	var response writerResponse
	if err := json.NewDecoder(io.LimitReader(connection, 16<<20)).Decode(&response); err != nil {
		return writerResponse{}, err
	}
	if response.Error != "" {
		return writerResponse{}, errors.New(response.Error)
	}
	return response, nil
}
