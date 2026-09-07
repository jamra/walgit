package wal

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const (
	blobFormatVersion        = 1
	blobChunkSize            = 8 << 20
	blobUploadConcurrency    = 4
	blobExternalizeThreshold = 1 << 20
	blobMaximumChunkBytes    = 64 << 20
)

type ChunkRef struct {
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

type BlobDescriptor struct {
	Version int        `json:"version"`
	SHA256  string     `json:"sha256"`
	Bytes   int64      `json:"bytes"`
	Chunks  []ChunkRef `json:"chunks"`
}

type ObjectBlob struct {
	Path string         `json:"path"`
	Mode uint32         `json:"mode"`
	Blob BlobDescriptor `json:"blob"`
}

type blobStore interface {
	Put(ChunkRef, []byte) error
	Get(ChunkRef) ([]byte, error)
}

type replicatedBlobStore struct {
	stores []blobStore
}

func (s replicatedBlobStore) Put(ref ChunkRef, data []byte) error {
	if len(s.stores) < 2 {
		return errors.New("replicated blob store requires at least two authorities")
	}
	errs := make(chan error, len(s.stores))
	for _, store := range s.stores {
		store := store
		go func() { errs <- store.Put(ref, data) }()
	}
	var failures []string
	for range s.stores {
		if err := <-errs; err != nil {
			failures = append(failures, err.Error())
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("blob did not reach every durable authority: %s", strings.Join(failures, "; "))
	}
	return nil
}

func (s replicatedBlobStore) Get(ref ChunkRef) ([]byte, error) {
	var failures []string
	for _, store := range s.stores {
		data, err := store.Get(ref)
		if err == nil {
			return data, nil
		}
		failures = append(failures, err.Error())
	}
	return nil, fmt.Errorf("blob unavailable from every authority: %s", strings.Join(failures, "; "))
}

// MemoryBlobStore is a deterministic test backend for chunking, replication,
// deduplication, corruption, and recovery tests.
type MemoryBlobStore struct {
	mu       sync.Mutex
	chunks   map[string][]byte
	putCalls int
}

func NewMemoryBlobStore() *MemoryBlobStore {
	return &MemoryBlobStore{chunks: make(map[string][]byte)}
}

func (s *MemoryBlobStore) Put(ref ChunkRef, data []byte) error {
	if err := validateChunk(ref, data); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.putCalls++
	if existing, ok := s.chunks[ref.SHA256]; ok {
		if !bytes.Equal(existing, data) {
			return fmt.Errorf("content-address collision for chunk %s", ref.SHA256)
		}
		return nil
	}
	s.chunks[ref.SHA256] = append([]byte(nil), data...)
	return nil
}

func (s *MemoryBlobStore) Get(ref ChunkRef) ([]byte, error) {
	if err := validateChunkRef(ref); err != nil {
		return nil, err
	}
	s.mu.Lock()
	data, ok := s.chunks[ref.SHA256]
	data = append([]byte(nil), data...)
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("chunk %s not found", ref.SHA256)
	}
	if err := validateChunk(ref, data); err != nil {
		return nil, err
	}
	return data, nil
}

func (s *MemoryBlobStore) ChunkCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.chunks)
}

func (s *MemoryBlobStore) PutCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.putCalls
}

func (s *MemoryBlobStore) Corrupt(digest string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.chunks[digest]
	if !ok {
		return fmt.Errorf("chunk %s not found", digest)
	}
	if len(data) == 0 {
		return errors.New("cannot corrupt empty chunk")
	}
	data[0] ^= 0xff
	return nil
}

type filesystemBlobStore struct{ store Store }

func (s filesystemBlobStore) Put(ref ChunkRef, data []byte) error {
	if err := validateChunk(ref, data); err != nil {
		return err
	}
	if err := s.store.checkHealthy(); err != nil {
		return err
	}
	path := s.path(ref.SHA256)
	if existing, err := os.ReadFile(path); err == nil {
		return validateChunk(ref, existing)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(directory, ".chunk-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := (&durabilityWriter{store: s.store, writer: tmp, operation: "write blob chunk"}).Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := s.store.syncFile(tmp, "sync blob chunk"); err != nil {
		tmp.Close()
		return err
	}
	if err := s.store.closeFile(tmp, "close blob chunk"); err != nil {
		return err
	}
	if err := s.store.rename(tmpName, path, "publish blob chunk"); err != nil {
		return err
	}
	for current := directory; ; current = filepath.Dir(current) {
		if err := s.store.syncDirectory(current, "sync blob chunk directory"); err != nil {
			return err
		}
		if current == s.store.Root {
			break
		}
	}
	return nil
}

func (s filesystemBlobStore) Get(ref ChunkRef) ([]byte, error) {
	if err := validateChunkRef(ref); err != nil {
		return nil, err
	}
	if err := s.store.checkHealthy(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(s.path(ref.SHA256))
	if err != nil {
		return nil, err
	}
	if err := validateChunk(ref, data); err != nil {
		return nil, err
	}
	return data, nil
}

func (s filesystemBlobStore) path(digest string) string {
	prefix := "invalid"
	if len(digest) >= 2 {
		prefix = digest[:2]
	}
	return filepath.Join(s.store.Root, ".walgit-blobs", "sha256", prefix, digest)
}

func (s Store) blobStorage() blobStore {
	if s.blobs != nil {
		return s.blobs
	}
	return filesystemBlobStore{store: Store{Root: s.Root, faults: s.faults}}
}

var chunkBufferPool = sync.Pool{
	New: func() any {
		buffer := make([]byte, blobChunkSize)
		return &buffer
	},
}

func storeObjectBlobs(objectDir string, storage blobStore) ([]ObjectBlob, error) {
	var paths []string
	if err := filepath.WalkDir(objectDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(objectDir, path)
		if err != nil {
			return err
		}
		if !transientObjectFile(rel) {
			paths = append(paths, path)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Strings(paths)
	objects := make([]ObjectBlob, 0, len(paths))
	for _, path := range paths {
		rel, err := filepath.Rel(objectDir, path)
		if err != nil {
			return nil, err
		}
		descriptor, err := storeFileBlob(path, storage)
		if err != nil {
			return nil, fmt.Errorf("store object %s: %w", rel, err)
		}
		objects = append(objects, ObjectBlob{Path: filepath.ToSlash(rel), Mode: 0o444, Blob: descriptor})
	}
	return objects, nil
}

func maybeStoreObjectBlobs(objectDir string, storage blobStore) ([]ObjectBlob, error) {
	var total int64
	err := filepath.WalkDir(objectDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(objectDir, path)
		if err != nil {
			return err
		}
		if !transientObjectFile(rel) {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			total += info.Size()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if total < blobExternalizeThreshold {
		return nil, nil
	}
	return storeObjectBlobs(objectDir, storage)
}

func storeFileBlob(path string, storage blobStore) (BlobDescriptor, error) {
	file, err := os.Open(path)
	if err != nil {
		return BlobDescriptor{}, err
	}
	defer file.Close()
	wholeHash := sha256.New()
	var refs []ChunkRef
	semaphore := make(chan struct{}, blobUploadConcurrency)
	errs := make(chan error, 1)
	var uploads sync.WaitGroup
	var total int64
	for {
		semaphore <- struct{}{}
		buffer := chunkBufferPool.Get().(*[]byte)
		n, readErr := io.ReadFull(file, *buffer)
		if errors.Is(readErr, io.EOF) && n == 0 {
			chunkBufferPool.Put(buffer)
			<-semaphore
			break
		}
		if readErr != nil && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			chunkBufferPool.Put(buffer)
			<-semaphore
			uploads.Wait()
			return BlobDescriptor{}, readErr
		}
		data := (*buffer)[:n]
		_, _ = wholeHash.Write(data)
		sum := sha256.Sum256(data)
		ref := ChunkRef{SHA256: hex.EncodeToString(sum[:]), Bytes: int64(n)}
		refs = append(refs, ref)
		total += int64(n)
		uploads.Add(1)
		go func() {
			defer uploads.Done()
			defer func() {
				chunkBufferPool.Put(buffer)
				<-semaphore
			}()
			if err := storage.Put(ref, data); err != nil {
				select {
				case errs <- err:
				default:
				}
			}
		}()
		if errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
	}
	uploads.Wait()
	close(errs)
	for err := range errs {
		return BlobDescriptor{}, err
	}
	return BlobDescriptor{
		Version: blobFormatVersion, SHA256: hex.EncodeToString(wholeHash.Sum(nil)),
		Bytes: total, Chunks: refs,
	}, nil
}

func restoreObjectBlobs(objects []ObjectBlob, objectDir string, storage blobStore) error {
	touched := make(map[string]bool)
	for _, object := range objects {
		if object.Blob.Version != blobFormatVersion {
			return fmt.Errorf("unsupported blob descriptor version %d", object.Blob.Version)
		}
		clean := filepath.Clean(filepath.FromSlash(object.Path))
		if clean == "." || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			return fmt.Errorf("unsafe blob object path %q", object.Path)
		}
		directory := filepath.Join(objectDir, filepath.Dir(clean))
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return err
		}
		tmp, err := os.CreateTemp(directory, ".walgit-object-*.tmp")
		if err != nil {
			return err
		}
		tmpName := tmp.Name()
		wholeHash := sha256.New()
		var total int64
		failed := false
		for _, ref := range object.Blob.Chunks {
			data, getErr := storage.Get(ref)
			if getErr != nil {
				err = getErr
				failed = true
				break
			}
			n, writeErr := io.MultiWriter(tmp, wholeHash).Write(data)
			total += int64(n)
			if writeErr != nil || n != len(data) {
				if writeErr == nil {
					writeErr = io.ErrShortWrite
				}
				err = writeErr
				failed = true
				break
			}
		}
		if !failed && total != object.Blob.Bytes {
			err = fmt.Errorf("blob %s has %d bytes, want %d", object.Path, total, object.Blob.Bytes)
			failed = true
		}
		if !failed && hex.EncodeToString(wholeHash.Sum(nil)) != object.Blob.SHA256 {
			err = fmt.Errorf("blob checksum mismatch for %s", object.Path)
			failed = true
		}
		if failed {
			tmp.Close()
			os.Remove(tmpName)
			return err
		}
		if err := tmp.Sync(); err != nil {
			tmp.Close()
			os.Remove(tmpName)
			return err
		}
		mode := os.FileMode(object.Mode)
		if mode == 0 {
			mode = 0o444
		}
		if err := tmp.Chmod(mode); err != nil {
			tmp.Close()
			os.Remove(tmpName)
			return err
		}
		if err := tmp.Close(); err != nil {
			os.Remove(tmpName)
			return err
		}
		if err := os.Rename(tmpName, filepath.Join(objectDir, clean)); err != nil {
			os.Remove(tmpName)
			return err
		}
		touched[directory] = true
	}
	return syncObjectDirectories(objectDir, touched)
}

func validateChunk(ref ChunkRef, data []byte) error {
	if err := validateChunkRef(ref); err != nil {
		return err
	}
	if ref.Bytes != int64(len(data)) {
		return fmt.Errorf("chunk %s has %d bytes, want %d", ref.SHA256, len(data), ref.Bytes)
	}
	sum := sha256.Sum256(data)
	if actual := hex.EncodeToString(sum[:]); actual != ref.SHA256 {
		return fmt.Errorf("chunk checksum mismatch: got %s, want %s", actual, ref.SHA256)
	}
	return nil
}

func validateChunkRef(ref ChunkRef) error {
	if len(ref.SHA256) != sha256.Size*2 || strings.ToLower(ref.SHA256) != ref.SHA256 {
		return fmt.Errorf("invalid chunk SHA-256 %q", ref.SHA256)
	}
	if _, err := hex.DecodeString(ref.SHA256); err != nil {
		return fmt.Errorf("invalid chunk SHA-256 %q: %w", ref.SHA256, err)
	}
	if ref.Bytes < 0 || ref.Bytes > blobMaximumChunkBytes {
		return fmt.Errorf("invalid chunk size %d", ref.Bytes)
	}
	return nil
}

func objectBlobBytes(objects []ObjectBlob) int64 {
	var total int64
	for _, object := range objects {
		total += object.Blob.Bytes
	}
	return total
}

func externalizeEntryMeta(meta EntryMeta, storage blobStore) (EntryMeta, error) {
	data, err := json.Marshal(meta)
	if err != nil {
		return EntryMeta{}, err
	}
	ref := chunkRef(data)
	if err := storage.Put(ref, data); err != nil {
		return EntryMeta{}, fmt.Errorf("store transaction descriptor: %w", err)
	}
	return EntryMeta{
		TransactionID: meta.TransactionID, CreatedAt: meta.CreatedAt,
		Updates: append([]RefUpdate(nil), meta.Updates...), Descriptor: &ref,
	}, nil
}

func resolveEntryMeta(meta EntryMeta, storage blobStore) (EntryMeta, error) {
	if meta.Descriptor == nil {
		return meta, nil
	}
	if storage == nil {
		return EntryMeta{}, errors.New("transaction references a descriptor but no blob store is configured")
	}
	data, err := storage.Get(*meta.Descriptor)
	if err != nil {
		return EntryMeta{}, fmt.Errorf("load transaction descriptor: %w", err)
	}
	var resolved EntryMeta
	if err := json.Unmarshal(data, &resolved); err != nil {
		return EntryMeta{}, fmt.Errorf("decode transaction descriptor: %w", err)
	}
	if resolved.Descriptor != nil {
		return EntryMeta{}, errors.New("transaction descriptor cannot reference another descriptor")
	}
	resolvedID, err := transactionID(resolved.Updates)
	if err != nil {
		return EntryMeta{}, err
	}
	if resolved.TransactionID != meta.TransactionID || resolvedID != meta.TransactionID {
		return EntryMeta{}, fmt.Errorf("transaction descriptor mismatch for %s", meta.TransactionID)
	}
	return resolved, nil
}

func externalizeCheckpointMeta(meta CheckpointMeta, storage blobStore) (CheckpointMeta, error) {
	data, err := json.Marshal(meta)
	if err != nil {
		return CheckpointMeta{}, err
	}
	ref := chunkRef(data)
	if err := storage.Put(ref, data); err != nil {
		return CheckpointMeta{}, fmt.Errorf("store checkpoint descriptor: %w", err)
	}
	return CheckpointMeta{Generation: meta.Generation, Descriptor: &ref}, nil
}

func resolveCheckpointMeta(meta CheckpointMeta, storage blobStore) (CheckpointMeta, error) {
	if meta.Descriptor == nil {
		return meta, nil
	}
	if storage == nil {
		return CheckpointMeta{}, errors.New("checkpoint references a descriptor but no blob store is configured")
	}
	data, err := storage.Get(*meta.Descriptor)
	if err != nil {
		return CheckpointMeta{}, fmt.Errorf("load checkpoint descriptor: %w", err)
	}
	var resolved CheckpointMeta
	if err := json.Unmarshal(data, &resolved); err != nil {
		return CheckpointMeta{}, fmt.Errorf("decode checkpoint descriptor: %w", err)
	}
	if resolved.Descriptor != nil {
		return CheckpointMeta{}, errors.New("checkpoint descriptor cannot reference another descriptor")
	}
	if resolved.Generation != meta.Generation {
		return CheckpointMeta{}, fmt.Errorf("checkpoint descriptor generation mismatch: got %d, want %d", resolved.Generation, meta.Generation)
	}
	return resolved, nil
}

func chunkRef(data []byte) ChunkRef {
	sum := sha256.Sum256(data)
	return ChunkRef{SHA256: hex.EncodeToString(sum[:]), Bytes: int64(len(data))}
}
