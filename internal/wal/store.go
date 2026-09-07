package wal

import (
	"archive/tar"
	"bufio"
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
	"syscall"
	"time"
)

const manifestVersion = 2

type RefUpdate struct {
	Old string `json:"old"`
	New string `json:"new"`
	Ref string `json:"ref"`
}

type EntryMeta struct {
	TransactionID string       `json:"transaction_id"`
	CreatedAt     time.Time    `json:"created_at"`
	Updates       []RefUpdate  `json:"updates"`
	Objects       []ObjectBlob `json:"objects,omitempty"`
	Descriptor    *ChunkRef    `json:"descriptor,omitempty"`
}

type ManifestEntry struct {
	Generation    uint64    `json:"generation"`
	TransactionID string    `json:"transaction_id"`
	File          string    `json:"file"`
	SHA256        string    `json:"sha256"`
	Bytes         int64     `json:"bytes"`
	PayloadBytes  int64     `json:"payload_bytes,omitempty"`
	Descriptor    *ChunkRef `json:"descriptor,omitempty"`
}

type Checkpoint struct {
	Generation   uint64 `json:"generation"`
	File         string `json:"file"`
	SHA256       string `json:"sha256"`
	Bytes        int64  `json:"bytes"`
	PayloadBytes int64  `json:"payload_bytes,omitempty"`
}

type CheckpointMeta struct {
	Generation   uint64            `json:"generation"`
	CreatedAt    time.Time         `json:"created_at"`
	Head         string            `json:"head"`
	ObjectFormat string            `json:"object_format"`
	Refs         map[string]string `json:"refs"`
	Objects      []ObjectBlob      `json:"objects,omitempty"`
	Descriptor   *ChunkRef         `json:"descriptor,omitempty"`
}

type PreparedTransaction struct {
	TransactionID string      `json:"transaction_id"`
	Generation    uint64      `json:"generation"`
	CreatedAt     time.Time   `json:"created_at"`
	Updates       []RefUpdate `json:"updates"`
}

type Manifest struct {
	Version           int                            `json:"version"`
	Generation        uint64                         `json:"generation"`
	Head              string                         `json:"head"`
	ObjectFormat      string                         `json:"object_format"`
	Refs              map[string]string              `json:"refs"`
	Entries           []ManifestEntry                `json:"entries"`
	Checkpoint        *Checkpoint                    `json:"checkpoint,omitempty"`
	Prepared          map[string]PreparedTransaction `json:"prepared,omitempty"`
	CertificateSHA256 string                         `json:"certificate_sha256,omitempty"`
	CertificateFloor  uint64                         `json:"certificate_floor,omitempty"`
}

type Store struct {
	Root                    string
	faults                  *storeFaults
	blobs                   blobStore
	certificates            certificateStore
	externalizeTransactions bool
}

type storeFaults struct {
	syncFile func(*os.File) error
	syncDir  func(string) error
}

var poisonedStores sync.Map

var archiveBufferPool = sync.Pool{
	New: func() any {
		buffer := make([]byte, 256<<10)
		return &buffer
	},
}

type GCResult struct {
	Pending     int `json:"pending"`
	Entries     int `json:"entries"`
	Checkpoints int `json:"checkpoints"`
}

func cloneManifest(m Manifest) Manifest {
	cloned := m
	cloned.Refs = cloneRefs(m.Refs)
	cloned.Entries = append([]ManifestEntry(nil), m.Entries...)
	if m.Checkpoint != nil {
		checkpoint := *m.Checkpoint
		cloned.Checkpoint = &checkpoint
	}
	if m.Prepared != nil {
		cloned.Prepared = make(map[string]PreparedTransaction, len(m.Prepared))
		for id, prepared := range m.Prepared {
			prepared.Updates = append([]RefUpdate(nil), prepared.Updates...)
			cloned.Prepared[id] = prepared
		}
	}
	return cloned
}

func (s Store) Initialize(repoID, head, objectFormat string) error {
	if err := s.checkHealthy(); err != nil {
		return err
	}
	if err := validateID(repoID); err != nil {
		return err
	}
	if head == "" {
		head = "refs/heads/main"
	}
	if objectFormat != "sha1" && objectFormat != "sha256" {
		return fmt.Errorf("unsupported object format %q", objectFormat)
	}
	dir := filepath.Join(s.Root, repoID)
	if err := os.MkdirAll(filepath.Join(dir, "pending"), 0o755); err != nil {
		return err
	}
	return s.withLock(repoID, func() error {
		m, err := readManifest(dir)
		if err == nil {
			if m.Head != head || m.ObjectFormat != objectFormat {
				return errors.New("existing manifest configuration does not match repository")
			}
			anchored, err := publishAnchor(s.certificates, repoID, m)
			if err != nil {
				return err
			}
			if anchored.CertificateSHA256 == m.CertificateSHA256 {
				return nil
			}
			if err := s.writeManifest(dir, anchored); err != nil {
				return err
			}
			return s.syncDirectory(dir, "initialize certificate anchor")
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		m = Manifest{Version: manifestVersion, Head: head, ObjectFormat: objectFormat, Refs: map[string]string{}}
		m, err = publishAnchor(s.certificates, repoID, m)
		if err != nil {
			return err
		}
		if err := s.writeManifest(dir, m); err != nil {
			return err
		}
		return s.syncDirectory(dir, "initialize manifest directory")
	})
}

func (s Store) Stage(repoID, objectDir string, updates []RefUpdate) error {
	if err := s.checkHealthy(); err != nil {
		return err
	}
	if err := validateID(repoID); err != nil {
		return err
	}
	txID, err := transactionID(updates)
	if err != nil {
		return err
	}
	pending := filepath.Join(s.Root, repoID, "pending")
	if err := os.MkdirAll(pending, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(pending, ".stage-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	var objects []ObjectBlob
	if s.certificates != nil || s.externalizeTransactions {
		objects, err = storeObjectBlobs(objectDir, s.blobStorage())
	}
	if err != nil {
		return err
	}
	meta := EntryMeta{TransactionID: txID, CreatedAt: time.Now().UTC(), Updates: updates, Objects: objects}
	if len(objects) > 0 || s.certificates != nil {
		meta, err = externalizeEntryMeta(meta, s.blobStorage())
		if err != nil {
			return err
		}
	}
	if err := writeArchive(&durabilityWriter{store: s, writer: tmp, operation: "write staged WAL"}, meta, objectDir); err != nil {
		tmp.Close()
		return err
	}
	if err := s.syncFile(tmp, "sync staged WAL"); err != nil {
		tmp.Close()
		return err
	}
	if err := s.closeFile(tmp, "close staged WAL"); err != nil {
		return err
	}
	if err := failpoint("stage.after_sync"); err != nil {
		return err
	}
	if err := s.rename(tmpName, filepath.Join(pending, txID+".wal"), "publish staged WAL"); err != nil {
		return err
	}
	return s.syncDirectory(pending, "sync staged WAL directory")
}

func (s Store) Commit(repoID string, updates []RefUpdate) (ManifestEntry, Manifest, error) {
	if err := validateID(repoID); err != nil {
		return ManifestEntry{}, Manifest{}, err
	}
	txID, err := transactionID(updates)
	if err != nil {
		return ManifestEntry{}, Manifest{}, err
	}
	var committed ManifestEntry
	var result Manifest
	err = s.withLock(repoID, func() error {
		dir := filepath.Join(s.Root, repoID)
		m, err := s.authoritativeManifest(repoID)
		if err != nil {
			return err
		}
		previous := cloneManifest(m)
		if updatesAlreadyApplied(m, updates) {
			committed = ManifestEntry{Generation: m.Generation, TransactionID: txID}
			for i := len(m.Entries) - 1; i >= 0; i-- {
				if m.Entries[i].TransactionID == txID {
					committed = m.Entries[i]
					break
				}
			}
			result = m
			_ = os.Remove(filepath.Join(dir, "pending", txID+".wal"))
			return nil
		}
		if s.certificates != nil {
			if err := validatePreparedLocks(m, txID, updates); err != nil {
				return err
			}
		}
		if err := validateUpdates(m, updates); err != nil {
			return err
		}
		generation := m.Generation + 1
		name := fmt.Sprintf("%020d-%s.wal", generation, txID[:16])
		finalPath := filepath.Join(dir, name)
		pendingPath := filepath.Join(dir, "pending", txID+".wal")
		if _, err := os.Stat(finalPath); errors.Is(err, os.ErrNotExist) {
			if err := s.rename(pendingPath, finalPath, "publish committed WAL"); err != nil {
				return fmt.Errorf("publish staged transaction: %w", err)
			}
		} else if err != nil {
			return err
		}
		if err := failpoint("commit.after_entry_rename"); err != nil {
			return err
		}
		digest, size, err := fileDigest(finalPath)
		if err != nil {
			return err
		}
		payloadBytes, descriptor, err := archiveEntryDetails(finalPath, s.blobStorage())
		if err != nil {
			return err
		}
		entry := ManifestEntry{
			Generation: generation, TransactionID: txID, File: name,
			SHA256: digest, Bytes: size, PayloadBytes: payloadBytes, Descriptor: descriptor,
		}
		for _, u := range updates {
			if isZeroOID(u.New) {
				delete(m.Refs, u.Ref)
			} else {
				m.Refs[u.Ref] = u.New
			}
		}
		m.Generation = generation
		m.Entries = append(m.Entries, entry)
		if s.certificates != nil {
			if m.Prepared == nil {
				m.Prepared = make(map[string]PreparedTransaction)
			}
			m.Prepared[txID] = PreparedTransaction{TransactionID: txID, Generation: generation, CreatedAt: time.Now().UTC(), Updates: updates}
		} else {
			// Version 2 manifests may contain records from the superseded
			// two-publication protocol. A new authoritative publication also
			// completes those transactions and removes their stale locks.
			m.Prepared = nil
		}
		m, err = publishTransition(s.certificates, repoID, previous, m, []CertificateEntry{{Entry: entry, Updates: updates}})
		if err != nil {
			return err
		}
		if err := s.writeManifest(dir, m); err != nil {
			return err
		}
		if err := s.syncDirectory(dir, "sync committed WAL and manifest directory"); err != nil {
			return err
		}
		committed, result = entry, m
		return failpoint("commit.after_manifest")
	})
	return committed, result, err
}

func (s Store) Finalize(repoID string, updates []RefUpdate) error {
	if s.certificates == nil {
		// The atomic manifest publication is the only durable commit point.
		// Git's committed notification only advances the disposable cache.
		return nil
	}
	txID, err := transactionID(updates)
	if err != nil {
		return err
	}
	return s.withLock(repoID, func() error {
		dir := filepath.Join(s.Root, repoID)
		m, err := s.authoritativeManifest(repoID)
		if err != nil {
			return err
		}
		if _, ok := m.Prepared[txID]; !ok {
			return nil
		}
		if err := failpoint("finalize.before_manifest"); err != nil {
			return err
		}
		delete(m.Prepared, txID)
		if err := s.writeManifest(dir, m); err != nil {
			return err
		}
		if err := s.syncDirectory(dir, "sync finalized manifest directory"); err != nil {
			return err
		}
		return failpoint("finalize.after_manifest")
	})
}

func (s Store) Abort(repoID string, updates []RefUpdate) error {
	txID, err := transactionID(updates)
	if err != nil {
		return err
	}
	if s.certificates == nil {
		// Once the index is published, it remains authoritative even if the
		// local cache transaction aborts. A retry/reconcile repairs that cache.
		_ = os.Remove(filepath.Join(s.Root, repoID, "pending", txID+".wal"))
		return nil
	}
	return s.withLock(repoID, func() error {
		dir := filepath.Join(s.Root, repoID)
		defer os.Remove(filepath.Join(dir, "pending", txID+".wal")) //nolint:errcheck
		m, err := s.authoritativeManifest(repoID)
		if err != nil {
			return err
		}
		_, prepared := m.Prepared[txID]
		if !prepared {
			if s.certificates == nil {
				return nil
			}
			found := false
			for _, entry := range m.Entries {
				if entry.TransactionID == txID {
					found = true
					break
				}
			}
			if !found {
				return nil
			}
		}
		before, after := transactionState(m, updates)
		if before {
			if prepared {
				delete(m.Prepared, txID)
				if err := s.writeManifest(dir, m); err != nil {
					return err
				}
				return s.syncDirectory(dir, "sync aborted manifest directory")
			}
			return nil
		}
		if !after {
			return fmt.Errorf("cannot roll back prepared transaction %s because a touched ref advanced", txID)
		}
		previous := cloneManifest(m)
		inverse := invertUpdates(updates)
		generation := m.Generation + 1
		rollbackID := "rollback-" + txID
		name := fmt.Sprintf("%020d-%s-rollback.wal", generation, txID[:16])
		finalPath := filepath.Join(dir, name)
		if _, err := os.Stat(finalPath); errors.Is(err, os.ErrNotExist) {
			tmp, err := os.CreateTemp(dir, ".rollback-*.tmp")
			if err != nil {
				return err
			}
			tmpName := tmp.Name()
			defer os.Remove(tmpName)
			meta := EntryMeta{TransactionID: rollbackID, CreatedAt: time.Now().UTC(), Updates: inverse}
			if s.certificates != nil {
				meta, err = externalizeEntryMeta(meta, s.blobStorage())
				if err != nil {
					tmp.Close()
					return err
				}
			}
			if err := writeMetadataArchive(&durabilityWriter{store: s, writer: tmp, operation: "write rollback WAL"}, meta); err != nil {
				tmp.Close()
				return err
			}
			if err := s.syncFile(tmp, "sync rollback WAL"); err != nil {
				tmp.Close()
				return err
			}
			if err := s.closeFile(tmp, "close rollback WAL"); err != nil {
				return err
			}
			if err := s.rename(tmpName, finalPath, "publish rollback WAL"); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		if err := failpoint("rollback.after_entry_rename"); err != nil {
			return err
		}
		digest, size, err := fileDigest(finalPath)
		if err != nil {
			return err
		}
		_, descriptor, err := archiveEntryDetails(finalPath, s.blobStorage())
		if err != nil {
			return err
		}
		for _, update := range inverse {
			if isZeroOID(update.New) {
				delete(m.Refs, update.Ref)
			} else {
				m.Refs[update.Ref] = update.New
			}
		}
		m.Generation = generation
		entry := ManifestEntry{
			Generation: generation, TransactionID: rollbackID, File: name, SHA256: digest, Bytes: size,
			Descriptor: descriptor,
		}
		m.Entries = append(m.Entries, entry)
		delete(m.Prepared, txID)
		m, err = publishTransition(s.certificates, repoID, previous, m, []CertificateEntry{{Entry: entry, Updates: inverse}})
		if err != nil {
			return err
		}
		if err := s.writeManifest(dir, m); err != nil {
			return err
		}
		if err := s.syncDirectory(dir, "sync rollback WAL and manifest directory"); err != nil {
			return err
		}
		return failpoint("rollback.after_manifest")
	})
}

func (s Store) Load(repoID string) (Manifest, error) {
	if err := s.checkHealthy(); err != nil {
		return Manifest{}, err
	}
	if err := validateID(repoID); err != nil {
		return Manifest{}, err
	}
	return s.authoritativeManifest(repoID)
}

func (s Store) authoritativeManifest(repoID string) (Manifest, error) {
	primary, primaryErr := readManifest(filepath.Join(s.Root, repoID))
	if s.certificates == nil {
		return primary, primaryErr
	}
	recovered, _, certificateErr := s.certificates.Recover(repoID)
	if primaryErr != nil {
		if certificateErr != nil {
			return Manifest{}, fmt.Errorf("primary manifest unavailable and certificate recovery failed: %w", certificateErr)
		}
		return recovered, nil
	}
	if certificateErr != nil {
		return Manifest{}, fmt.Errorf("validate dual-authority certificate chain: %w", certificateErr)
	}
	if recovered.Generation > primary.Generation {
		return recovered, nil
	}
	if recovered.Generation < primary.Generation {
		return Manifest{}, errors.New("primary manifest is ahead of the dual-authority certificate chain")
	}
	if primary.CertificateSHA256 != recovered.CertificateSHA256 {
		return Manifest{}, errors.New("primary manifest and dual-authority certificate chain disagree")
	}
	return primary, nil
}

func (s Store) ReplayFrom(repoID string, generation uint64, gitObjects string, apply func(ManifestEntry, EntryMeta) error) (Manifest, error) {
	m, err := s.Load(repoID)
	if err != nil {
		return Manifest{}, err
	}
	if generation > m.Generation {
		return Manifest{}, fmt.Errorf("local generation %d is ahead of manifest generation %d", generation, m.Generation)
	}
	if generation < m.CertificateFloor {
		return Manifest{}, fmt.Errorf("local generation %d predates recoverable certificate floor %d", generation, m.CertificateFloor)
	}
	for _, entry := range m.Entries {
		if entry.Generation <= generation {
			continue
		}
		if entry.Descriptor != nil {
			meta, err := replayEntryDescriptor(entry, gitObjects, s.blobStorage())
			if err != nil {
				return Manifest{}, fmt.Errorf("replay descriptor for generation %d: %w", entry.Generation, err)
			}
			if err := apply(entry, meta); err != nil {
				return Manifest{}, fmt.Errorf("apply %s: %w", entry.File, err)
			}
			continue
		}
		path := filepath.Join(s.Root, repoID, entry.File)
		f, err := os.Open(path)
		if err != nil {
			return Manifest{}, err
		}
		hash := sha256.New()
		meta, replayErr := readArchive(io.TeeReader(f, hash), gitObjects, s.blobStorage())
		closeErr := f.Close()
		if replayErr != nil {
			return Manifest{}, fmt.Errorf("replay %s: %w", entry.File, replayErr)
		}
		if closeErr != nil {
			return Manifest{}, closeErr
		}
		if got := hex.EncodeToString(hash.Sum(nil)); got != entry.SHA256 {
			return Manifest{}, fmt.Errorf("checksum mismatch for %s", entry.File)
		}
		if meta.TransactionID != entry.TransactionID {
			return Manifest{}, fmt.Errorf("transaction mismatch for %s", entry.File)
		}
		if err := apply(entry, meta); err != nil {
			return Manifest{}, fmt.Errorf("apply %s: %w", entry.File, err)
		}
	}
	return m, nil
}

func (s Store) CreateCheckpoint(repoID, gitObjects string, expectedGeneration uint64) (Checkpoint, error) {
	if err := s.checkHealthy(); err != nil {
		return Checkpoint{}, err
	}
	m, err := s.Load(repoID)
	if err != nil {
		return Checkpoint{}, err
	}
	if m.Generation != expectedGeneration {
		return Checkpoint{}, fmt.Errorf("manifest advanced during checkpoint: expected generation %d, found %d", expectedGeneration, m.Generation)
	}
	if len(m.Prepared) > 0 {
		return Checkpoint{}, errors.New("cannot checkpoint while reference transactions are prepared")
	}
	dir := filepath.Join(s.Root, repoID)
	checkpointDir := filepath.Join(dir, "checkpoints")
	if err := os.MkdirAll(checkpointDir, 0o755); err != nil {
		return Checkpoint{}, err
	}
	tmp, err := os.CreateTemp(checkpointDir, ".checkpoint-*.tmp")
	if err != nil {
		return Checkpoint{}, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	objects, err := maybeStoreObjectBlobs(gitObjects, s.blobStorage())
	if err != nil {
		return Checkpoint{}, err
	}
	meta := CheckpointMeta{
		Generation: m.Generation, CreatedAt: time.Now().UTC(), Head: m.Head,
		ObjectFormat: m.ObjectFormat, Refs: cloneRefs(m.Refs), Objects: objects,
	}
	if len(objects) > 0 {
		meta, err = externalizeCheckpointMeta(meta, s.blobStorage())
		if err != nil {
			return Checkpoint{}, err
		}
	}
	if err := writeCheckpointArchive(&durabilityWriter{store: s, writer: tmp, operation: "write checkpoint"}, meta, gitObjects); err != nil {
		tmp.Close()
		return Checkpoint{}, err
	}
	if err := s.syncFile(tmp, "sync checkpoint"); err != nil {
		tmp.Close()
		return Checkpoint{}, err
	}
	if err := s.closeFile(tmp, "close checkpoint"); err != nil {
		return Checkpoint{}, err
	}
	digest, size, err := fileDigest(tmpName)
	if err != nil {
		return Checkpoint{}, err
	}
	name := fmt.Sprintf("%020d-%s.checkpoint", m.Generation, digest[:16])
	finalPath := filepath.Join(checkpointDir, name)
	if err := s.rename(tmpName, finalPath, "publish checkpoint"); err != nil {
		return Checkpoint{}, err
	}
	// The checkpoint's directory entry must reach stable storage before a
	// durable manifest is allowed to reference it.
	if err := s.syncDirectory(checkpointDir, "sync checkpoint directory"); err != nil {
		return Checkpoint{}, err
	}
	checkpoint := Checkpoint{
		Generation: m.Generation, File: filepath.ToSlash(filepath.Join("checkpoints", name)),
		SHA256: digest, Bytes: size, PayloadBytes: objectBlobBytes(objects),
	}
	err = s.withLock(repoID, func() error {
		latest, err := s.authoritativeManifest(repoID)
		if err != nil {
			return err
		}
		if latest.Checkpoint != nil && latest.Checkpoint.Generation >= checkpoint.Generation {
			checkpoint = *latest.Checkpoint
		} else {
			latest.Checkpoint = &checkpoint
		}
		kept := latest.Entries[:0]
		for _, entry := range latest.Entries {
			if entry.Generation > checkpoint.Generation {
				kept = append(kept, entry)
			}
		}
		latest.Entries = kept
		if err := s.writeManifest(dir, latest); err != nil {
			return err
		}
		return s.syncDirectory(dir, "sync checkpoint manifest directory")
	})
	return checkpoint, err
}

func (s Store) RestoreCheckpoint(repoID, gitObjects string) (CheckpointMeta, error) {
	m, err := s.Load(repoID)
	if err != nil {
		return CheckpointMeta{}, err
	}
	if m.Checkpoint == nil {
		return CheckpointMeta{}, errors.New("manifest has no checkpoint")
	}
	path := filepath.Join(s.Root, repoID, filepath.FromSlash(m.Checkpoint.File))
	f, err := os.Open(path)
	if err != nil {
		return CheckpointMeta{}, err
	}
	hash := sha256.New()
	meta, restoreErr := readCheckpointArchive(io.TeeReader(f, hash), gitObjects, s.blobStorage())
	closeErr := f.Close()
	if restoreErr != nil {
		return CheckpointMeta{}, restoreErr
	}
	if closeErr != nil {
		return CheckpointMeta{}, closeErr
	}
	if got := hex.EncodeToString(hash.Sum(nil)); got != m.Checkpoint.SHA256 {
		return CheckpointMeta{}, fmt.Errorf("checksum mismatch for checkpoint %s", m.Checkpoint.File)
	}
	if meta.Generation != m.Checkpoint.Generation {
		return CheckpointMeta{}, fmt.Errorf("checkpoint generation mismatch: metadata %d, manifest %d", meta.Generation, m.Checkpoint.Generation)
	}
	return meta, nil
}

func (s Store) CleanupPending(repoID string, olderThan time.Time) (int, error) {
	dir := filepath.Join(s.Root, repoID, "pending")
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return removed, err
		}
		if !info.Mode().IsRegular() || !info.ModTime().Before(olderThan) {
			continue
		}
		if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return removed, err
		}
		removed++
	}
	return removed, nil
}

func (s Store) GarbageCollect(repoID string, olderThan time.Time) (GCResult, error) {
	var result GCResult
	err := s.withLock(repoID, func() error {
		dir := filepath.Join(s.Root, repoID)
		m, err := s.authoritativeManifest(repoID)
		if err != nil {
			return err
		}
		referenced := make(map[string]bool, len(m.Entries)+1)
		for _, entry := range m.Entries {
			referenced[filepath.Clean(entry.File)] = true
		}
		if m.Checkpoint != nil {
			referenced[filepath.Clean(filepath.FromSlash(m.Checkpoint.File))] = true
		}
		for _, scan := range []struct {
			path   string
			kind   string
			suffix string
		}{{dir, "entry", ".wal"}, {filepath.Join(dir, "checkpoints"), "checkpoint", ".checkpoint"}} {
			entries, err := os.ReadDir(scan.path)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return err
			}
			for _, entry := range entries {
				if entry.IsDir() || !strings.HasSuffix(entry.Name(), scan.suffix) {
					continue
				}
				info, err := entry.Info()
				if err != nil {
					return err
				}
				rel, err := filepath.Rel(dir, filepath.Join(scan.path, entry.Name()))
				if err != nil {
					return err
				}
				if referenced[filepath.Clean(rel)] || !info.ModTime().Before(olderThan) {
					continue
				}
				if err := os.Remove(filepath.Join(scan.path, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
					return err
				}
				if scan.kind == "entry" {
					result.Entries++
				} else {
					result.Checkpoints++
				}
			}
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	pending, err := s.CleanupPending(repoID, olderThan)
	result.Pending = pending
	return result, err
}

func (s Store) withLock(repoID string, fn func() error) error {
	if err := s.checkHealthy(); err != nil {
		return err
	}
	dir := filepath.Join(s.Root, repoID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	lock, err := os.OpenFile(filepath.Join(dir, "manifest.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) //nolint:errcheck
	return fn()
}

func validateUpdates(m Manifest, updates []RefUpdate) error {
	for _, u := range updates {
		current, ok := m.Refs[u.Ref]
		if !ok {
			current = strings.Repeat("0", len(u.Old))
		}
		if current != u.Old {
			return fmt.Errorf("manifest conflict for %s: expected %s, found %s", u.Ref, u.Old, current)
		}
	}
	return nil
}

func validatePreparedLocks(m Manifest, transactionID string, updates []RefUpdate) error {
	touched := make(map[string]bool, len(updates))
	for _, update := range updates {
		touched[update.Ref] = true
	}
	for id, prepared := range m.Prepared {
		if id == transactionID {
			continue
		}
		for _, update := range prepared.Updates {
			if touched[update.Ref] {
				return fmt.Errorf("ref %s is locked by prepared transaction %s", update.Ref, id)
			}
		}
	}
	return nil
}

func transactionState(m Manifest, updates []RefUpdate) (before, after bool) {
	before, after = true, true
	for _, update := range updates {
		current, ok := m.Refs[update.Ref]
		if !ok {
			current = strings.Repeat("0", len(update.Old))
		}
		if current != update.Old {
			before = false
		}
		if current != update.New {
			after = false
		}
	}
	return before, after
}

func invertUpdates(updates []RefUpdate) []RefUpdate {
	inverse := make([]RefUpdate, len(updates))
	for i, update := range updates {
		inverse[i] = RefUpdate{Old: update.New, New: update.Old, Ref: update.Ref}
	}
	return inverse
}

func updatesAlreadyApplied(m Manifest, updates []RefUpdate) bool {
	for _, update := range updates {
		current, ok := m.Refs[update.Ref]
		if isZeroOID(update.New) {
			if ok {
				return false
			}
			continue
		}
		if !ok || current != update.New {
			return false
		}
	}
	return true
}

func readManifest(dir string) (Manifest, error) {
	data, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return Manifest{}, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return Manifest{}, err
	}
	if m.Version != manifestVersion {
		return Manifest{}, fmt.Errorf("unsupported manifest version %d", m.Version)
	}
	if m.Refs == nil {
		m.Refs = map[string]string{}
	}
	return m, nil
}

func (s Store) writeManifest(dir string, m Manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tmp, err := os.CreateTemp(dir, ".manifest-*.tmp")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err := (&durabilityWriter{store: s, writer: tmp, operation: "write manifest"}).Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := s.syncFile(tmp, "sync manifest"); err != nil {
		tmp.Close()
		return err
	}
	if err := s.closeFile(tmp, "close manifest"); err != nil {
		return err
	}
	return s.rename(name, filepath.Join(dir, "manifest.json"), "publish manifest")
}

type durabilityWriter struct {
	store     Store
	writer    io.Writer
	operation string
}

func (w *durabilityWriter) Write(data []byte) (int, error) {
	n, err := w.writer.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return n, w.store.poison(w.operation, err)
	}
	return n, nil
}

func (s Store) checkHealthy() error {
	root := filepath.Clean(s.Root)
	if cause, ok := poisonedStores.Load(root); ok {
		return fmt.Errorf("WAL store is poisoned after a persistence failure: %v", cause)
	}
	data, err := os.ReadFile(filepath.Join(root, ".walgit-poisoned"))
	if err == nil {
		cause := strings.TrimSpace(string(data))
		if cause == "" {
			cause = "unknown persistence failure"
		}
		poisonedStores.Store(root, cause)
		return fmt.Errorf("WAL store is poisoned after a persistence failure: %s", cause)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("check WAL store health: %w", err)
	}
	return nil
}

func (s Store) poison(operation string, cause error) error {
	poisoned := fmt.Errorf("%s: %w", operation, cause)
	root := filepath.Clean(s.Root)
	poisonedStores.Store(root, poisoned)

	// This sentinel makes independent hook processes fail closed too. It is
	// best-effort because the same filesystem has just reported that it cannot
	// be trusted. Operators must recover/verify the store before removing it.
	if err := os.MkdirAll(root, 0o755); err == nil {
		path := filepath.Join(root, ".walgit-poisoned")
		if f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600); err == nil {
			_, _ = fmt.Fprintf(f, "%s\n", poisoned)
			_ = f.Sync()
			_ = f.Close()
			_ = syncDir(root)
		}
	}
	return fmt.Errorf("WAL store poisoned after persistence failure: %w", poisoned)
}

func (s Store) syncFile(file *os.File, operation string) error {
	var err error
	if s.faults != nil && s.faults.syncFile != nil {
		err = s.faults.syncFile(file)
	} else {
		err = file.Sync()
	}
	if err != nil {
		return s.poison(operation, err)
	}
	return nil
}

func (s Store) closeFile(file *os.File, operation string) error {
	if err := file.Close(); err != nil {
		return s.poison(operation, err)
	}
	return nil
}

func (s Store) rename(oldPath, newPath, operation string) error {
	if err := os.Rename(oldPath, newPath); err != nil {
		if isPersistenceFailure(err) {
			return s.poison(operation, err)
		}
		return fmt.Errorf("%s: %w", operation, err)
	}
	return nil
}

func isPersistenceFailure(err error) bool {
	return errors.Is(err, syscall.EIO) || errors.Is(err, syscall.ENOSPC) ||
		errors.Is(err, syscall.EDQUOT) || errors.Is(err, syscall.EROFS) ||
		errors.Is(err, syscall.ENODEV) || errors.Is(err, syscall.ESTALE)
}

func (s Store) syncDirectory(path, operation string) error {
	var err error
	if s.faults != nil && s.faults.syncDir != nil {
		err = s.faults.syncDir(path)
	} else {
		err = syncDir(path)
	}
	if err != nil {
		return s.poison(operation, err)
	}
	return nil
}

func writeArchive(w io.Writer, meta EntryMeta, objectDir string) error {
	tw := tar.NewWriter(w)
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if err := tw.WriteHeader(&tar.Header{Name: "meta.json", Mode: 0o644, Size: int64(len(metaJSON))}); err != nil {
		return err
	}
	if _, err := tw.Write(metaJSON); err != nil {
		return err
	}
	if len(meta.Objects) > 0 || meta.Descriptor != nil {
		return tw.Close()
	}
	return writeObjectFiles(tw, objectDir)
}

func writeCheckpointArchive(w io.Writer, meta CheckpointMeta, objectDir string) error {
	tw := tar.NewWriter(w)
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if err := tw.WriteHeader(&tar.Header{Name: "checkpoint.json", Mode: 0o644, Size: int64(len(metaJSON))}); err != nil {
		return err
	}
	if _, err := tw.Write(metaJSON); err != nil {
		return err
	}
	if len(meta.Objects) > 0 || meta.Descriptor != nil {
		return tw.Close()
	}
	return writeObjectFiles(tw, objectDir)
}

func writeMetadataArchive(w io.Writer, meta EntryMeta) error {
	tw := tar.NewWriter(w)
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if err := tw.WriteHeader(&tar.Header{Name: "meta.json", Mode: 0o644, Size: int64(len(metaJSON))}); err != nil {
		return err
	}
	if _, err := tw.Write(metaJSON); err != nil {
		return err
	}
	return tw.Close()
}

func writeObjectFiles(tw *tar.Writer, objectDir string) error {
	var paths []string
	if err := filepath.WalkDir(objectDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			rel, relErr := filepath.Rel(objectDir, path)
			if relErr != nil {
				return relErr
			}
			if transientObjectFile(rel) {
				return nil
			}
			paths = append(paths, path)
		}
		return nil
	}); err != nil {
		return err
	}
	sort.Strings(paths)
	for _, path := range paths {
		rel, err := filepath.Rel(objectDir, path)
		if err != nil {
			return err
		}
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if err := tw.WriteHeader(&tar.Header{Name: "objects/" + filepath.ToSlash(rel), Mode: 0o444, Size: info.Size()}); err != nil {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		buffer := archiveBufferPool.Get().(*[]byte)
		_, copyErr := io.CopyBuffer(tw, f, *buffer)
		archiveBufferPool.Put(buffer)
		closeErr := f.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return tw.Close()
}

func readArchive(r io.Reader, objectDir string, blobs blobStore) (EntryMeta, error) {
	tr := tar.NewReader(bufio.NewReader(r))
	var meta EntryMeta
	seenMeta := false
	touched := make(map[string]bool)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return meta, err
		}
		if h.Name == "meta.json" {
			if err := json.NewDecoder(io.LimitReader(tr, h.Size)).Decode(&meta); err != nil {
				return meta, err
			}
			seenMeta = true
			continue
		}
		if !strings.HasPrefix(h.Name, "objects/") {
			return meta, fmt.Errorf("unexpected archive path %q", h.Name)
		}
		directory, err := extractObject(tr, h.Name, objectDir)
		if err != nil {
			return meta, err
		}
		touched[directory] = true
	}
	if !seenMeta {
		return meta, errors.New("entry has no metadata")
	}
	meta, err := resolveEntryMeta(meta, blobs)
	if err != nil {
		return meta, err
	}
	if err := syncObjectDirectories(objectDir, touched); err != nil {
		return meta, fmt.Errorf("make replayed objects durable: %w", err)
	}
	if len(meta.Objects) > 0 {
		if blobs == nil {
			return meta, errors.New("entry references blobs but no blob store is configured")
		}
		if err := restoreObjectBlobs(meta.Objects, objectDir, blobs); err != nil {
			return meta, fmt.Errorf("restore entry blobs: %w", err)
		}
	}
	return meta, nil
}

func readCheckpointArchive(r io.Reader, objectDir string, blobs blobStore) (CheckpointMeta, error) {
	tr := tar.NewReader(bufio.NewReader(r))
	var meta CheckpointMeta
	seenMeta := false
	touched := make(map[string]bool)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return meta, err
		}
		if h.Name == "checkpoint.json" {
			if err := json.NewDecoder(io.LimitReader(tr, h.Size)).Decode(&meta); err != nil {
				return meta, err
			}
			seenMeta = true
			continue
		}
		if !strings.HasPrefix(h.Name, "objects/") {
			return meta, fmt.Errorf("unexpected checkpoint path %q", h.Name)
		}
		directory, err := extractObject(tr, h.Name, objectDir)
		if err != nil {
			return meta, err
		}
		touched[directory] = true
	}
	if !seenMeta {
		return meta, errors.New("checkpoint has no metadata")
	}
	meta, err := resolveCheckpointMeta(meta, blobs)
	if err != nil {
		return meta, err
	}
	if err := syncObjectDirectories(objectDir, touched); err != nil {
		return meta, fmt.Errorf("make restored checkpoint objects durable: %w", err)
	}
	if len(meta.Objects) > 0 {
		if blobs == nil {
			return meta, errors.New("checkpoint references blobs but no blob store is configured")
		}
		if err := restoreObjectBlobs(meta.Objects, objectDir, blobs); err != nil {
			return meta, fmt.Errorf("restore checkpoint blobs: %w", err)
		}
	}
	return meta, nil
}

func validateEntryArchive(r io.Reader, expectedTransactionID string) error {
	meta, err := readEntryArchiveMeta(r)
	if err != nil {
		return err
	}
	if meta.TransactionID != expectedTransactionID {
		return fmt.Errorf("transaction mismatch: got %s, want %s", meta.TransactionID, expectedTransactionID)
	}
	return nil
}

func readEntryArchiveMeta(r io.Reader) (EntryMeta, error) {
	tr := tar.NewReader(bufio.NewReader(r))
	var meta EntryMeta
	seenMeta := false
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return meta, err
		}
		if header.Name == "meta.json" {
			if seenMeta {
				return meta, errors.New("entry contains duplicate metadata")
			}
			if err := json.NewDecoder(io.LimitReader(tr, header.Size)).Decode(&meta); err != nil {
				return meta, err
			}
			seenMeta = true
			continue
		}
		if !safeArchivedObjectPath(header.Name) {
			return meta, fmt.Errorf("unexpected archive path %q", header.Name)
		}
	}
	if !seenMeta {
		return meta, errors.New("entry has no metadata")
	}
	return meta, nil
}

func archiveEntryDetails(path string, blobs blobStore) (int64, *ChunkRef, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, nil, err
	}
	defer file.Close()
	tr := tar.NewReader(bufio.NewReader(file))
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return 0, nil, errors.New("entry has no metadata")
		}
		if err != nil {
			return 0, nil, err
		}
		if header.Name != "meta.json" {
			continue
		}
		var meta EntryMeta
		if err := json.NewDecoder(io.LimitReader(tr, header.Size)).Decode(&meta); err != nil {
			return 0, nil, err
		}
		descriptor := meta.Descriptor
		meta, err = resolveEntryMeta(meta, blobs)
		if err != nil {
			return 0, nil, err
		}
		return objectBlobBytes(meta.Objects), descriptor, nil
	}
}

func archivePayloadBytes(path string, blobs blobStore) (int64, error) {
	payloadBytes, _, err := archiveEntryDetails(path, blobs)
	return payloadBytes, err
}

func validateCheckpointArchive(r io.Reader, expectedGeneration uint64) error {
	tr := tar.NewReader(bufio.NewReader(r))
	var meta CheckpointMeta
	seenMeta := false
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if header.Name == "checkpoint.json" {
			if seenMeta {
				return errors.New("checkpoint contains duplicate metadata")
			}
			if err := json.NewDecoder(io.LimitReader(tr, header.Size)).Decode(&meta); err != nil {
				return err
			}
			seenMeta = true
			continue
		}
		if !safeArchivedObjectPath(header.Name) {
			return fmt.Errorf("unexpected checkpoint path %q", header.Name)
		}
	}
	if !seenMeta {
		return errors.New("checkpoint has no metadata")
	}
	if meta.Generation != expectedGeneration {
		return fmt.Errorf("checkpoint generation mismatch: got %d, want %d", meta.Generation, expectedGeneration)
	}
	return nil
}

func safeArchivedObjectPath(name string) bool {
	if !strings.HasPrefix(name, "objects/") {
		return false
	}
	rel := strings.TrimPrefix(name, "objects/")
	clean := filepath.Clean(filepath.FromSlash(rel))
	return clean != "." && !filepath.IsAbs(clean) && clean != ".." && !strings.HasPrefix(clean, ".."+string(filepath.Separator))
}

func extractObject(r io.Reader, archiveName, objectDir string) (string, error) {
	rel := strings.TrimPrefix(archiveName, "objects/")
	clean := filepath.Clean(filepath.FromSlash(rel))
	if clean == "." || filepath.IsAbs(clean) || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("unsafe archive path %q", archiveName)
	}
	directory := filepath.Join(objectDir, filepath.Dir(clean))
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(directory, ".walgit-object-*.tmp")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	_, copyErr := io.Copy(tmp, r)
	if copyErr != nil {
		tmp.Close()
		return "", copyErr
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return "", err
	}
	if err := tmp.Chmod(0o444); err != nil {
		tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmpName, filepath.Join(objectDir, clean)); err != nil {
		return "", err
	}
	return directory, nil
}

func syncObjectDirectories(objectDir string, touched map[string]bool) error {
	root := filepath.Clean(objectDir)
	directories := make(map[string]bool, len(touched)+1)
	for directory := range touched {
		for current := filepath.Clean(directory); ; current = filepath.Dir(current) {
			directories[current] = true
			if current == root {
				break
			}
			parent := filepath.Dir(current)
			if parent == current || !strings.HasPrefix(current+string(filepath.Separator), root+string(filepath.Separator)) {
				return fmt.Errorf("object directory %q escapes %q", directory, root)
			}
		}
	}
	ordered := make([]string, 0, len(directories))
	for directory := range directories {
		ordered = append(ordered, directory)
	}
	sort.Slice(ordered, func(i, j int) bool {
		return strings.Count(ordered[i], string(filepath.Separator)) > strings.Count(ordered[j], string(filepath.Separator))
	})
	for _, directory := range ordered {
		if err := syncDir(directory); err != nil {
			return err
		}
	}
	return nil
}

func transientObjectFile(path string) bool {
	name := filepath.Base(path)
	return strings.HasSuffix(name, ".keep") || strings.HasSuffix(name, ".lock") ||
		strings.HasSuffix(name, ".tmp") || strings.HasPrefix(name, "tmp_obj_") ||
		strings.HasPrefix(name, ".walgit-object-")
}

func transactionID(updates []RefUpdate) (string, error) {
	data, err := json.Marshal(updates)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func transactionIdentityMatches(identity string, updates []RefUpdate) bool {
	digest, err := transactionID(updates)
	if err != nil {
		return false
	}
	if identity == digest {
		return true
	}
	if !strings.HasPrefix(identity, "rollback-") {
		return false
	}
	original, err := transactionID(invertUpdates(updates))
	return err == nil && identity == "rollback-"+original
}

func cloneRefs(refs map[string]string) map[string]string {
	cloned := make(map[string]string, len(refs))
	for ref, oid := range refs {
		cloned[ref] = oid
	}
	return cloned
}

func fileDigest(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func validateID(id string) error {
	if id == "" || id == "." || id == ".." || strings.ContainsAny(id, `/\\`) {
		return fmt.Errorf("invalid repository ID %q", id)
	}
	return nil
}

func isZeroOID(oid string) bool { return strings.Trim(oid, "0") == "" }

func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func failpoint(name string) error {
	for _, configured := range strings.Split(os.Getenv("WALGIT_FAILPOINT"), ",") {
		if strings.TrimSpace(configured) == name {
			return fmt.Errorf("injected failure at %s", name)
		}
	}
	return nil
}
