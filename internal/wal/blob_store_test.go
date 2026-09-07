package wal

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBlobBackedWALChunksReplicatesDeduplicatesAndRestores(t *testing.T) {
	store, id, objects, updates := testTransaction(t)
	primary := NewMemoryBlobStore()
	secondary := NewMemoryBlobStore()
	store.blobs = replicatedBlobStore{stores: []blobStore{primary, secondary}}
	store.externalizeTransactions = true

	large := bytes.Repeat([]byte("a"), 2*blobChunkSize+12345)
	large[blobChunkSize] = 'b'
	large[2*blobChunkSize] = 'c'
	largePath := filepath.Join(objects, "pack", "large.pack")
	if err := os.WriteFile(largePath, large, 0o444); err != nil {
		t.Fatal(err)
	}
	if err := store.Stage(id, objects, updates); err != nil {
		t.Fatal(err)
	}
	entry, _, err := store.Commit(id, updates)
	if err != nil {
		t.Fatal(err)
	}
	if primary.ChunkCount() != 5 || secondary.ChunkCount() != 5 {
		t.Fatalf("unexpected replicated chunk counts: primary=%d secondary=%d", primary.ChunkCount(), secondary.ChunkCount())
	}
	if entry.Bytes >= int64(len(large))/100 {
		t.Fatalf("WAL descriptor is unexpectedly large: WAL=%d payload=%d", entry.Bytes, len(large))
	}
	if entry.PayloadBytes != int64(len(large))+int64(len("test objects")) {
		t.Fatalf("manifest payload bytes = %d, want %d", entry.PayloadBytes, len(large)+len("test objects"))
	}

	restored := filepath.Join(t.TempDir(), "objects")
	var replayed EntryMeta
	if _, err := store.ReplayFrom(id, 0, restored, func(_ ManifestEntry, meta EntryMeta) error {
		replayed = meta
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(restored, "pack", "large.pack"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, large) || len(replayed.Objects) != 2 {
		t.Fatal("blob-backed WAL did not reconstruct the original object set")
	}

	// A checkpoint sees the same object content and must reuse the existing
	// content-addressed chunks instead of creating new stored data.
	if err := store.Finalize(id, updates); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := store.CreateCheckpoint(id, objects, 1)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.PayloadBytes != entry.PayloadBytes {
		t.Fatalf("checkpoint payload bytes = %d, want %d", checkpoint.PayloadBytes, entry.PayloadBytes)
	}
	if primary.ChunkCount() != 6 || secondary.ChunkCount() != 6 {
		t.Fatalf("checkpoint defeated deduplication: primary=%d secondary=%d", primary.ChunkCount(), secondary.ChunkCount())
	}
}

func TestSmallWALRemainsSingleInlineArchive(t *testing.T) {
	store, id, objects, updates := testTransaction(t)
	blobs := NewMemoryBlobStore()
	store.blobs = blobs
	if err := store.Stage(id, objects, updates); err != nil {
		t.Fatal(err)
	}
	if blobs.ChunkCount() != 0 {
		t.Fatalf("small transaction unexpectedly externalized %d chunks", blobs.ChunkCount())
	}
	if _, _, err := store.Commit(id, updates); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReplayFrom(id, 0, filepath.Join(t.TempDir(), "objects"), func(ManifestEntry, EntryMeta) error { return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedBlobReadFallsBackFromCorruptAuthority(t *testing.T) {
	store, id, objects, updates := testTransaction(t)
	large := bytes.Repeat([]byte("x"), blobExternalizeThreshold)
	if err := os.WriteFile(filepath.Join(objects, "pack", "large.pack"), large, 0o444); err != nil {
		t.Fatal(err)
	}
	primary := NewMemoryBlobStore()
	secondary := NewMemoryBlobStore()
	store.blobs = replicatedBlobStore{stores: []blobStore{primary, secondary}}
	store.externalizeTransactions = true
	if err := store.Stage(id, objects, updates); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Commit(id, updates); err != nil {
		t.Fatal(err)
	}
	digest := chunkRef(large).SHA256
	if err := primary.Corrupt(digest); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReplayFrom(id, 0, filepath.Join(t.TempDir(), "objects"), func(ManifestEntry, EntryMeta) error { return nil }); err != nil {
		t.Fatalf("healthy secondary did not mask corrupt primary: %v", err)
	}
	if err := secondary.Corrupt(digest); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReplayFrom(id, 0, filepath.Join(t.TempDir(), "objects"), func(ManifestEntry, EntryMeta) error { return nil }); err == nil {
		t.Fatal("replay accepted a chunk corrupted in both authorities")
	}
}

func TestReplicatedBlobWriteRequiresEveryAuthority(t *testing.T) {
	store, id, objects, updates := testTransaction(t)
	if err := os.WriteFile(filepath.Join(objects, "pack", "large.pack"), bytes.Repeat([]byte("x"), blobExternalizeThreshold), 0o444); err != nil {
		t.Fatal(err)
	}
	primary := NewMemoryBlobStore()
	store.blobs = replicatedBlobStore{stores: []blobStore{primary, failingBlobStore{}}}
	store.externalizeTransactions = true
	if err := store.Stage(id, objects, updates); err == nil || !strings.Contains(err.Error(), "every durable authority") {
		t.Fatalf("stage acknowledged a one-sided blob write: %v", err)
	}
	manifest, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Generation != 0 || len(manifest.Entries) != 0 {
		t.Fatalf("one-sided blob write changed authority: %#v", manifest)
	}
}

func TestRequireBlobReplicationFailsClosedWithoutSecondary(t *testing.T) {
	t.Setenv("WALGIT_REQUIRE_BLOB_REPLICATION", "true")
	t.Setenv("WALGIT_BLOB_SECONDARY_STORE", "")
	if _, err := Open(filepath.Join(t.TempDir(), "store")); err == nil || !strings.Contains(err.Error(), "SECONDARY_STORE") {
		t.Fatalf("required replication opened without a secondary: %v", err)
	}
}

func TestBlobReplicationRejectsSameFilesystemThroughAlias(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	t.Setenv("WALGIT_BLOB_SECONDARY_STORE", "file://"+root)
	t.Setenv("WALGIT_REQUIRE_BLOB_REPLICATION", "true")
	if _, err := Open(root); err == nil || !strings.Contains(err.Error(), "different locations") {
		t.Fatalf("same blob authority was accepted twice: %v", err)
	}
}

func TestOpenConfiguresFilesystemBlobReplicationAndFallback(t *testing.T) {
	primaryRoot := filepath.Join(t.TempDir(), "primary")
	secondaryRoot := filepath.Join(t.TempDir(), "secondary")
	t.Setenv("WALGIT_BLOB_SECONDARY_STORE", secondaryRoot)
	t.Setenv("WALGIT_REQUIRE_BLOB_REPLICATION", "true")
	backend, err := Open(primaryRoot)
	if err != nil {
		t.Fatal(err)
	}
	store := backend.(Store)
	id := "repo"
	if err := store.Initialize(id, "refs/heads/main", "sha1"); err != nil {
		t.Fatal(err)
	}
	objects := filepath.Join(t.TempDir(), "objects")
	if err := os.MkdirAll(filepath.Join(objects, "pack"), 0o755); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("r"), blobExternalizeThreshold)
	if err := os.WriteFile(filepath.Join(objects, "pack", "large.pack"), payload, 0o444); err != nil {
		t.Fatal(err)
	}
	updates := []RefUpdate{{Old: strings.Repeat("0", 40), New: strings.Repeat("a", 40), Ref: "refs/heads/main"}}
	if err := store.Stage(id, objects, updates); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Commit(id, updates); err != nil {
		t.Fatal(err)
	}

	primaryBlobs := regularFiles(t, filepath.Join(primaryRoot, ".walgit-blobs"))
	secondaryBlobs := regularFiles(t, filepath.Join(secondaryRoot, ".walgit-blobs"))
	if len(primaryBlobs) != 2 || len(secondaryBlobs) != 2 {
		t.Fatalf("unexpected filesystem replicas: primary=%d secondary=%d", len(primaryBlobs), len(secondaryBlobs))
	}
	if err := os.Remove(primaryBlobs[0]); err != nil {
		t.Fatal(err)
	}
	replayed := filepath.Join(t.TempDir(), "replayed")
	if _, err := store.ReplayFrom(id, 0, replayed, func(ManifestEntry, EntryMeta) error { return nil }); err != nil {
		t.Fatalf("filesystem secondary did not satisfy replay: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(replayed, "pack", "large.pack"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("filesystem secondary replay changed the payload")
	}
}

func TestLegacyEmbeddedArchiveStillReplays(t *testing.T) {
	objects := filepath.Join(t.TempDir(), "objects")
	if err := os.MkdirAll(filepath.Join(objects, "pack"), 0o755); err != nil {
		t.Fatal(err)
	}
	want := []byte("legacy embedded object")
	if err := os.WriteFile(filepath.Join(objects, "pack", "legacy.pack"), want, 0o444); err != nil {
		t.Fatal(err)
	}
	meta := EntryMeta{TransactionID: strings.Repeat("a", 64)}
	var archive bytes.Buffer
	if err := writeArchive(&archive, meta, objects); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(t.TempDir(), "objects")
	if _, err := readArchive(bytes.NewReader(archive.Bytes()), restored, nil); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(restored, "pack", "legacy.pack"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("legacy object contains %q, want %q", got, want)
	}
}

type failingBlobStore struct{}

func (failingBlobStore) Put(ChunkRef, []byte) error { return errors.New("injected secondary failure") }
func (failingBlobStore) Get(ChunkRef) ([]byte, error) {
	return nil, errors.New("injected secondary failure")
}

func regularFiles(t *testing.T, root string) []string {
	t.Helper()
	var files []string
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			files = append(files, path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return files
}
