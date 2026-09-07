package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCommitRecoversAfterEntryRenameFailure(t *testing.T) {
	store, id, objects, updates := testTransaction(t)
	if err := store.Stage(id, objects, updates); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WALGIT_FAILPOINT", "commit.after_entry_rename")
	if _, _, err := store.Commit(id, updates); err == nil {
		t.Fatal("expected injected failure")
	}
	t.Setenv("WALGIT_FAILPOINT", "")
	entry, manifest, err := store.Commit(id, updates)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Generation != 1 || manifest.Generation != 1 || manifest.Refs[updates[0].Ref] != updates[0].New {
		t.Fatalf("unexpected committed state: entry=%#v manifest=%#v", entry, manifest)
	}
}

func TestCommitIsIdempotentAfterManifestFailure(t *testing.T) {
	store, id, objects, updates := testTransaction(t)
	if err := store.Stage(id, objects, updates); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WALGIT_FAILPOINT", "commit.after_manifest")
	if _, _, err := store.Commit(id, updates); err == nil {
		t.Fatal("expected injected failure")
	}
	t.Setenv("WALGIT_FAILPOINT", "")
	entry, manifest, err := store.Commit(id, updates)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Generation != 1 || manifest.Generation != 1 || len(manifest.Entries) != 1 {
		t.Fatalf("transaction was not idempotent: entry=%#v manifest=%#v", entry, manifest)
	}
}

func TestCommitRejectsManifestConflict(t *testing.T) {
	store, id, objects, updates := testTransaction(t)
	updates[0].Old = strings.Repeat("b", 40)
	if err := store.Stage(id, objects, updates); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Commit(id, updates); err == nil || !strings.Contains(err.Error(), "manifest conflict") {
		t.Fatalf("expected manifest conflict, got %v", err)
	}
}

func TestAbortAppendsCompensatingTransaction(t *testing.T) {
	store, id, objects, updates := testTransaction(t)
	if err := store.Stage(id, objects, updates); err != nil {
		t.Fatal(err)
	}
	if _, manifest, err := store.Commit(id, updates); err != nil {
		t.Fatal(err)
	} else if len(manifest.Prepared) != 1 {
		t.Fatalf("commit did not record prepared transaction: %#v", manifest.Prepared)
	}
	if err := store.Abort(id, updates); err != nil {
		t.Fatal(err)
	}
	if err := store.Abort(id, updates); err != nil {
		t.Fatalf("abort retry was not idempotent: %v", err)
	}
	manifest, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Generation != 2 || len(manifest.Entries) != 2 || len(manifest.Prepared) != 0 || len(manifest.Refs) != 0 {
		t.Fatalf("unexpected compensated manifest: %#v", manifest)
	}
	var applied []RefUpdate
	_, err = store.ReplayFrom(id, 0, filepath.Join(t.TempDir(), "objects"), func(_ ManifestEntry, meta EntryMeta) error {
		applied = append(applied, meta.Updates...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(applied) != 2 || applied[1].Old != updates[0].New || applied[1].New != updates[0].Old {
		t.Fatalf("unexpected replayed compensation: %#v", applied)
	}
}

func TestPreparedTransactionLocksTouchedRefsUntilFinalized(t *testing.T) {
	store, id, objects, first := testTransaction(t)
	if err := store.Stage(id, objects, first); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Commit(id, first); err != nil {
		t.Fatal(err)
	}
	second := []RefUpdate{{Old: first[0].New, New: strings.Repeat("b", 40), Ref: first[0].Ref}}
	if err := store.Stage(id, objects, second); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Commit(id, second); err == nil || !strings.Contains(err.Error(), "locked by prepared transaction") {
		t.Fatalf("expected prepared ref lock, got %v", err)
	}
	if err := store.Finalize(id, first); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Commit(id, second); err != nil {
		t.Fatalf("commit after finalize failed: %v", err)
	}
}

func TestRepeatedIdenticalRefTransitionAfterCycleIsNotDeduplicated(t *testing.T) {
	store, id, objects, forward := testTransaction(t)
	for cycle, updates := range [][]RefUpdate{forward, invertUpdates(forward), forward} {
		if err := store.Stage(id, objects, updates); err != nil {
			t.Fatal(err)
		}
		if _, _, err := store.Commit(id, updates); err != nil {
			t.Fatalf("cycle %d commit: %v", cycle, err)
		}
		if err := store.Finalize(id, updates); err != nil {
			t.Fatalf("cycle %d finalize: %v", cycle, err)
		}
	}
	manifest, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Generation != 3 || manifest.Refs[forward[0].Ref] != forward[0].New {
		t.Fatalf("repeated transition was lost: %#v", manifest)
	}
}

func TestConcurrentIndependentCommitsAreLinearized(t *testing.T) {
	root := t.TempDir()
	store := Store{Root: filepath.Join(root, "store")}
	if err := store.Initialize("repo", "refs/heads/main", "sha1"); err != nil {
		t.Fatal(err)
	}
	objects := filepath.Join(root, "objects")
	if err := os.MkdirAll(objects, 0o755); err != nil {
		t.Fatal(err)
	}
	const writers = 12
	updates := make([][]RefUpdate, writers)
	for i := range updates {
		oid := fmt.Sprintf("%040x", i+1)
		updates[i] = []RefUpdate{{Old: strings.Repeat("0", 40), New: oid, Ref: "refs/heads/branch-" + string(rune('a'+i))}}
		if err := store.Stage("repo", objects, updates[i]); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := range updates {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := store.Commit("repo", updates[i])
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	m, err := store.Load("repo")
	if err != nil {
		t.Fatal(err)
	}
	if m.Generation != writers || len(m.Entries) != writers || len(m.Refs) != writers {
		t.Fatalf("concurrent commits were lost: %#v", m)
	}
}

func TestCheckpointBoundsReplayAndGarbageCollectsOldEntries(t *testing.T) {
	store, id, objects, updates := testTransaction(t)
	if err := store.Stage(id, objects, updates); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Commit(id, updates); err != nil {
		t.Fatal(err)
	}
	if err := store.Finalize(id, updates); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := store.CreateCheckpoint(id, objects, 1)
	if err != nil {
		t.Fatal(err)
	}
	m, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Generation != 1 || len(m.Entries) != 0 || m.Checkpoint == nil {
		t.Fatalf("checkpoint did not compact manifest: checkpoint=%#v manifest=%#v", checkpoint, m)
	}
	if err := store.Stage(id, objects, updates); err != nil {
		t.Fatal(err)
	}
	if entry, retried, err := store.Commit(id, updates); err != nil {
		t.Fatalf("retry after compaction was not idempotent: %v", err)
	} else if entry.Generation != 1 || retried.Generation != 1 || len(retried.Entries) != 0 {
		t.Fatalf("retry changed compacted state: entry=%#v manifest=%#v", entry, retried)
	}
	restoredObjects := filepath.Join(t.TempDir(), "objects")
	meta, err := store.RestoreCheckpoint(id, restoredObjects)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Refs[updates[0].Ref] != updates[0].New {
		t.Fatalf("checkpoint refs do not match: %#v", meta.Refs)
	}
	gc, err := store.GarbageCollect(id, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if gc.Entries != 1 || gc.Checkpoints != 0 {
		t.Fatalf("unexpected garbage collection result: %#v", gc)
	}
	if _, err := os.Stat(filepath.Join(store.Root, id, filepath.FromSlash(checkpoint.File))); err != nil {
		t.Fatalf("referenced checkpoint was removed: %v", err)
	}
}

func TestReplayRejectsCorruptedWALEntry(t *testing.T) {
	store, id, objects, updates := testTransaction(t)
	if err := store.Stage(id, objects, updates); err != nil {
		t.Fatal(err)
	}
	entry, _, err := store.Commit(id, updates)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(store.Root, id, entry.File)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = store.ReplayFrom(id, 0, filepath.Join(t.TempDir(), "objects"), func(ManifestEntry, EntryMeta) error { return nil })
	if err == nil {
		t.Fatal("corrupted WAL entry replayed successfully")
	}
}

func TestReplayAtomicallyReplacesPartialObjectAndSkipsTransientFiles(t *testing.T) {
	store, id, objects, updates := testTransaction(t)
	for _, name := range []string{"objects.keep", "objects.lock", ".walgit-object-stale.tmp"} {
		if err := os.WriteFile(filepath.Join(objects, "pack", name), []byte("transient"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Stage(id, objects, updates); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Commit(id, updates); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "objects")
	if err := os.MkdirAll(filepath.Join(destination, "pack"), 0o755); err != nil {
		t.Fatal(err)
	}
	objectPath := filepath.Join(destination, "pack", "objects.pack")
	if err := os.WriteFile(objectPath, []byte("partial"), 0o444); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReplayFrom(id, 0, destination, func(ManifestEntry, EntryMeta) error { return nil }); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(objectPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "test objects" {
		t.Fatalf("replayed object contains %q, want complete object", data)
	}
	for _, name := range []string{"objects.keep", "objects.lock", ".walgit-object-stale.tmp"} {
		if _, err := os.Stat(filepath.Join(destination, "pack", name)); !os.IsNotExist(err) {
			t.Fatalf("transient file %s was replayed: %v", name, err)
		}
	}
}

func TestPersistenceFailurePoisonsFilesystemStore(t *testing.T) {
	store, id, objects, updates := testTransaction(t)
	syncCalls := 0
	store.faults = &storeFaults{syncFile: func(*os.File) error {
		syncCalls++
		return errors.New("injected writeback EIO")
	}}
	if err := store.Stage(id, objects, updates); err == nil || !strings.Contains(err.Error(), "poisoned") {
		t.Fatalf("expected a poisoning persistence failure, got %v", err)
	}
	if syncCalls != 1 {
		t.Fatalf("sync called %d times, want 1", syncCalls)
	}

	// Simulate fsyncgate's dangerous second phase: a later fsync would report
	// success. The store must remain unavailable instead of retrying it.
	store.faults.syncFile = func(*os.File) error {
		syncCalls++
		return nil
	}
	if err := store.Stage(id, objects, updates); err == nil || !strings.Contains(err.Error(), "poisoned") {
		t.Fatalf("poisoned store accepted a later operation: %v", err)
	}
	if syncCalls != 1 {
		t.Fatalf("poisoned store retried fsync; calls=%d", syncCalls)
	}
	if _, err := store.Load(id); err == nil || !strings.Contains(err.Error(), "poisoned") {
		t.Fatalf("poisoned store remained readable: %v", err)
	}
	if _, err := os.Stat(filepath.Join(store.Root, ".walgit-poisoned")); err != nil {
		t.Fatalf("persistent poison sentinel missing: %v", err)
	}
}

func TestMissingStagedTransactionDoesNotPoisonFilesystemStore(t *testing.T) {
	store, id, _, updates := testTransaction(t)
	if _, _, err := store.Commit(id, updates); err == nil {
		t.Fatal("commit without a staged transaction succeeded")
	}
	if _, err := store.Load(id); err != nil {
		t.Fatalf("logical transaction error poisoned healthy storage: %v", err)
	}
}

func TestCheckpointDirectoryIsSyncedBeforeManifestPublication(t *testing.T) {
	store, id, objects, updates := testTransaction(t)
	if err := store.Stage(id, objects, updates); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Commit(id, updates); err != nil {
		t.Fatal(err)
	}
	if err := store.Finalize(id, updates); err != nil {
		t.Fatal(err)
	}
	var synced []string
	store.faults = &storeFaults{syncDir: func(path string) error {
		synced = append(synced, filepath.Clean(path))
		return nil
	}}
	if _, err := store.CreateCheckpoint(id, objects, 1); err != nil {
		t.Fatal(err)
	}
	checkpointDir := filepath.Join(store.Root, id, "checkpoints")
	repoDir := filepath.Join(store.Root, id)
	if len(synced) < 2 || synced[0] != checkpointDir || synced[len(synced)-1] != repoDir {
		t.Fatalf("unexpected directory sync order: %v", synced)
	}
}

func testTransaction(t *testing.T) (Store, string, string, []RefUpdate) {
	t.Helper()
	root := t.TempDir()
	store := Store{Root: filepath.Join(root, "store")}
	id := "repo"
	if err := store.Initialize(id, "refs/heads/main", "sha1"); err != nil {
		t.Fatal(err)
	}
	objects := filepath.Join(root, "objects")
	if err := os.MkdirAll(filepath.Join(objects, "pack"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(objects, "pack", "objects.pack"), []byte("test objects"), 0o644); err != nil {
		t.Fatal(err)
	}
	updates := []RefUpdate{{
		Old: strings.Repeat("0", 40),
		New: strings.Repeat("a", 40),
		Ref: "refs/heads/main",
	}}
	return store, id, objects, updates
}
