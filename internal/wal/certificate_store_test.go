package wal

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDualAuthorityCertificateRecoversWithoutPrimaryManifestOrWAL(t *testing.T) {
	store, primary, secondary := dualFilesystemStore(t)
	id, objects, updates := certificateTransaction(t, store)
	if err := store.Stage(id, objects, updates); err != nil {
		t.Fatal(err)
	}
	entry, manifest, err := store.Commit(id, updates)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Finalize(id, updates); err != nil {
		t.Fatal(err)
	}
	if entry.Descriptor == nil || manifest.CertificateSHA256 == "" {
		t.Fatalf("commit is not independently replayable: entry=%#v manifest=%#v", entry, manifest)
	}
	if got := len(regularFiles(t, filepath.Join(primary.store.Root, ".walgit-certificates", id))); got != 2 {
		t.Fatalf("primary certificate count = %d, want genesis and commit", got)
	}
	if got := len(regularFiles(t, filepath.Join(secondary.store.Root, ".walgit-certificates", id))); got != 2 {
		t.Fatalf("secondary certificate count = %d, want genesis and commit", got)
	}

	if err := os.Rename(store.Root, store.Root+".lost"); err != nil {
		t.Fatal(err)
	}

	recovered, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Generation != 1 || recovered.Refs[updates[0].Ref] != updates[0].New {
		t.Fatalf("certificate recovery returned the wrong authority: %#v", recovered)
	}
	replayed := filepath.Join(t.TempDir(), "replayed")
	if _, err := store.ReplayFrom(id, 0, replayed, func(got ManifestEntry, meta EntryMeta) error {
		if got.Generation != 1 || meta.TransactionID != entry.TransactionID {
			t.Fatalf("unexpected recovered entry: %#v %#v", got, meta)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(filepath.Join(objects, "pack", "objects.pack"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(replayed, "pack", "objects.pack"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("certificate recovery changed the object payload")
	}
}

func TestCertificateWriteMustReachBothAuthorities(t *testing.T) {
	store, primary, secondary := dualFilesystemStore(t)
	id, objects, updates := certificateTransaction(t, store)
	if err := store.Stage(id, objects, updates); err != nil {
		t.Fatal(err)
	}
	store.certificates = replicatedCertificateStore{authorities: []certificateAuthority{
		primary, failingPutCertificateAuthority{certificateAuthority: secondary},
	}}
	if _, _, err := store.Commit(id, updates); err == nil || !strings.Contains(err.Error(), "every durable authority") {
		t.Fatalf("one-sided certificate write was acknowledged: %v", err)
	}
	manifest, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Generation != 0 || len(manifest.Entries) != 0 {
		t.Fatalf("one-sided certificate became authoritative while both stores were readable: %#v", manifest)
	}
}

func TestCertificateRecoveryFallsBackFromCorruptAuthority(t *testing.T) {
	store, primary, secondary := dualFilesystemStore(t)
	id, objects, updates := certificateTransaction(t, store)
	if err := store.Stage(id, objects, updates); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Commit(id, updates); err != nil {
		t.Fatal(err)
	}
	files := regularFiles(t, filepath.Join(primary.store.Root, ".walgit-certificates", id))
	last := files[len(files)-1]
	data, err := os.ReadFile(last)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)/2] ^= 0xff
	if err := os.WriteFile(last, data, 0o600); err != nil {
		t.Fatal(err)
	}
	manifest, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Generation != 1 {
		t.Fatalf("healthy secondary did not recover generation 1: %#v", manifest)
	}

	secondaryFiles := regularFiles(t, filepath.Join(secondary.store.Root, ".walgit-certificates", id))
	secondaryLast := secondaryFiles[len(secondaryFiles)-1]
	secondaryData, err := os.ReadFile(secondaryLast)
	if err != nil {
		t.Fatal(err)
	}
	secondaryData[len(secondaryData)/2] ^= 0xff
	if err := os.WriteFile(secondaryLast, secondaryData, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(id); err == nil {
		t.Fatal("corrupt certificate chains in both authorities were accepted")
	}
}

func TestCertificateChainIncludesAbortCompensation(t *testing.T) {
	store, primary, secondary := dualFilesystemStore(t)
	id, objects, updates := certificateTransaction(t, store)
	if err := store.Stage(id, objects, updates); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Commit(id, updates); err != nil {
		t.Fatal(err)
	}
	// The certificate is already authoritative here. Losing the mutable
	// manifest must not prevent Git's later abort notification from appending
	// the compensating certificate.
	if err := os.Remove(filepath.Join(store.Root, id, "manifest.json")); err != nil {
		t.Fatal(err)
	}
	if err := store.Abort(id, updates); err != nil {
		t.Fatal(err)
	}
	manifest, _, err := store.certificates.Recover(id)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Generation != 2 || len(manifest.Refs) != 0 || len(manifest.Entries) != 2 {
		t.Fatalf("certificate chain did not retain the compensation: %#v", manifest)
	}
	if manifest.Entries[1].Descriptor == nil || !strings.HasPrefix(manifest.Entries[1].TransactionID, "rollback-") {
		t.Fatalf("rollback is not independently replayable: %#v", manifest.Entries[1])
	}
	if got := len(regularFiles(t, filepath.Join(primary.store.Root, ".walgit-certificates", id))); got != 3 {
		t.Fatalf("primary certificate count = %d, want 3", got)
	}
	if got := len(regularFiles(t, filepath.Join(secondary.store.Root, ".walgit-certificates", id))); got != 3 {
		t.Fatalf("secondary certificate count = %d, want 3", got)
	}
}

func TestCertificateBatchedCommitIsOneAtomicLink(t *testing.T) {
	primaryClient := newMemoryS3()
	secondaryClient := newMemoryS3()
	store := &S3Store{
		client: primaryClient, bucket: "primary", prefix: "walgit", timeout: time.Second,
		unconditionalWrites: true, staged: make(map[string]stagedS3Transaction),
	}
	primary := s3BlobStore{store: store}
	secondaryStore := &S3Store{client: secondaryClient, bucket: "secondary", prefix: "walgit", timeout: time.Second}
	secondary := s3BlobStore{store: secondaryStore}
	store.blobs = replicatedBlobStore{stores: []blobStore{primary, secondary}}
	store.certificates = replicatedCertificateStore{authorities: []certificateAuthority{primary, secondary}}
	if err := store.Initialize("repo", "refs/heads/main", "sha1"); err != nil {
		t.Fatal(err)
	}
	objects := t.TempDir()
	first := []RefUpdate{{Old: strings.Repeat("0", 40), New: strings.Repeat("a", 40), Ref: "refs/heads/main"}}
	second := []RefUpdate{{Old: strings.Repeat("a", 40), New: strings.Repeat("b", 40), Ref: "refs/heads/main"}}
	if err := store.Stage("repo", objects, first); err != nil {
		t.Fatal(err)
	}
	if err := store.Stage("repo", objects, second); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CommitBatch("repo", [][]RefUpdate{first, second}); err != nil {
		t.Fatal(err)
	}
	recovered, _, err := store.certificates.Recover("repo")
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Generation != 2 || len(recovered.Entries) != 2 || recovered.Refs[first[0].Ref] != second[0].New {
		t.Fatalf("batched certificate recovered the wrong state: %#v", recovered)
	}
	primaryClient.mu.Lock()
	certificateCount := 0
	for key := range primaryClient.objects {
		if strings.Contains(key, "/.walgit-certificates/") {
			certificateCount++
		}
	}
	primaryClient.mu.Unlock()
	if certificateCount != 2 {
		t.Fatalf("batched commit wrote %d certificates, want genesis plus one batch", certificateCount)
	}
}

func TestCertificatePublishFailsWhenAnAuthorityLostItsParent(t *testing.T) {
	primaryClient := newMemoryS3()
	secondaryClient := newMemoryS3()
	store := &S3Store{
		client: primaryClient, bucket: "primary", prefix: "walgit", timeout: time.Second,
		unconditionalWrites: true, staged: make(map[string]stagedS3Transaction),
	}
	primary := s3BlobStore{store: store}
	secondaryStore := &S3Store{client: secondaryClient, bucket: "secondary", prefix: "walgit", timeout: time.Second}
	secondary := s3BlobStore{store: secondaryStore}
	store.blobs = replicatedBlobStore{stores: []blobStore{primary, secondary}}
	store.certificates = replicatedCertificateStore{authorities: []certificateAuthority{primary, secondary}}
	if err := store.Initialize("repo", "refs/heads/main", "sha1"); err != nil {
		t.Fatal(err)
	}
	objects := t.TempDir()
	first := []RefUpdate{{Old: strings.Repeat("0", 40), New: strings.Repeat("a", 40), Ref: "refs/heads/main"}}
	if err := store.Stage("repo", objects, first); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Commit("repo", first); err != nil {
		t.Fatal(err)
	}

	secondaryClient.mu.Lock()
	for key := range secondaryClient.objects {
		if strings.Contains(key, "/.walgit-certificates/") && strings.Contains(key, "/00000000000000000001-") {
			delete(secondaryClient.objects, key)
		}
	}
	secondaryClient.mu.Unlock()
	second := []RefUpdate{{Old: strings.Repeat("a", 40), New: strings.Repeat("b", 40), Ref: "refs/heads/main"}}
	if err := store.Stage("repo", objects, second); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Commit("repo", second); err == nil || !strings.Contains(err.Error(), "parent certificate") {
		t.Fatalf("commit ignored a missing parent certificate: %v", err)
	}
}

func TestS3CertificatesRecoverFromSecondaryOnly(t *testing.T) {
	primaryClient := newMemoryS3()
	secondaryClient := newMemoryS3()
	store := &S3Store{
		client: primaryClient, bucket: "primary", prefix: "walgit", timeout: time.Second,
		unconditionalWrites: true, staged: make(map[string]stagedS3Transaction),
	}
	primary := s3BlobStore{store: store}
	secondaryStore := &S3Store{client: secondaryClient, bucket: "secondary", prefix: "walgit", timeout: time.Second}
	secondary := s3BlobStore{store: secondaryStore}
	store.blobs = replicatedBlobStore{stores: []blobStore{primary, secondary}}
	store.certificates = replicatedCertificateStore{authorities: []certificateAuthority{primary, secondary}}
	if err := store.Initialize("repo", "refs/heads/main", "sha1"); err != nil {
		t.Fatal(err)
	}
	objects := filepath.Join(t.TempDir(), "objects")
	if err := os.MkdirAll(filepath.Join(objects, "pack"), 0o755); err != nil {
		t.Fatal(err)
	}
	payload := []byte("small payload forced through replicated blobs")
	if err := os.WriteFile(filepath.Join(objects, "pack", "objects.pack"), payload, 0o444); err != nil {
		t.Fatal(err)
	}
	updates := []RefUpdate{{Old: strings.Repeat("0", 40), New: strings.Repeat("a", 40), Ref: "refs/heads/main"}}
	if err := store.Stage("repo", objects, updates); err != nil {
		t.Fatal(err)
	}
	entry, _, err := store.Commit("repo", updates)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Descriptor == nil {
		t.Fatal("small strict-mode transaction was not externalized")
	}

	primaryClient.mu.Lock()
	clear(primaryClient.objects)
	primaryClient.mu.Unlock()
	store.cachedManifest = nil

	manifest, err := store.Load("repo")
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Generation != 1 {
		t.Fatalf("secondary certificate recovery returned %#v", manifest)
	}
	if store.cachedManifest != nil {
		t.Fatal("survivor-only recovery was cached as writable coordinator state")
	}
	replayed := filepath.Join(t.TempDir(), "replayed")
	if _, err := store.ReplayManifest("repo", 0, manifest, replayed, func(ManifestEntry, EntryMeta) error { return nil }); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(replayed, "pack", "objects.pack"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("S3 secondary recovery changed the payload")
	}
}

func TestDualAuthorityConfigurationRequiresSecondary(t *testing.T) {
	t.Setenv("WALGIT_REQUIRE_DUAL_AUTHORITY", "true")
	t.Setenv("WALGIT_BLOB_SECONDARY_STORE", "")
	if _, err := Open(filepath.Join(t.TempDir(), "store")); err == nil || !strings.Contains(err.Error(), "SECONDARY_STORE") {
		t.Fatalf("dual-authority mode opened without a secondary: %v", err)
	}
}

func TestDualAuthorityConfigurationEnablesCertificates(t *testing.T) {
	primary := filepath.Join(t.TempDir(), "primary")
	secondary := filepath.Join(t.TempDir(), "secondary")
	t.Setenv("WALGIT_REQUIRE_DUAL_AUTHORITY", "true")
	t.Setenv("WALGIT_BLOB_SECONDARY_STORE", secondary)
	backend, err := Open(primary)
	if err != nil {
		t.Fatal(err)
	}
	store := backend.(Store)
	if store.certificates == nil {
		t.Fatal("dual-authority configuration did not enable certificates")
	}
}

func TestLegacyCertificateAnchorRefusesFreshReplay(t *testing.T) {
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
	primary := filesystemBlobStore{store: store}
	secondary := filesystemBlobStore{store: Store{Root: filepath.Join(t.TempDir(), "secondary")}}
	store.blobs = replicatedBlobStore{stores: []blobStore{primary, secondary}}
	store.certificates = replicatedCertificateStore{authorities: []certificateAuthority{primary, secondary}}
	if err := store.Initialize(id, "refs/heads/main", "sha1"); err != nil {
		t.Fatal(err)
	}
	manifest, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.CertificateFloor != 1 {
		t.Fatalf("legacy recovery floor = %d, want 1", manifest.CertificateFloor)
	}
	if _, err := store.ReplayFrom(id, 0, filepath.Join(t.TempDir(), "fresh"), func(ManifestEntry, EntryMeta) error { return nil }); err == nil || !strings.Contains(err.Error(), "recover") {
		t.Fatalf("fresh replay crossed an unprotected legacy history: %v", err)
	}
}

func TestS3DualAuthorityRequiresSingleWriter(t *testing.T) {
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("WALGIT_REQUIRE_DUAL_AUTHORITY", "true")
	t.Setenv("WALGIT_BLOB_SECONDARY_STORE", "s3://secondary/walgit")
	t.Setenv("WALGIT_S3_DISABLE_CONDITIONAL_WRITES", "false")
	if _, err := OpenS3("s3://primary/walgit"); err == nil || !strings.Contains(err.Error(), "one repository writer") {
		t.Fatalf("S3 dual-authority mode accepted uncoordinated writers: %v", err)
	}
}

func dualFilesystemStore(t *testing.T) (Store, filesystemBlobStore, filesystemBlobStore) {
	t.Helper()
	primaryStore := Store{Root: filepath.Join(t.TempDir(), "primary")}
	primary := filesystemBlobStore{store: primaryStore}
	secondary := filesystemBlobStore{store: Store{Root: filepath.Join(t.TempDir(), "secondary")}}
	primaryStore.blobs = replicatedBlobStore{stores: []blobStore{primary, secondary}}
	primaryStore.certificates = replicatedCertificateStore{authorities: []certificateAuthority{primary, secondary}}
	return primaryStore, primary, secondary
}

func certificateTransaction(t *testing.T, store Store) (string, string, []RefUpdate) {
	t.Helper()
	id := "repo"
	if err := store.Initialize(id, "refs/heads/main", "sha1"); err != nil {
		t.Fatal(err)
	}
	objects := filepath.Join(t.TempDir(), "objects")
	if err := os.MkdirAll(filepath.Join(objects, "pack"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(objects, "pack", "objects.pack"), []byte("certificate objects"), 0o444); err != nil {
		t.Fatal(err)
	}
	updates := []RefUpdate{{Old: strings.Repeat("0", 40), New: strings.Repeat("a", 40), Ref: "refs/heads/main"}}
	return id, objects, updates
}

type failingPutCertificateAuthority struct {
	certificateAuthority
}

func (f failingPutCertificateAuthority) PutCertificate(string, uint64, string, []byte) error {
	return errors.New("injected certificate write failure")
}
