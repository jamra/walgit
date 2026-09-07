package wal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

func TestS3BackendConformance(t *testing.T) {
	client := newMemoryS3()
	store := &S3Store{client: client, bucket: "bucket", prefix: "walgit", timeout: time.Second}
	if err := store.Initialize("repo", "refs/heads/main", "sha1"); err != nil {
		t.Fatal(err)
	}
	objects := filepath.Join(t.TempDir(), "objects")
	if err := os.MkdirAll(filepath.Join(objects, "pack"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(objects, "pack", "test.pack"), []byte("object data"), 0o644); err != nil {
		t.Fatal(err)
	}
	updates := []RefUpdate{{Old: strings.Repeat("0", 40), New: strings.Repeat("a", 40), Ref: "refs/heads/main"}}
	if err := store.Stage("repo", objects, updates); err != nil {
		t.Fatal(err)
	}
	entry, manifest, err := store.Commit("repo", updates)
	if err != nil {
		t.Fatal(err)
	}
	if entry.Generation != 1 || manifest.Refs[updates[0].Ref] != updates[0].New {
		t.Fatalf("unexpected S3 manifest: %#v", manifest)
	}
	if retry, retried, err := store.Commit("repo", updates); err != nil {
		t.Fatal(err)
	} else if retry.Generation != 1 || retried.Generation != 1 || len(retried.Entries) != 1 {
		t.Fatalf("S3 commit was not idempotent: %#v %#v", retry, retried)
	}
	if err := store.Finalize("repo", updates); err != nil {
		t.Fatal(err)
	}
	replayed := filepath.Join(t.TempDir(), "objects")
	if _, err := store.ReplayFrom("repo", 0, replayed, func(got ManifestEntry, meta EntryMeta) error {
		if got.TransactionID != entry.TransactionID || len(meta.Updates) != 1 {
			t.Fatalf("unexpected replay metadata: %#v %#v", got, meta)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := store.CreateCheckpoint("repo", objects, 1)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Generation != 1 {
		t.Fatalf("unexpected checkpoint: %#v", checkpoint)
	}
	checkpointObjects := filepath.Join(t.TempDir(), "objects")
	meta, err := store.RestoreCheckpoint("repo", checkpointObjects)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Refs[updates[0].Ref] != updates[0].New {
		t.Fatalf("checkpoint refs do not match: %#v", meta.Refs)
	}
	gc, err := store.GarbageCollect("repo", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if gc.Entries != 1 || gc.Checkpoints != 0 {
		t.Fatalf("unexpected S3 garbage collection: %#v", gc)
	}
}

func TestS3AbortDoesNotRewriteCommittedIndex(t *testing.T) {
	store := &S3Store{client: newMemoryS3(), bucket: "bucket", prefix: "walgit", timeout: time.Second}
	if err := store.Initialize("repo", "refs/heads/main", "sha1"); err != nil {
		t.Fatal(err)
	}
	objects := t.TempDir()
	updates := []RefUpdate{{Old: strings.Repeat("0", 40), New: strings.Repeat("a", 40), Ref: "refs/heads/main"}}
	if err := store.Stage("repo", objects, updates); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Commit("repo", updates); err != nil {
		t.Fatal(err)
	}
	if err := store.Abort("repo", updates); err != nil {
		t.Fatal(err)
	}
	if err := store.Abort("repo", updates); err != nil {
		t.Fatalf("abort retry was not idempotent: %v", err)
	}
	manifest, err := store.Load("repo")
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Generation != 1 || len(manifest.Entries) != 1 || manifest.Refs[updates[0].Ref] != updates[0].New || len(manifest.Prepared) != 0 {
		t.Fatalf("abort changed the authoritative S3 index: %#v", manifest)
	}
	var replayed []RefUpdate
	_, err = store.ReplayFrom("repo", 0, filepath.Join(t.TempDir(), "objects"), func(_ ManifestEntry, meta EntryMeta) error {
		replayed = append(replayed, meta.Updates...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 1 || replayed[0].New != updates[0].New {
		t.Fatalf("unexpected S3 replay after local abort: %#v", replayed)
	}
}

func TestS3DeleteRepositoryIsConfinedToRepositoryPrefix(t *testing.T) {
	client := newMemoryS3()
	store := &S3Store{client: client, bucket: "bucket", prefix: "walgit-benchmark-test", timeout: time.Second}
	for _, id := range []string{"target", "preserve"} {
		if err := store.Initialize(id, "refs/heads/main", "sha1"); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.deleteRepository("target"); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	for key := range client.objects {
		if strings.HasPrefix(key, store.repoPrefix("target")) {
			t.Fatalf("target object survived cleanup: %s", key)
		}
	}
	if _, ok := client.objects[store.key("preserve", "manifest.json")]; !ok {
		t.Fatal("cleanup removed an adjacent repository")
	}
}

func TestS3DeleteBenchmarkPrefixIncludesGlobalObjectsButNotAdjacentPrefixes(t *testing.T) {
	client := newMemoryS3()
	store := &S3Store{client: client, bucket: "bucket", prefix: "benchmarks/walgit-benchmark-random", timeout: time.Second}
	client.objects["benchmarks/walgit-benchmark-random/repo/manifest.json"] = memoryS3Object{data: []byte("manifest")}
	client.objects["benchmarks/walgit-benchmark-random/.walgit-blobs/sha256/aa"] = memoryS3Object{data: []byte("blob")}
	client.objects["benchmarks/walgit-benchmark-random-adjacent/repo/manifest.json"] = memoryS3Object{data: []byte("preserve")}
	if err := store.deletePrefix(); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	for key := range client.objects {
		if strings.HasPrefix(key, "benchmarks/walgit-benchmark-random/") {
			t.Fatalf("benchmark object survived cleanup: %s", key)
		}
	}
	if _, ok := client.objects["benchmarks/walgit-benchmark-random-adjacent/repo/manifest.json"]; !ok {
		t.Fatal("cleanup removed an adjacent prefix")
	}
}

func TestS3BenchmarkCleanupRequiresGeneratedChildPrefix(t *testing.T) {
	for _, location := range []string{"s3://bucket", "s3://bucket/production", "file:///tmp/walgit-benchmark-test", "s3://bucket/walgit-benchmark-"} {
		if err := validateS3BenchmarkLocation(location); err == nil {
			t.Fatalf("cleanup accepted unsafe location %q", location)
		}
	}
	if err := validateS3BenchmarkLocation("s3://bucket/bench/walgit-benchmark-20260906T120000Z-0123456789abcdef0123456789abcdef"); err != nil {
		t.Fatalf("cleanup rejected isolated location: %v", err)
	}
}

func TestS3ExpressDirectoryBucketRejectsPathStyle(t *testing.T) {
	t.Setenv("WALGIT_S3_PATH_STYLE", "true")
	_, err := openS3Single("s3://benchmark--usw2-az1--x-s3/walgit", false)
	if err == nil || !strings.Contains(err.Error(), "virtual-hosted-style") {
		t.Fatalf("directory bucket accepted path-style requests: %v", err)
	}
}

func TestS3ExpressBenchmarkCleanupSkipsUnsupportedVersionListing(t *testing.T) {
	client := &directoryMemoryS3{memoryS3: newMemoryS3()}
	store := &S3Store{
		client: client, bucket: "benchmark--usw2-az1--x-s3",
		prefix: "bench/walgit-benchmark-20260907T120000Z-0123456789abcdef0123456789abcdef", timeout: time.Second,
	}
	client.objects[store.key("repo", "manifest.json")] = memoryS3Object{data: []byte("manifest")}
	if err := store.deletePrefix(); err != nil {
		t.Fatal(err)
	}
	if client.versionCalls != 0 {
		t.Fatalf("directory bucket cleanup made %d version-list calls", client.versionCalls)
	}
	if len(client.objects) != 0 {
		t.Fatalf("directory bucket cleanup left objects: %#v", client.objects)
	}
}

func TestS3SingleWriterInitializationDoesNotReplaceExistingManifest(t *testing.T) {
	store := &S3Store{
		client: newMemoryS3(), bucket: "bucket", prefix: "walgit", timeout: time.Second,
		unconditionalWrites: true,
	}
	if err := store.Initialize("repo", "refs/heads/main", "sha1"); err != nil {
		t.Fatal(err)
	}
	objects := t.TempDir()
	updates := []RefUpdate{{Old: strings.Repeat("0", 40), New: strings.Repeat("a", 40), Ref: "refs/heads/main"}}
	if err := store.Stage("repo", objects, updates); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Commit("repo", updates); err != nil {
		t.Fatal(err)
	}
	if err := store.Finalize("repo", updates); err != nil {
		t.Fatal(err)
	}
	if err := store.Initialize("repo", "refs/heads/main", "sha1"); err != nil {
		t.Fatal(err)
	}
	manifest, err := store.Load("repo")
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Generation != 1 || manifest.Refs["refs/heads/main"] != updates[0].New {
		t.Fatalf("single-writer initialization replaced existing state: %#v", manifest)
	}
}

func TestS3CoordinatedBatchUsesOneManifestWriteAndCachedMetadata(t *testing.T) {
	client := newMemoryS3()
	store := &S3Store{
		client: client, bucket: "bucket", prefix: "walgit", timeout: time.Second,
		unconditionalWrites: true, staged: make(map[string]stagedS3Transaction),
	}
	if err := store.Initialize("repo", "refs/heads/main", "sha1"); err != nil {
		t.Fatal(err)
	}
	objects := t.TempDir()
	zero := strings.Repeat("0", 40)
	batches := [][]RefUpdate{
		{{Old: zero, New: strings.Repeat("a", 40), Ref: "refs/heads/a"}},
		{{Old: zero, New: strings.Repeat("b", 40), Ref: "refs/heads/b"}},
	}
	for _, updates := range batches {
		if err := store.Stage("repo", objects, updates); err != nil {
			t.Fatal(err)
		}
	}
	client.resetCalls()
	entries, manifest, err := store.CommitBatch("repo", batches)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || manifest.Generation != 2 || len(manifest.Prepared) != 0 {
		t.Fatalf("unexpected coordinated batch: %#v %#v", entries, manifest)
	}
	if put, get, head := client.calls(); put != 1 || get != 1 || head != 0 {
		t.Fatalf("cold batch calls: put=%d get=%d head=%d", put, get, head)
	}

	third := []RefUpdate{{Old: zero, New: strings.Repeat("c", 40), Ref: "refs/heads/c"}}
	if err := store.Stage("repo", objects, third); err != nil {
		t.Fatal(err)
	}
	client.resetCalls()
	if _, _, err := store.Commit("repo", third); err != nil {
		t.Fatal(err)
	}
	if put, get, head := client.calls(); put != 1 || get != 0 || head != 0 {
		t.Fatalf("warm commit calls: put=%d get=%d head=%d", put, get, head)
	}
	client.resetCalls()
	if err := store.Finalize("repo", third); err != nil {
		t.Fatal(err)
	}
	if put, get, head := client.calls(); put != 0 || get != 0 || head != 0 {
		t.Fatalf("coordinated finalize performed I/O: put=%d get=%d head=%d", put, get, head)
	}
	client.resetCalls()
	if err := store.Abort("repo", third); err != nil {
		t.Fatal(err)
	}
	rolledBack, err := store.Load("repo")
	if err != nil {
		t.Fatal(err)
	}
	if rolledBack.Generation != 3 || rolledBack.Refs["refs/heads/c"] != third[0].New {
		t.Fatalf("abort changed the authoritative index: %#v", rolledBack)
	}
	if put, _, _ := client.calls(); put != 0 {
		t.Fatalf("abort performed %d S3 writes, want zero", put)
	}
}

func TestS3ConcurrentCommitsUseManifestCAS(t *testing.T) {
	client := newMemoryS3()
	initializer := &S3Store{client: client, bucket: "bucket", prefix: "walgit", timeout: time.Second}
	if err := initializer.Initialize("repo", "refs/heads/main", "sha1"); err != nil {
		t.Fatal(err)
	}
	objects := t.TempDir()
	const writers = 12
	updates := make([][]RefUpdate, writers)
	stores := make([]*S3Store, writers)
	for i := range updates {
		oid := strings.Repeat(string("abcdef"[i%6]), 40)
		updates[i] = []RefUpdate{{Old: strings.Repeat("0", 40), New: oid, Ref: "refs/heads/branch-" + string(rune('a'+i))}}
		stores[i] = &S3Store{client: client, bucket: "bucket", prefix: "walgit", timeout: time.Second}
		if err := stores[i].Stage("repo", objects, updates[i]); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := range updates {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := stores[i].Commit("repo", updates[i])
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
	m, err := initializer.Load("repo")
	if err != nil {
		t.Fatal(err)
	}
	if m.Generation != writers || len(m.Refs) != writers {
		t.Fatalf("S3 CAS lost updates: %#v", m)
	}
}

func TestS3CommitUsesOneAuthoritativeIndexPublication(t *testing.T) {
	client := newMemoryS3()
	store := &S3Store{client: client, bucket: "bucket", prefix: "walgit", timeout: time.Second}
	if err := store.Initialize("repo", "refs/heads/main", "sha1"); err != nil {
		t.Fatal(err)
	}
	updates := []RefUpdate{{Old: strings.Repeat("0", 40), New: strings.Repeat("a", 40), Ref: "refs/heads/main"}}
	if err := store.Stage("repo", t.TempDir(), updates); err != nil {
		t.Fatal(err)
	}
	client.resetCalls()
	if _, manifest, err := store.Commit("repo", updates); err != nil {
		t.Fatal(err)
	} else if manifest.Generation != 1 || len(manifest.Prepared) != 0 {
		t.Fatalf("unexpected committed index: %#v", manifest)
	}
	if put, get, head := client.calls(); put != 1 || get != 1 || head != 0 {
		t.Fatalf("commit calls: put=%d get=%d head=%d, want one GET and one CAS PUT", put, get, head)
	}
	client.resetCalls()
	if err := store.Finalize("repo", updates); err != nil {
		t.Fatal(err)
	}
	if put, get, head := client.calls(); put != 0 || get != 0 || head != 0 {
		t.Fatalf("finalize performed remote I/O: put=%d get=%d head=%d", put, get, head)
	}
}

func TestS3CrashBoundariesLeaveIndexRecoverable(t *testing.T) {
	for _, testCase := range []struct {
		name               string
		failpoint          string
		stage              bool
		wantWAL            bool
		wantGeneration     uint64
		wantCommitResponse bool
	}{
		{name: "before WAL upload", failpoint: "s3.stage.before_wal_upload", stage: true},
		{name: "after WAL upload", failpoint: "s3.stage.after_wal_upload", stage: true, wantWAL: true},
		{name: "before index CAS", failpoint: "s3.commit.before_index_cas", wantWAL: true},
		{name: "after index CAS", failpoint: "s3.commit.after_index_cas", wantWAL: true, wantGeneration: 1, wantCommitResponse: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			client := newMemoryS3()
			store := &S3Store{client: client, bucket: "bucket", prefix: "walgit", timeout: time.Second}
			if err := store.Initialize("repo", "refs/heads/main", "sha1"); err != nil {
				t.Fatal(err)
			}
			updates := []RefUpdate{{Old: strings.Repeat("0", 40), New: strings.Repeat("a", 40), Ref: "refs/heads/main"}}
			txID, err := transactionID(updates)
			if err != nil {
				t.Fatal(err)
			}
			if !testCase.stage {
				if err := store.Stage("repo", t.TempDir(), updates); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("WALGIT_FAILPOINT", testCase.failpoint)
			if testCase.stage {
				err = store.Stage("repo", t.TempDir(), updates)
			} else {
				_, _, err = store.Commit("repo", updates)
			}
			if err == nil {
				t.Fatal("injected crash boundary unexpectedly succeeded")
			}
			t.Setenv("WALGIT_FAILPOINT", "")
			client.mu.Lock()
			_, walExists := client.objects[store.key("repo", "transactions/"+txID+".wal")]
			client.mu.Unlock()
			if walExists != testCase.wantWAL {
				t.Fatalf("WAL existence=%v, want %v", walExists, testCase.wantWAL)
			}
			manifest, err := store.Load("repo")
			if err != nil {
				t.Fatal(err)
			}
			if manifest.Generation != testCase.wantGeneration {
				t.Fatalf("generation=%d, want %d", manifest.Generation, testCase.wantGeneration)
			}
			if testCase.wantCommitResponse {
				client.resetCalls()
				entry, retried, err := store.Commit("repo", updates)
				if err != nil || entry.Generation != 1 || retried.Generation != 1 {
					t.Fatalf("idempotent retry: entry=%#v manifest=%#v err=%v", entry, retried, err)
				}
				if put, _, _ := client.calls(); put != 0 {
					t.Fatalf("retry after lost success response performed %d writes", put)
				}
			}
		})
	}
}

func TestS3StageStreamsWithoutLocalFileAndRequestsSHA256(t *testing.T) {
	client := &observingS3{memoryS3: newMemoryS3()}
	store := &S3Store{client: client, bucket: "bucket", prefix: "walgit", timeout: time.Second}
	if err := store.Initialize("repo", "refs/heads/main", "sha1"); err != nil {
		t.Fatal(err)
	}
	objects := filepath.Join(t.TempDir(), "objects")
	if err := os.MkdirAll(filepath.Join(objects, "pack"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(objects, "pack", "objects.pack"), bytes.Repeat([]byte("x"), 1<<20), 0o644); err != nil {
		t.Fatal(err)
	}
	updates := []RefUpdate{{Old: strings.Repeat("0", 40), New: strings.Repeat("a", 40), Ref: "refs/heads/main"}}
	if err := store.Stage("repo", objects, updates); err != nil {
		t.Fatal(err)
	}
	if client.transactionBodyWasFile {
		t.Fatal("transaction upload used an os.File instead of the streaming pipe")
	}
	if !client.transactionContentLengthSet {
		t.Fatal("transaction upload omitted the content length required by AWS S3")
	}
	if client.transactionContentLength <= 1<<20 {
		t.Fatalf("transaction content length = %d, want tar overhead plus payload", client.transactionContentLength)
	}
	if client.transactionChecksum != types.ChecksumAlgorithmSha256 {
		t.Fatalf("checksum algorithm = %q, want SHA256", client.transactionChecksum)
	}
}

func TestPlannedArchiveLengthsMatchEncodedBytes(t *testing.T) {
	objects := filepath.Join(t.TempDir(), "objects")
	longDirectory := strings.Repeat("nested-", 16)
	objectPath := filepath.Join(objects, longDirectory, "object.pack")
	if err := os.MkdirAll(filepath.Dir(objectPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(objectPath, bytes.Repeat([]byte("payload"), 1000), 0o644); err != nil {
		t.Fatal(err)
	}
	entry := EntryMeta{
		TransactionID: strings.Repeat("a", 64), CreatedAt: time.Unix(1, 0).UTC(),
		Updates: []RefUpdate{{Old: strings.Repeat("0", 40), New: strings.Repeat("b", 40), Ref: "refs/heads/main"}},
	}
	checkpoint := CheckpointMeta{
		Generation: 1, CreatedAt: time.Unix(1, 0).UTC(), Head: "refs/heads/main",
		ObjectFormat: "sha1", Refs: map[string]string{"refs/heads/main": strings.Repeat("b", 40)},
	}

	var entryArchive bytes.Buffer
	if err := writeArchive(&entryArchive, entry, objects); err != nil {
		t.Fatal(err)
	}
	entryLength, err := entryArchiveSize(entry, objects)
	if err != nil {
		t.Fatal(err)
	}
	if entryLength != int64(entryArchive.Len()) {
		t.Fatalf("entry archive length = %d, encoded = %d", entryLength, entryArchive.Len())
	}

	var checkpointArchive bytes.Buffer
	if err := writeCheckpointArchive(&checkpointArchive, checkpoint, objects); err != nil {
		t.Fatal(err)
	}
	checkpointLength, err := checkpointArchiveSize(checkpoint, objects)
	if err != nil {
		t.Fatal(err)
	}
	if checkpointLength != int64(checkpointArchive.Len()) {
		t.Fatalf("checkpoint archive length = %d, encoded = %d", checkpointLength, checkpointArchive.Len())
	}

	var metadataArchive bytes.Buffer
	if err := writeMetadataArchive(&metadataArchive, entry); err != nil {
		t.Fatal(err)
	}
	metadataLength, err := metadataArchiveSize(entry)
	if err != nil {
		t.Fatal(err)
	}
	if metadataLength != int64(metadataArchive.Len()) {
		t.Fatalf("metadata archive length = %d, encoded = %d", metadataLength, metadataArchive.Len())
	}
}

func TestS3DefaultLargeStageKeepsOneMonolithicWALObject(t *testing.T) {
	client := newMemoryS3()
	store := &S3Store{client: client, bucket: "bucket", prefix: "walgit", timeout: time.Second}
	if err := store.Initialize("repo", "refs/heads/main", "sha1"); err != nil {
		t.Fatal(err)
	}
	objects := filepath.Join(t.TempDir(), "objects")
	if err := os.MkdirAll(filepath.Join(objects, "pack"), 0o755); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("x"), blobExternalizeThreshold)
	if err := os.WriteFile(filepath.Join(objects, "pack", "objects.pack"), payload, 0o444); err != nil {
		t.Fatal(err)
	}
	updates := []RefUpdate{{Old: strings.Repeat("0", 40), New: strings.Repeat("a", 40), Ref: "refs/heads/main"}}
	if err := store.Stage("repo", objects, updates); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	walObjects := 0
	blobObjects := 0
	for key, object := range client.objects {
		if strings.Contains(key, "/transactions/") {
			walObjects++
			if len(object.data) <= len(payload) {
				t.Fatalf("monolithic WAL bytes=%d, payload=%d", len(object.data), len(payload))
			}
		}
		if strings.Contains(key, "/.walgit-blobs/") {
			blobObjects++
		}
	}
	if walObjects != 1 || blobObjects != 0 {
		t.Fatalf("WAL objects=%d blob objects=%d, want 1 and 0", walObjects, blobObjects)
	}
}

func TestS3BlobBackedStageStoresTinyWALAndReplays(t *testing.T) {
	client := newMemoryS3()
	store := &S3Store{client: client, bucket: "bucket", prefix: "walgit", timeout: time.Second, externalizeTransactions: true}
	if err := store.Initialize("repo", "refs/heads/main", "sha1"); err != nil {
		t.Fatal(err)
	}
	objects := filepath.Join(t.TempDir(), "objects")
	if err := os.MkdirAll(filepath.Join(objects, "pack"), 0o755); err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("blob payload"), blobExternalizeThreshold/len("blob payload")+1)
	payload = payload[:blobExternalizeThreshold]
	if err := os.WriteFile(filepath.Join(objects, "pack", "objects.pack"), payload, 0o444); err != nil {
		t.Fatal(err)
	}
	updates := []RefUpdate{{Old: strings.Repeat("0", 40), New: strings.Repeat("a", 40), Ref: "refs/heads/main"}}
	if err := store.Stage("repo", objects, updates); err != nil {
		t.Fatal(err)
	}

	txID, err := transactionID(updates)
	if err != nil {
		t.Fatal(err)
	}
	transactionKey := store.key("repo", "transactions/"+txID+".wal")
	client.mu.Lock()
	transaction := client.objects[transactionKey]
	var blobKeys []string
	for key := range client.objects {
		if strings.Contains(key, "/.walgit-blobs/sha256/") {
			blobKeys = append(blobKeys, key)
		}
	}
	client.mu.Unlock()
	if len(transaction.data) >= len(payload)/100 {
		t.Fatalf("blob-backed WAL is %d bytes for a %d-byte payload", len(transaction.data), len(payload))
	}
	if len(blobKeys) != 2 {
		t.Fatalf("S3 stage stored %d blob objects, want one data chunk and one descriptor: %v", len(blobKeys), blobKeys)
	}

	entry, _, err := store.Commit("repo", updates)
	if err != nil {
		t.Fatal(err)
	}
	if entry.PayloadBytes != int64(len(payload)) {
		t.Fatalf("manifest payload bytes = %d, want %d", entry.PayloadBytes, len(payload))
	}
	replayed := filepath.Join(t.TempDir(), "objects")
	if _, err := store.ReplayFrom("repo", 0, replayed, func(ManifestEntry, EntryMeta) error { return nil }); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(replayed, "pack", "objects.pack"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("S3 blob-backed replay changed the object payload")
	}
}

func TestAWSSDKAcceptsUnseekableStreamingArchive(t *testing.T) {
	var received int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPut {
			http.Error(response, "unexpected method", http.StatusMethodNotAllowed)
			return
		}
		if request.ContentLength <= 0 {
			http.Error(response, "missing content length", http.StatusLengthRequired)
			return
		}
		var err error
		received, err = io.Copy(io.Discard, request.Body)
		if err != nil {
			http.Error(response, err.Error(), http.StatusBadRequest)
			return
		}
		response.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client := s3.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("test", "test", "")),
		HTTPClient:  server.Client(),
	}, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(server.URL)
		options.UsePathStyle = true
	})
	store := &S3Store{client: client, bucket: "bucket", prefix: "walgit", timeout: 5 * time.Second}
	updates := []RefUpdate{{Old: strings.Repeat("0", 40), New: strings.Repeat("a", 40), Ref: "refs/heads/main"}}
	if err := store.Stage("repo", t.TempDir(), updates); err != nil {
		t.Fatalf("AWS SDK rejected streaming pipe: %v", err)
	}
	if received == 0 {
		t.Fatal("test S3 endpoint received an empty request")
	}
}

func TestS3StageResolvesAmbiguousSuccessfulUpload(t *testing.T) {
	client := &observingS3{memoryS3: newMemoryS3()}
	store := &S3Store{client: client, bucket: "bucket", prefix: "walgit", timeout: time.Second}
	if err := store.Initialize("repo", "refs/heads/main", "sha1"); err != nil {
		t.Fatal(err)
	}
	client.failTransactionAfterStore = true
	objects := t.TempDir()
	updates := []RefUpdate{{Old: strings.Repeat("0", 40), New: strings.Repeat("a", 40), Ref: "refs/heads/main"}}
	if err := store.Stage("repo", objects, updates); err != nil {
		t.Fatalf("ambiguous completed upload was not resolved: %v", err)
	}
	entry, _, err := store.Commit("repo", updates)
	if err != nil {
		t.Fatal(err)
	}
	if entry.SHA256 == "" || entry.Bytes == 0 {
		t.Fatalf("resolved object metadata is incomplete: %#v", entry)
	}
}

func TestS3ColdCommitRecoversDigestForStreamedObject(t *testing.T) {
	client := newMemoryS3()
	store := &S3Store{client: client, bucket: "bucket", prefix: "walgit", timeout: time.Second}
	if err := store.Initialize("repo", "refs/heads/main", "sha1"); err != nil {
		t.Fatal(err)
	}
	objects := t.TempDir()
	updates := []RefUpdate{{Old: strings.Repeat("0", 40), New: strings.Repeat("a", 40), Ref: "refs/heads/main"}}
	if err := store.Stage("repo", objects, updates); err != nil {
		t.Fatal(err)
	}

	// A restarted coordinator has no in-memory digest. It must derive and
	// verify the immutable transaction before publishing it in the manifest.
	restarted := &S3Store{client: client, bucket: "bucket", prefix: "walgit", timeout: time.Second}
	entry, _, err := restarted.Commit("repo", updates)
	if err != nil {
		t.Fatal(err)
	}
	if entry.SHA256 == "" || entry.Bytes == 0 {
		t.Fatalf("cold commit recovered incomplete metadata: %#v", entry)
	}
}

func TestS3ColdCommitRejectsCorruptedStreamedObject(t *testing.T) {
	client := newMemoryS3()
	store := &S3Store{client: client, bucket: "bucket", prefix: "walgit", timeout: time.Second}
	if err := store.Initialize("repo", "refs/heads/main", "sha1"); err != nil {
		t.Fatal(err)
	}
	updates := []RefUpdate{{Old: strings.Repeat("0", 40), New: strings.Repeat("a", 40), Ref: "refs/heads/main"}}
	if err := store.Stage("repo", t.TempDir(), updates); err != nil {
		t.Fatal(err)
	}
	txID, err := transactionID(updates)
	if err != nil {
		t.Fatal(err)
	}
	key := store.key("repo", "transactions/"+txID+".wal")
	client.mu.Lock()
	object := client.objects[key]
	object.data[0] ^= 0xff
	client.objects[key] = object
	client.mu.Unlock()

	restarted := &S3Store{client: client, bucket: "bucket", prefix: "walgit", timeout: time.Second}
	if _, _, err := restarted.Commit("repo", updates); err == nil {
		t.Fatal("cold commit published a corrupted streamed object")
	}
}

func TestS3StageRejectsProviderChecksumMismatch(t *testing.T) {
	client := &observingS3{memoryS3: newMemoryS3(), corruptTransactionChecksum: true}
	store := &S3Store{client: client, bucket: "bucket", prefix: "walgit", timeout: time.Second}
	if err := store.Initialize("repo", "refs/heads/main", "sha1"); err != nil {
		t.Fatal(err)
	}
	updates := []RefUpdate{{Old: strings.Repeat("0", 40), New: strings.Repeat("a", 40), Ref: "refs/heads/main"}}
	if err := store.Stage("repo", t.TempDir(), updates); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("expected provider checksum mismatch, got %v", err)
	}
}

type observingS3 struct {
	*memoryS3
	transactionBodyWasFile      bool
	transactionContentLengthSet bool
	transactionContentLength    int64
	transactionChecksum         types.ChecksumAlgorithm
	failTransactionAfterStore   bool
	corruptTransactionChecksum  bool
}

func (s *observingS3) PutObject(ctx context.Context, input *s3.PutObjectInput, options ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	transaction := strings.Contains(aws.ToString(input.Key), "/transactions/")
	if transaction {
		_, s.transactionBodyWasFile = input.Body.(*os.File)
		s.transactionContentLengthSet = input.ContentLength != nil
		s.transactionContentLength = aws.ToInt64(input.ContentLength)
		s.transactionChecksum = input.ChecksumAlgorithm
	}
	out, err := s.memoryS3.PutObject(ctx, input, options...)
	if err != nil || !transaction {
		return out, err
	}
	if s.failTransactionAfterStore {
		s.failTransactionAfterStore = false
		return nil, errors.New("injected connection loss after durable store")
	}
	if s.corruptTransactionChecksum {
		out.ChecksumSHA256 = aws.String("not-the-uploaded-checksum")
	}
	return out, nil
}

type memoryS3 struct {
	mu        sync.Mutex
	objects   map[string]memoryS3Object
	putCalls  int
	getCalls  int
	headCalls int
}

type directoryMemoryS3 struct {
	*memoryS3
	versionCalls int
}

func (m *directoryMemoryS3) ListObjectVersions(_ context.Context, _ *s3.ListObjectVersionsInput, _ ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error) {
	m.versionCalls++
	return nil, errors.New("ListObjectVersions is unsupported for directory buckets")
}

type memoryS3Object struct {
	data       []byte
	etag       string
	metadata   map[string]string
	modifiedAt time.Time
}

func newMemoryS3() *memoryS3 { return &memoryS3{objects: make(map[string]memoryS3Object)} }

func (m *memoryS3) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	data, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	key := aws.ToString(input.Key)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.putCalls++
	current, exists := m.objects[key]
	if aws.ToString(input.IfNoneMatch) == "*" && exists {
		return nil, testAPIError{code: "PreconditionFailed"}
	}
	if input.IfMatch != nil && (!exists || current.etag != aws.ToString(input.IfMatch)) {
		return nil, testAPIError{code: "PreconditionFailed"}
	}
	sum := sha256.Sum256(data)
	etag := `"` + hex.EncodeToString(sum[:]) + `"`
	metadata := make(map[string]string, len(input.Metadata))
	for key, value := range input.Metadata {
		metadata[key] = value
	}
	m.objects[key] = memoryS3Object{data: append([]byte(nil), data...), etag: etag, metadata: metadata, modifiedAt: time.Now()}
	return &s3.PutObjectOutput{ETag: aws.String(etag)}, nil
}

func (m *memoryS3) GetObject(_ context.Context, input *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getCalls++
	object, ok := m.objects[aws.ToString(input.Key)]
	if !ok {
		return nil, testAPIError{code: "NoSuchKey"}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(object.data)), ETag: aws.String(object.etag)}, nil
}

func (m *memoryS3) HeadObject(_ context.Context, input *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.headCalls++
	object, ok := m.objects[aws.ToString(input.Key)]
	if !ok {
		return nil, testAPIError{code: "NotFound"}
	}
	metadata := make(map[string]string, len(object.metadata))
	for key, value := range object.metadata {
		metadata[key] = value
	}
	return &s3.HeadObjectOutput{ContentLength: aws.Int64(int64(len(object.data))), ETag: aws.String(object.etag), Metadata: metadata}, nil
}

func (m *memoryS3) resetCalls() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.putCalls, m.getCalls, m.headCalls = 0, 0, 0
}

func (m *memoryS3) calls() (put, get, head int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.putCalls, m.getCalls, m.headCalls
}

func (m *memoryS3) DeleteObject(_ context.Context, input *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, aws.ToString(input.Key))
	return &s3.DeleteObjectOutput{}, nil
}

func (m *memoryS3) ListObjectsV2(_ context.Context, input *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var keys []string
	for key := range m.objects {
		if strings.HasPrefix(key, aws.ToString(input.Prefix)) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	out := &s3.ListObjectsV2Output{IsTruncated: aws.Bool(false)}
	for _, key := range keys {
		object := m.objects[key]
		out.Contents = append(out.Contents, types.Object{Key: aws.String(key), LastModified: aws.Time(object.modifiedAt), Size: aws.Int64(int64(len(object.data)))})
	}
	return out, nil
}

type testAPIError struct{ code string }

func (e testAPIError) Error() string                 { return e.code }
func (e testAPIError) ErrorCode() string             { return e.code }
func (e testAPIError) ErrorMessage() string          { return e.code }
func (e testAPIError) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }
