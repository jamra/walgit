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

func TestS3AbortPublishesCompensatingTransaction(t *testing.T) {
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
	if manifest.Generation != 2 || len(manifest.Entries) != 2 || len(manifest.Refs) != 0 || len(manifest.Prepared) != 0 {
		t.Fatalf("unexpected compensated S3 manifest: %#v", manifest)
	}
	var replayed []RefUpdate
	_, err = store.ReplayFrom("repo", 0, filepath.Join(t.TempDir(), "objects"), func(_ ManifestEntry, meta EntryMeta) error {
		replayed = append(replayed, meta.Updates...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 2 || replayed[1].New != updates[0].Old {
		t.Fatalf("unexpected S3 rollback replay: %#v", replayed)
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
	if err := store.Abort("repo", third); err != nil {
		t.Fatal(err)
	}
	rolledBack, err := store.Load("repo")
	if err != nil {
		t.Fatal(err)
	}
	if rolledBack.Generation != 4 || rolledBack.Refs["refs/heads/c"] != "" {
		t.Fatalf("coordinated abort did not append compensation: %#v", rolledBack)
	}
	if err := store.Stage("repo", objects, third); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Commit("repo", third); err != nil {
		t.Fatal(err)
	}
	if err := store.Abort("repo", third); err != nil {
		t.Fatal(err)
	}
	repeated, err := store.Load("repo")
	if err != nil {
		t.Fatal(err)
	}
	if repeated.Generation != 6 || repeated.Refs["refs/heads/c"] != "" {
		t.Fatalf("repeated coordinated abort did not compensate again: %#v", repeated)
	}
}

func TestS3ConcurrentCommitsUseManifestCAS(t *testing.T) {
	store := &S3Store{client: newMemoryS3(), bucket: "bucket", prefix: "walgit", timeout: time.Second}
	if err := store.Initialize("repo", "refs/heads/main", "sha1"); err != nil {
		t.Fatal(err)
	}
	objects := t.TempDir()
	const writers = 12
	updates := make([][]RefUpdate, writers)
	for i := range updates {
		oid := strings.Repeat(string("abcdef"[i%6]), 40)
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
	if m.Generation != writers || len(m.Refs) != writers {
		t.Fatalf("S3 CAS lost updates: %#v", m)
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
	if client.transactionContentLengthSet {
		t.Fatal("transaction upload unexpectedly required a precomputed content length")
	}
	if client.transactionChecksum != types.ChecksumAlgorithmSha256 {
		t.Fatalf("checksum algorithm = %q, want SHA256", client.transactionChecksum)
	}
}

func TestAWSSDKAcceptsUnseekableStreamingArchive(t *testing.T) {
	var received int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPut {
			http.Error(response, "unexpected method", http.StatusMethodNotAllowed)
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
	transactionChecksum         types.ChecksumAlgorithm
	failTransactionAfterStore   bool
	corruptTransactionChecksum  bool
}

func (s *observingS3) PutObject(ctx context.Context, input *s3.PutObjectInput, options ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	transaction := strings.Contains(aws.ToString(input.Key), "/transactions/")
	if transaction {
		_, s.transactionBodyWasFile = input.Body.(*os.File)
		s.transactionContentLengthSet = input.ContentLength != nil
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
