package app

import (
	cryptorand "crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"walgit/internal/wal"
)

type multiNodeBenchmarkResult struct {
	Nodes                      int                       `json:"nodes"`
	SequentialPushes           int                       `json:"sequential_pushes"`
	ConcurrentWriters          int                       `json:"concurrent_writers"`
	BlobBytes                  int                       `json:"blob_bytes"`
	PushLatency                latencySummary            `json:"rotating_push_latency_ms"`
	PushLatencyByNode          map[string]latencySummary `json:"push_latency_by_node_ms"`
	CrossNodeReadLatency       latencySummary            `json:"cross_node_read_after_write_latency_ms"`
	ConcurrentWriteLatency     latencySummary            `json:"concurrent_write_latency_ms"`
	ConcurrentWriteBatchMillis float64                   `json:"concurrent_write_batch_millis"`
	FinalGeneration            uint64                    `json:"final_generation"`
	FinalReferenceCount        int                       `json:"final_reference_count"`
	WALBytes                   int64                     `json:"wal_bytes"`
	FinalHead                  string                    `json:"final_head"`
	Storage                    string                    `json:"storage"`
	S3StorageClass             string                    `json:"s3_storage_class,omitempty"`
	ConsistencyMode            string                    `json:"consistency_mode"`
	StoreLocation              string                    `json:"store_location,omitempty"`
	CleanupComplete            bool                      `json:"cleanup_complete"`
	Writer                     *WriterStats              `json:"writer,omitempty"`
	BenchmarkFilesDirectory    string                    `json:"benchmark_files_dir,omitempty"`
}

func BenchmarkNodes(nodes, pushes, blobBytes int, keep bool, output io.Writer) error {
	return BenchmarkNodesAtStore(nodes, pushes, blobBytes, "", keep, output)
}

func BenchmarkNodesAtStore(nodes, pushes, blobBytes int, storeBase string, keep bool, output io.Writer) (returnErr error) {
	if nodes < 2 {
		return errors.New("multi-node benchmark requires at least two nodes")
	}
	if nodes > 32 {
		return errors.New("multi-node benchmark supports at most 32 nodes")
	}
	if pushes < 1 || blobBytes < 1 {
		return errors.New("pushes and blob-bytes must be positive")
	}
	root, err := os.MkdirTemp("", "walgit-multinode-bench-")
	if err != nil {
		return err
	}
	if !keep {
		defer os.RemoveAll(root)
	}
	store := filepath.Join(root, "store")
	storage := "filesystem"
	s3StorageClass := ""
	consistencyMode := "conditional-manifest-cas"
	cleanupComplete := true
	cleanupNeeded := false
	if storeBase != "" {
		if !strings.HasPrefix(storeBase, "s3://") {
			return errors.New("external benchmark store must use an s3:// URI")
		}
		random := make([]byte, 16)
		if _, err := cryptorand.Read(random); err != nil {
			return err
		}
		store = strings.TrimRight(storeBase, "/") + fmt.Sprintf("/walgit-benchmark-%s-%x", time.Now().UTC().Format("20060102T150405Z"), random)
		storage = "s3"
		s3StorageClass = classifyS3StorageClass(storeBase)
		cleanupComplete = false
		cleanupNeeded = !keep
		defer func() {
			if !cleanupNeeded {
				return
			}
			if err := wal.DeleteS3BenchmarkPrefix(store); err != nil && returnErr == nil {
				returnErr = fmt.Errorf("clean up isolated benchmark prefix: %w", err)
			}
		}()
	}
	concurrentWriterCount := nodes
	var writerHandle *WriterHandle
	if disabled, _ := strconv.ParseBool(os.Getenv("WALGIT_S3_DISABLE_CONDITIONAL_WRITES")); storage == "s3" && disabled {
		concurrentWriterCount = 1
		consistencyMode = "externally-serialized-single-writer"
	}
	if enabled, _ := strconv.ParseBool(os.Getenv("WALGIT_BENCH_PERSISTENT_WRITER")); enabled {
		socket := filepath.Join(root, "writer.sock")
		writerHandle, err = StartWriter(socket, store, "multinode", 5*time.Millisecond, 64)
		if err != nil {
			return err
		}
		defer writerHandle.Close() //nolint:errcheck
		previousSocket, hadSocket := os.LookupEnv(writerSocketEnvironment)
		if err := os.Setenv(writerSocketEnvironment, socket); err != nil {
			return err
		}
		defer func() {
			if hadSocket {
				_ = os.Setenv(writerSocketEnvironment, previousSocket)
			} else {
				_ = os.Unsetenv(writerSocketEnvironment)
			}
		}()
		concurrentWriterCount = nodes
		consistencyMode = "coordinated-single-writer"
	}
	nodePaths := make([]string, nodes)
	for i := range nodePaths {
		nodePaths[i] = filepath.Join(root, fmt.Sprintf("node-%02d.git", i))
		if err := Init(nodePaths[i], store, "multinode"); err != nil {
			return err
		}
	}
	work := filepath.Join(root, "work")
	if err := run("", "git", "init", "-b", "main", work); err != nil {
		return err
	}
	for _, kv := range [][2]string{{"user.name", "walgit benchmark"}, {"user.email", "bench@walgit.invalid"}, {"commit.gpgsign", "false"}} {
		if err := run("", "git", "-C", work, "config", kv[0], kv[1]); err != nil {
			return err
		}
	}
	executable, err := benchmarkExecutable()
	if err != nil {
		return err
	}
	receiveGateway := shellQuote(executable) + " gateway -store " + shellQuote(store) + " -id multinode -service receive-pack"
	uploadGateway := shellQuote(executable) + " gateway -store " + shellQuote(store) + " -id multinode -service upload-pack"
	rng := rand.New(rand.NewSource(2))
	payload := make([]byte, blobBytes)
	pushSamples := make([]time.Duration, 0, pushes)
	readSamples := make([]time.Duration, 0, pushes)
	perNodeSamples := make([][]time.Duration, nodes)
	var finalHead string
	for i := 0; i < pushes; i++ {
		if _, err := rng.Read(payload); err != nil {
			return err
		}
		copy(payload, []byte(strconv.Itoa(i)+":"))
		if err := os.WriteFile(filepath.Join(work, "payload.bin"), payload, 0o644); err != nil {
			return err
		}
		for _, args := range [][]string{{"-C", work, "add", "payload.bin"}, {"-C", work, "commit", "-q", "-m", fmt.Sprintf("rotating push %d", i+1)}} {
			if err := run("", "git", args...); err != nil {
				return err
			}
		}
		writer := i % nodes
		if writerHandle != nil {
			writer = 0
		}
		started := time.Now()
		if err := run("", "git", "-C", work, "push", "-q", "--receive-pack="+receiveGateway, nodePaths[writer], "HEAD:refs/heads/main"); err != nil {
			return fmt.Errorf("node %d push %d: %w", writer, i+1, err)
		}
		elapsed := time.Since(started)
		pushSamples = append(pushSamples, elapsed)
		perNodeSamples[writer] = append(perNodeSamples[writer], elapsed)
		finalHead, err = commandOutput("", "git", "-C", work, "rev-parse", "HEAD")
		if err != nil {
			return err
		}
		reader := (writer + 1) % nodes
		started = time.Now()
		advertisement, err := commandOutput("", "git", "ls-remote", "--upload-pack="+uploadGateway, nodePaths[reader], "refs/heads/main")
		if err != nil {
			return fmt.Errorf("node %d cross-node read after push %d: %w", reader, i+1, err)
		}
		readSamples = append(readSamples, time.Since(started))
		if advertisement != finalHead+"\trefs/heads/main" {
			return fmt.Errorf("node %d advertised %q after push %d, want %s", reader, advertisement, i+1, finalHead)
		}
	}
	for _, node := range nodePaths {
		if err := Reconcile(node, store, "multinode"); err != nil {
			return err
		}
	}
	writers := make([]string, concurrentWriterCount)
	for i := range writers {
		writers[i] = filepath.Join(root, fmt.Sprintf("writer-%02d", i))
		if err := run("", "git", "clone", "-q", "--no-local", work, writers[i]); err != nil {
			return err
		}
		for _, kv := range [][2]string{{"user.name", "walgit benchmark"}, {"user.email", "bench@walgit.invalid"}, {"commit.gpgsign", "false"}} {
			if err := run("", "git", "-C", writers[i], "config", kv[0], kv[1]); err != nil {
				return err
			}
		}
		fileName := fmt.Sprintf("concurrent-%02d.txt", i)
		if err := os.WriteFile(filepath.Join(writers[i], fileName), []byte(fmt.Sprintf("writer %d\n", i)), 0o644); err != nil {
			return err
		}
		for _, args := range [][]string{{"-C", writers[i], "add", fileName}, {"-C", writers[i], "commit", "-q", "-m", fmt.Sprintf("concurrent writer %d", i)}} {
			if err := run("", "git", args...); err != nil {
				return err
			}
		}
	}
	concurrentSamples := make([]time.Duration, concurrentWriterCount)
	concurrentErrors := make([]error, concurrentWriterCount)
	start := make(chan struct{})
	var group sync.WaitGroup
	batchStarted := time.Now()
	for i := range writers {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			<-start
			started := time.Now()
			branch := fmt.Sprintf("HEAD:refs/heads/concurrent/%02d", i)
			targetNode := i
			if writerHandle != nil {
				targetNode = 0
			}
			concurrentErrors[i] = run("", "git", "-C", writers[i], "push", "-q", "--receive-pack="+receiveGateway, nodePaths[targetNode], branch)
			concurrentSamples[i] = time.Since(started)
		}(i)
	}
	close(start)
	group.Wait()
	batchElapsed := time.Since(batchStarted)
	for i, err := range concurrentErrors {
		if err != nil {
			return fmt.Errorf("concurrent writer %d: %w", i, err)
		}
	}
	for i, node := range nodePaths {
		if err := Reconcile(node, store, "multinode"); err != nil {
			return fmt.Errorf("final reconcile node %d: %w", i, err)
		}
		if err := run("", "git", "-C", node, "fsck", "--strict"); err != nil {
			return fmt.Errorf("verify node %d: %w", i, err)
		}
	}
	backend, err := wal.Open(store)
	if err != nil {
		return err
	}
	manifest, err := backend.Load("multinode")
	if err != nil {
		return err
	}
	wantGeneration := uint64(pushes + concurrentWriterCount)
	if manifest.Generation != wantGeneration {
		return fmt.Errorf("final generation %d, want %d", manifest.Generation, wantGeneration)
	}
	if len(manifest.Refs) != concurrentWriterCount+1 {
		return fmt.Errorf("final reference count %d, want %d", len(manifest.Refs), concurrentWriterCount+1)
	}
	if manifest.Refs["refs/heads/main"] != finalHead {
		return fmt.Errorf("final main %s, want %s", manifest.Refs["refs/heads/main"], finalHead)
	}
	var walBytes int64
	for _, entry := range manifest.Entries {
		walBytes += entry.Bytes + entry.PayloadBytes
	}
	byNode := make(map[string]latencySummary, nodes)
	for i, samples := range perNodeSamples {
		if len(samples) > 0 {
			byNode[fmt.Sprintf("node-%02d", i)] = summarizeLatencies(samples)
		}
	}
	result := multiNodeBenchmarkResult{
		Nodes: nodes, SequentialPushes: pushes, ConcurrentWriters: concurrentWriterCount, BlobBytes: blobBytes,
		PushLatency: summarizeLatencies(pushSamples), PushLatencyByNode: byNode,
		CrossNodeReadLatency: summarizeLatencies(readSamples), ConcurrentWriteLatency: summarizeLatencies(concurrentSamples),
		ConcurrentWriteBatchMillis: float64(batchElapsed.Microseconds()) / 1000,
		FinalGeneration:            manifest.Generation, FinalReferenceCount: len(manifest.Refs), WALBytes: walBytes, FinalHead: finalHead,
		Storage: storage, S3StorageClass: s3StorageClass, ConsistencyMode: consistencyMode, CleanupComplete: cleanupComplete,
	}
	if writerHandle != nil {
		stats := writerHandle.Stats()
		result.Writer = &stats
	}
	if storage == "s3" {
		result.StoreLocation = store
	}
	if cleanupNeeded {
		if err := wal.DeleteS3BenchmarkPrefix(store); err != nil {
			return fmt.Errorf("clean up isolated benchmark prefix: %w", err)
		}
		cleanupNeeded = false
		result.CleanupComplete = true
		result.StoreLocation = ""
	}
	if keep {
		result.BenchmarkFilesDirectory = root
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

func classifyS3StorageClass(location string) string {
	bucket, _, _ := strings.Cut(strings.TrimPrefix(location, "s3://"), "/")
	if strings.HasSuffix(bucket, "--x-s3") {
		return "express-one-zone"
	}
	return "standard-or-general-purpose"
}

func benchmarkExecutable() (string, error) {
	if override := os.Getenv("WALGIT_EXECUTABLE"); override != "" {
		return filepath.Abs(override)
	}
	return os.Executable()
}
