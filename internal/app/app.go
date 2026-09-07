package app

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"walgit/internal/wal"
)

func Init(repo, store, id string) error {
	if repo == "" || store == "" || id == "" {
		return errors.New("init requires -repo, -store, and -id")
	}
	absRepo, err := filepath.Abs(repo)
	if err != nil {
		return err
	}
	storeLocation, err := normalizeStoreLocation(store)
	if err != nil {
		return err
	}
	backend, err := wal.Open(storeLocation)
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(absRepo, "HEAD")); errors.Is(err, os.ErrNotExist) {
		if err := run("", "git", "init", "--bare", "--initial-branch=main", absRepo); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if err := run("", "git", "-C", absRepo, "config", "receive.unpackLimit", "1"); err != nil {
		return err
	}
	if err := run("", "git", "-C", absRepo, "config", "receive.autogc", "false"); err != nil {
		return err
	}
	if err := run("", "git", "-C", absRepo, "config", "gc.auto", "0"); err != nil {
		return err
	}
	if err := run("", "git", "-C", absRepo, "config", "maintenance.auto", "false"); err != nil {
		return err
	}
	// A generation marker is only useful if Git's corresponding objects and
	// references survive the same crash. Force real fsync semantics (including
	// on macOS, where Git may otherwise select writeout-only) for data Git has
	// committed before walgit persists that marker.
	if err := run("", "git", "-C", absRepo, "config", "core.fsync", "committed"); err != nil {
		return err
	}
	if err := run("", "git", "-C", absRepo, "config", "core.fsyncMethod", "fsync"); err != nil {
		return err
	}
	if err := run("", "git", "-C", absRepo, "config", "http.receivepack", "true"); err != nil {
		return err
	}
	head, err := commandOutput("", "git", "-C", absRepo, "symbolic-ref", "HEAD")
	if err != nil {
		return err
	}
	objectFormat, err := commandOutput("", "git", "-C", absRepo, "rev-parse", "--show-object-format")
	if err != nil {
		return err
	}
	if err := backend.Initialize(id, head, objectFormat); err != nil {
		return err
	}
	if err := writeLocalState(absRepo, localState{RepositoryID: id}); err != nil {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	if override := os.Getenv("WALGIT_EXECUTABLE"); override != "" {
		exe, err = filepath.Abs(override)
		if err != nil {
			return err
		}
	}
	hookEnv := "#!/bin/sh\n" +
		"export WALGIT_STORE=" + shellQuote(storeLocation) + "\n" +
		"export WALGIT_REPO_ID=" + shellQuote(id) + "\n" +
		"export WALGIT_REPO=" + shellQuote(absRepo) + "\n"
	preHook := hookEnv + "exec " + shellQuote(exe) + " hook pre-receive\n"
	refHook := hookEnv + "exec " + shellQuote(exe) + " hook reference-transaction \"$@\"\n"
	if err := os.WriteFile(filepath.Join(absRepo, "hooks", "pre-receive"), []byte(preHook), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(absRepo, "hooks", "reference-transaction"), []byte(refHook), 0o755); err != nil {
		return err
	}
	_, err = backend.CleanupPending(id, time.Now().Add(-24*time.Hour))
	return err
}

func PreReceive(input io.Reader) error {
	store := os.Getenv("WALGIT_STORE")
	id := os.Getenv("WALGIT_REPO_ID")
	objects := os.Getenv("GIT_QUARANTINE_PATH")
	if store == "" || id == "" || objects == "" {
		return errors.New("hook environment is incomplete")
	}
	updates, err := readUpdates(input)
	if err != nil {
		return err
	}
	if len(updates) == 0 {
		return nil
	}
	if socket := os.Getenv(writerSocketEnvironment); socket != "" {
		_, err := callWriter(socket, writerRequest{Operation: "stage", ObjectDir: objects, Updates: updates})
		return err
	}
	backend, err := wal.Open(store)
	if err != nil {
		return err
	}
	return backend.Stage(id, objects, updates)
}

func ReferenceTransaction(state string, input io.Reader) error {
	store := os.Getenv("WALGIT_STORE")
	id := os.Getenv("WALGIT_REPO_ID")
	if store == "" || id == "" {
		return errors.New("hook environment is incomplete")
	}
	updates, err := readReferenceUpdates(input)
	if err != nil {
		return err
	}
	if socket := os.Getenv(writerSocketEnvironment); socket != "" {
		return referenceTransactionWithWriter(socket, state, updates)
	}
	switch state {
	case "prepared":
		backend, openErr := wal.Open(store)
		if openErr != nil {
			return openErr
		}
		_, _, err = backend.Commit(id, updates)
		return err
	case "aborted":
		backend, openErr := wal.Open(store)
		if openErr != nil {
			return openErr
		}
		return backend.Abort(id, updates)
	case "committed":
		repo := os.Getenv("WALGIT_REPO")
		if repo == "" {
			return errors.New("hook repository path is missing")
		}
		backend, openErr := wal.Open(store)
		if openErr != nil {
			return openErr
		}
		if err := markCurrentIfManifestMatches(repo, store, id); err != nil {
			return err
		}
		if err := backend.Finalize(id, updates); err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("unknown reference transaction state %q", state)
	}
}

func referenceTransactionWithWriter(socket, state string, updates []wal.RefUpdate) error {
	operation := state
	if state == "prepared" {
		operation = "commit"
	} else if state == "aborted" {
		operation = "abort"
	} else if state == "committed" {
		operation = "finalize"
	}
	if operation != "commit" && operation != "abort" && operation != "finalize" {
		return fmt.Errorf("unknown reference transaction state %q", state)
	}
	response, err := callWriter(socket, writerRequest{Operation: operation, Updates: updates})
	if err != nil {
		return err
	}
	if state != "committed" {
		return nil
	}
	repo := os.Getenv("WALGIT_REPO")
	id := os.Getenv("WALGIT_REPO_ID")
	if repo == "" || id == "" {
		return errors.New("hook repository path is missing")
	}
	return markCurrentFromManifest(repo, id, response.Manifest)
}

func Restore(repo, store, id string) error {
	if repo == "" || store == "" || id == "" {
		return errors.New("restore requires -repo, -store, and -id")
	}
	absRepo, err := filepath.Abs(repo)
	if err != nil {
		return err
	}
	backend, err := wal.Open(store)
	if err != nil {
		return err
	}
	m, err := backend.Load(id)
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(absRepo, "HEAD")); errors.Is(err, os.ErrNotExist) {
		args := []string{"init", "--bare", "--initial-branch=main"}
		if m.ObjectFormat == "sha256" {
			args = append(args, "--object-format=sha256")
		}
		args = append(args, absRepo)
		if err := run("", "git", args...); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(absRepo, stateFileName)); errors.Is(err, os.ErrNotExist) {
		if err := writeLocalState(absRepo, localState{RepositoryID: id}); err != nil {
			return err
		}
	}
	return Reconcile(absRepo, store, id)
}

func Gateway(repo, store, id, service string, stdin io.Reader, stdout, stderr io.Writer) error {
	if repo == "" || store == "" || id == "" {
		return errors.New("gateway requires a repository path, -store, and -id")
	}
	if service != "upload-pack" && service != "receive-pack" {
		return fmt.Errorf("unsupported Git service %q", service)
	}
	if err := reconcileForGateway(repo, store, id); err != nil {
		return err
	}
	cmd := exec.Command("git-"+service, repo)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = stdin, stdout, stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("git-%s: %w", service, err)
	}
	return nil
}

func reconcileForGateway(repo, store, id string) error {
	if socket := os.Getenv(writerSocketEnvironment); socket != "" {
		response, err := callWriter(socket, writerRequest{Operation: "load"})
		if err != nil {
			return err
		}
		state, err := readLocalState(repo, id)
		if err != nil {
			return err
		}
		if state.Generation == response.Manifest.Generation {
			return nil
		}
		return reconcileWithManifest(repo, store, id, response.Manifest)
	}
	return Reconcile(repo, store, id)
}

type benchmarkResult struct {
	Pushes                  int            `json:"pushes"`
	BlobBytes               int            `json:"blob_bytes"`
	WALLatency              latencySummary `json:"wal_latency_ms"`
	PlainLatency            latencySummary `json:"plain_latency_ms"`
	WALPushesPerSecond      float64        `json:"wal_pushes_per_second"`
	PlainPushesPerSecond    float64        `json:"plain_pushes_per_second"`
	WALLatencyOverheadPct   float64        `json:"wal_mean_latency_overhead_percent"`
	WALBytes                int64          `json:"wal_bytes"`
	WALRestoreMillis        float64        `json:"wal_restore_millis"`
	CheckpointCreateMillis  float64        `json:"checkpoint_create_millis"`
	CheckpointRestoreMillis float64        `json:"checkpoint_restore_millis"`
	CheckpointBytes         int64          `json:"checkpoint_bytes"`
	RemoteHead              string         `json:"remote_head"`
	RestoredHead            string         `json:"restored_head"`
	BenchmarkFilesDirectory string         `json:"benchmark_files_dir,omitempty"`
}

type latencySummary struct {
	Mean float64 `json:"mean"`
	P50  float64 `json:"p50"`
	P90  float64 `json:"p90"`
	P99  float64 `json:"p99"`
	Max  float64 `json:"max"`
}

func Benchmark(pushes, blobBytes int, keep bool, output io.Writer) error {
	if pushes < 1 || blobBytes < 1 {
		return errors.New("pushes and blob-bytes must be positive")
	}
	root, err := os.MkdirTemp("", "walgit-bench-")
	if err != nil {
		return err
	}
	if !keep {
		defer os.RemoveAll(root)
	}
	remote := filepath.Join(root, "remote.git")
	plain := filepath.Join(root, "plain.git")
	store := filepath.Join(root, "store")
	work := filepath.Join(root, "work")
	restored := filepath.Join(root, "restored.git")
	checkpointRestored := filepath.Join(root, "checkpoint-restored.git")
	if err := Init(remote, store, "bench"); err != nil {
		return err
	}
	if err := run("", "git", "init", "--bare", plain); err != nil {
		return err
	}
	if err := run("", "git", "init", "-b", "main", work); err != nil {
		return err
	}
	for _, kv := range [][2]string{{"user.name", "walgit benchmark"}, {"user.email", "bench@walgit.invalid"}, {"commit.gpgsign", "false"}} {
		if err := run("", "git", "-C", work, "config", kv[0], kv[1]); err != nil {
			return err
		}
	}
	if err := run("", "git", "-C", work, "remote", "add", "origin", remote); err != nil {
		return err
	}
	if err := run("", "git", "-C", work, "remote", "add", "plain", plain); err != nil {
		return err
	}
	rng := rand.New(rand.NewSource(1))
	payload := make([]byte, blobBytes)
	walSamples := make([]time.Duration, 0, pushes)
	plainSamples := make([]time.Duration, 0, pushes)
	for i := 0; i < pushes; i++ {
		if _, err := rng.Read(payload); err != nil {
			return err
		}
		copy(payload, []byte(strconv.Itoa(i)+":"))
		if err := os.WriteFile(filepath.Join(work, "payload.bin"), payload, 0o644); err != nil {
			return err
		}
		if err := run("", "git", "-C", work, "add", "payload.bin"); err != nil {
			return err
		}
		if err := run("", "git", "-C", work, "commit", "-q", "-m", fmt.Sprintf("push %d", i+1)); err != nil {
			return err
		}
		started := time.Now()
		if err := run("", "git", "-C", work, "push", "-q", "plain", "HEAD:refs/heads/main"); err != nil {
			return fmt.Errorf("plain push %d: %w", i+1, err)
		}
		plainSamples = append(plainSamples, time.Since(started))
		started = time.Now()
		if err := run("", "git", "-C", work, "push", "-q", "origin", "HEAD:refs/heads/main"); err != nil {
			return fmt.Errorf("push %d: %w", i+1, err)
		}
		walSamples = append(walSamples, time.Since(started))
	}
	restoreStarted := time.Now()
	if err := Restore(restored, store, "bench"); err != nil {
		return err
	}
	restoreElapsed := time.Since(restoreStarted)
	if err := run("", "git", "-C", restored, "fsck", "--strict"); err != nil {
		return err
	}
	remoteHead, err := commandOutput("", "git", "-C", remote, "rev-parse", "refs/heads/main")
	if err != nil {
		return err
	}
	restoredHead, err := commandOutput("", "git", "-C", restored, "rev-parse", "refs/heads/main")
	if err != nil {
		return err
	}
	if remoteHead != restoredHead {
		return fmt.Errorf("restored head %s does not match remote %s", restoredHead, remoteHead)
	}
	benchmarkStore, err := wal.Open(store)
	if err != nil {
		return err
	}
	preCompactionManifest, err := benchmarkStore.Load("bench")
	if err != nil {
		return err
	}
	var walBytes int64
	for _, entry := range preCompactionManifest.Entries {
		walBytes += entry.Bytes + entry.PayloadBytes
	}
	checkpointStarted := time.Now()
	checkpoint, err := Compact(remote, store, "bench")
	if err != nil {
		return err
	}
	checkpointElapsed := time.Since(checkpointStarted)
	checkpointRestoreStarted := time.Now()
	if err := Restore(checkpointRestored, store, "bench"); err != nil {
		return err
	}
	checkpointRestoreElapsed := time.Since(checkpointRestoreStarted)
	if err := run("", "git", "-C", checkpointRestored, "fsck", "--strict"); err != nil {
		return err
	}
	checkpointHead, err := commandOutput("", "git", "-C", checkpointRestored, "rev-parse", "refs/heads/main")
	if err != nil {
		return err
	}
	if checkpointHead != remoteHead {
		return fmt.Errorf("checkpoint-restored head %s does not match remote %s", checkpointHead, remoteHead)
	}
	walLatency := summarizeLatencies(walSamples)
	plainLatency := summarizeLatencies(plainSamples)
	walElapsed := sumDurations(walSamples)
	plainElapsed := sumDurations(plainSamples)
	result := benchmarkResult{
		Pushes:                  pushes,
		BlobBytes:               blobBytes,
		WALLatency:              walLatency,
		PlainLatency:            plainLatency,
		WALPushesPerSecond:      float64(pushes) / walElapsed.Seconds(),
		PlainPushesPerSecond:    float64(pushes) / plainElapsed.Seconds(),
		WALLatencyOverheadPct:   (walLatency.Mean/plainLatency.Mean - 1) * 100,
		WALBytes:                walBytes,
		WALRestoreMillis:        float64(restoreElapsed.Microseconds()) / 1000,
		CheckpointCreateMillis:  float64(checkpointElapsed.Microseconds()) / 1000,
		CheckpointRestoreMillis: float64(checkpointRestoreElapsed.Microseconds()) / 1000,
		CheckpointBytes:         checkpoint.Bytes + checkpoint.PayloadBytes,
		RemoteHead:              remoteHead,
		RestoredHead:            restoredHead,
	}
	if keep {
		result.BenchmarkFilesDirectory = root
	}
	enc := json.NewEncoder(output)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

func summarizeLatencies(samples []time.Duration) latencySummary {
	values := append([]time.Duration(nil), samples...)
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	percentile := func(p float64) float64 {
		index := int(float64(len(values)-1)*p + 0.5)
		return float64(values[index].Microseconds()) / 1000
	}
	return latencySummary{
		Mean: float64(sumDurations(values).Microseconds()) / 1000 / float64(len(values)),
		P50:  percentile(0.50),
		P90:  percentile(0.90),
		P99:  percentile(0.99),
		Max:  percentile(1),
	}
}

func sumDurations(samples []time.Duration) time.Duration {
	var total time.Duration
	for _, sample := range samples {
		total += sample
	}
	return total
}

func readUpdates(r io.Reader) ([]wal.RefUpdate, error) {
	return readUpdatesAllowingHEAD(r, false)
}

func readReferenceUpdates(r io.Reader) ([]wal.RefUpdate, error) {
	return readUpdatesAllowingHEAD(r, true)
}

func readUpdatesAllowingHEAD(r io.Reader, allowHEAD bool) ([]wal.RefUpdate, error) {
	var updates []wal.RefUpdate
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())
		validOIDLength := len(parts) == 3 && (len(parts[0]) == 40 || len(parts[0]) == 64) && len(parts[1]) == len(parts[0])
		if !validOIDLength {
			return nil, fmt.Errorf("invalid reference-transaction input %q", scanner.Text())
		}
		if allowHEAD && parts[2] == "HEAD" {
			continue
		}
		if !strings.HasPrefix(parts[2], "refs/") {
			return nil, fmt.Errorf("invalid reference-transaction input %q", scanner.Text())
		}
		updates = append(updates, wal.RefUpdate{Old: parts[0], New: parts[1], Ref: parts[2]})
	}
	return updates, scanner.Err()
}

func run(dir, name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

func commandOutput(dir, name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }

func normalizeStoreLocation(location string) (string, error) {
	if strings.Contains(location, "://") {
		return location, nil
	}
	return filepath.Abs(location)
}
