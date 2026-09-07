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
	"time"

	"walgit/internal/wal"
)

type dualAuthorityBenchmarkResult struct {
	Version                   int                   `json:"version"`
	Pushes                    int                   `json:"pushes"`
	BlobBytes                 int                   `json:"blob_bytes"`
	DurabilityMode            string                `json:"durability_mode"`
	RawGitLatency             latencySummary        `json:"raw_git_latency_ms"`
	SingleAuthorityLatency    latencySummary        `json:"single_authority_latency_ms"`
	ReplicatedBlobLatency     latencySummary        `json:"replicated_blob_latency_ms"`
	DualAuthorityLatency      latencySummary        `json:"dual_authority_latency_ms"`
	SingleOverRawPercent      float64               `json:"single_authority_over_raw_percent"`
	ReplicationOverSinglePct  float64               `json:"blob_replication_over_single_percent"`
	CertificateOverReplicaPct float64               `json:"certificate_commit_over_replication_percent"`
	ReplicationAddedMeanMs    float64               `json:"blob_replication_added_mean_millis"`
	CertificateAddedMeanMs    float64               `json:"certificate_commit_added_mean_millis"`
	ScrubMillis               float64               `json:"dual_authority_scrub_millis"`
	Scrub                     wal.ScrubReport       `json:"scrub"`
	PrimaryRestore            DisasterRestoreReport `json:"primary_restore"`
	SecondaryRestore          DisasterRestoreReport `json:"secondary_restore"`
	SingleWriter              WriterStats           `json:"single_authority_writer"`
	ReplicatedBlobWriter      WriterStats           `json:"replicated_blob_writer"`
	DualWriter                WriterStats           `json:"dual_authority_writer"`
	CleanupComplete           bool                  `json:"cleanup_complete"`
	PrimaryIsolatedPrefix     string                `json:"primary_isolated_prefix,omitempty"`
	SecondaryIsolatedPrefix   string                `json:"secondary_isolated_prefix,omitempty"`
	BenchmarkFilesDirectory   string                `json:"benchmark_files_dir,omitempty"`
}

// BenchmarkDualAuthorities compares local raw Git, one S3 authority,
// replicated blobs, and a strict two-authority certificate commit. Every
// durable write is confined beneath a fresh cryptographically random child
// prefix. The result also proves that either authority can independently
// reconstruct and fsck the repository.
func BenchmarkDualAuthorities(primaryBase, secondaryBase string, pushes, blobBytes int, allowUnprotected, keep bool, output io.Writer) (returnErr error) {
	if pushes < 1 || blobBytes < 1 {
		return errors.New("pushes and blob-bytes must be positive")
	}
	if !strings.HasPrefix(primaryBase, "s3://") || !strings.HasPrefix(secondaryBase, "s3://") {
		return errors.New("dual-authority benchmark requires two s3:// base URIs")
	}
	if strings.TrimRight(primaryBase, "/") == strings.TrimRight(secondaryBase, "/") {
		return errors.New("primary and secondary benchmark stores must be different locations")
	}
	if !allowUnprotected && !keep {
		return errors.New("protected benchmark objects may be retention-locked; use -keep or explicitly select -allow-unprotected")
	}
	primaryRun, secondaryRun, err := isolatedBenchmarkPrefixes(primaryBase, secondaryBase)
	if err != nil {
		return err
	}
	primarySingle := primaryRun + "/single"
	primaryReplicated := primaryRun + "/replicated"
	secondaryReplicated := secondaryRun + "/replicated"
	primaryDual := primaryRun + "/dual"
	secondaryDual := secondaryRun + "/dual"
	cleanupNeeded := !keep
	cleanupComplete := !cleanupNeeded
	if cleanupNeeded {
		defer func() {
			if !cleanupNeeded {
				return
			}
			primaryErr := wal.DeleteS3BenchmarkPrefixWithRole(primaryRun, false)
			secondaryErr := wal.DeleteS3BenchmarkPrefixWithRole(secondaryRun, true)
			if returnErr == nil && (primaryErr != nil || secondaryErr != nil) {
				returnErr = fmt.Errorf("clean isolated benchmark prefixes: primary=%v secondary=%v", primaryErr, secondaryErr)
			}
		}()
	}

	root, err := os.MkdirTemp("", "walgit-dual-bench-")
	if err != nil {
		return err
	}
	if !keep {
		defer os.RemoveAll(root) //nolint:errcheck
	}
	restoreEnvironment := preserveEnvironment(
		"WALGIT_BLOB_SECONDARY_STORE", "WALGIT_REQUIRE_BLOB_REPLICATION", "WALGIT_REQUIRE_DUAL_AUTHORITY",
		"WALGIT_S3_DISABLE_CONDITIONAL_WRITES", "WALGIT_ALLOW_UNPROTECTED_S3", writerSocketEnvironment,
	)
	defer restoreEnvironment()
	if err := os.Setenv("WALGIT_S3_DISABLE_CONDITIONAL_WRITES", "true"); err != nil {
		return err
	}

	plain := filepath.Join(root, "plain.git")
	singleRepo := filepath.Join(root, "single.git")
	replicatedRepo := filepath.Join(root, "replicated.git")
	dualRepo := filepath.Join(root, "dual.git")
	work := filepath.Join(root, "work")
	if err := run("", "git", "init", "--bare", "--initial-branch=main", plain); err != nil {
		return err
	}
	setSingleAuthorityEnvironment()
	if err := Init(singleRepo, primarySingle, "bench"); err != nil {
		return fmt.Errorf("initialize single-authority benchmark: %w", err)
	}
	singleSocket := filepath.Join(root, "single-writer.sock")
	singleWriter, err := StartWriter(singleSocket, primarySingle, "bench", 0, 64)
	if err != nil {
		return err
	}
	singleClosed := false
	defer func() {
		if !singleClosed {
			_ = singleWriter.Close()
		}
	}()
	if err := setReplicatedBlobEnvironment(secondaryReplicated); err != nil {
		return err
	}
	if err := Init(replicatedRepo, primaryReplicated, "bench"); err != nil {
		return fmt.Errorf("initialize replicated-blob benchmark: %w", err)
	}
	replicatedSocket := filepath.Join(root, "replicated-writer.sock")
	replicatedWriter, err := StartWriter(replicatedSocket, primaryReplicated, "bench", 0, 64)
	if err != nil {
		return err
	}
	replicatedClosed := false
	defer func() {
		if !replicatedClosed {
			_ = replicatedWriter.Close()
		}
	}()

	if err := setDualAuthorityEnvironment(secondaryDual, allowUnprotected); err != nil {
		return err
	}
	if err := Init(dualRepo, primaryDual, "bench"); err != nil {
		return fmt.Errorf("initialize dual-authority benchmark: %w", err)
	}
	dualSocket := filepath.Join(root, "dual-writer.sock")
	dualWriter, err := StartWriter(dualSocket, primaryDual, "bench", 0, 64)
	if err != nil {
		return err
	}
	dualClosed := false
	defer func() {
		if !dualClosed {
			_ = dualWriter.Close()
		}
	}()

	if err := run("", "git", "init", "-b", "main", work); err != nil {
		return err
	}
	for _, kv := range [][2]string{{"user.name", "walgit benchmark"}, {"user.email", "bench@walgit.invalid"}, {"commit.gpgsign", "false"}} {
		if err := run("", "git", "-C", work, "config", kv[0], kv[1]); err != nil {
			return err
		}
	}

	rng := rand.New(rand.NewSource(3))
	payload := make([]byte, blobBytes)
	rawSamples := make([]time.Duration, 0, pushes)
	singleSamples := make([]time.Duration, 0, pushes)
	replicatedSamples := make([]time.Duration, 0, pushes)
	dualSamples := make([]time.Duration, 0, pushes)
	for index := 0; index < pushes; index++ {
		if _, err := rng.Read(payload); err != nil {
			return err
		}
		copy(payload, []byte(strconv.Itoa(index)+":"))
		if err := os.WriteFile(filepath.Join(work, "payload.bin"), payload, 0o644); err != nil {
			return err
		}
		for _, args := range [][]string{{"-C", work, "add", "payload.bin"}, {"-C", work, "commit", "-q", "-m", fmt.Sprintf("durability push %d", index+1)}} {
			if err := run("", "git", args...); err != nil {
				return err
			}
		}
		started := time.Now()
		if err := run("", "git", "-C", work, "push", "-q", plain, "HEAD:refs/heads/main"); err != nil {
			return fmt.Errorf("raw Git push %d: %w", index+1, err)
		}
		rawSamples = append(rawSamples, time.Since(started))
		if err := os.Setenv(writerSocketEnvironment, singleSocket); err != nil {
			return err
		}
		started = time.Now()
		if err := run("", "git", "-C", work, "push", "-q", singleRepo, "HEAD:refs/heads/main"); err != nil {
			return fmt.Errorf("single-authority push %d: %w", index+1, err)
		}
		singleSamples = append(singleSamples, time.Since(started))
		if err := os.Setenv(writerSocketEnvironment, replicatedSocket); err != nil {
			return err
		}
		started = time.Now()
		if err := run("", "git", "-C", work, "push", "-q", replicatedRepo, "HEAD:refs/heads/main"); err != nil {
			return fmt.Errorf("replicated-blob push %d: %w", index+1, err)
		}
		replicatedSamples = append(replicatedSamples, time.Since(started))
		if err := os.Setenv(writerSocketEnvironment, dualSocket); err != nil {
			return err
		}
		started = time.Now()
		if err := run("", "git", "-C", work, "push", "-q", dualRepo, "HEAD:refs/heads/main"); err != nil {
			return fmt.Errorf("dual-authority push %d: %w", index+1, err)
		}
		dualSamples = append(dualSamples, time.Since(started))
	}
	if err := singleWriter.Close(); err != nil {
		return err
	}
	singleClosed = true
	if err := replicatedWriter.Close(); err != nil {
		return err
	}
	replicatedClosed = true
	if err := dualWriter.Close(); err != nil {
		return err
	}
	dualClosed = true
	_ = os.Unsetenv(writerSocketEnvironment)
	if err := setDualAuthorityEnvironment(secondaryDual, allowUnprotected); err != nil {
		return err
	}

	scrubStarted := time.Now()
	scrub, err := wal.Scrub(primaryDual, "bench")
	scrubMillis := milliseconds(time.Since(scrubStarted))
	if err != nil {
		return fmt.Errorf("post-benchmark dual-authority scrub: %w", err)
	}
	primaryRestore, err := DisasterRestoreDrill(primaryDual, "bench", "primary", false)
	if err != nil {
		return fmt.Errorf("primary disaster restore drill: %w", err)
	}
	secondaryRestore, err := DisasterRestoreDrill(primaryDual, "bench", "secondary", false)
	if err != nil {
		return fmt.Errorf("secondary disaster restore drill: %w", err)
	}

	if cleanupNeeded {
		primaryErr := wal.DeleteS3BenchmarkPrefixWithRole(primaryRun, false)
		secondaryErr := wal.DeleteS3BenchmarkPrefixWithRole(secondaryRun, true)
		if primaryErr != nil || secondaryErr != nil {
			return fmt.Errorf("clean isolated benchmark prefixes: primary=%v secondary=%v", primaryErr, secondaryErr)
		}
		cleanupNeeded = false
		cleanupComplete = true
	}
	rawLatency := summarizeLatencies(rawSamples)
	singleLatency := summarizeLatencies(singleSamples)
	replicatedLatency := summarizeLatencies(replicatedSamples)
	dualLatency := summarizeLatencies(dualSamples)
	mode := "object-lock-enforced"
	if allowUnprotected {
		mode = "UNPROTECTED_BENCHMARK_ONLY"
	}
	result := dualAuthorityBenchmarkResult{
		Version: 1, Pushes: pushes, BlobBytes: blobBytes, DurabilityMode: mode,
		RawGitLatency: rawLatency, SingleAuthorityLatency: singleLatency,
		ReplicatedBlobLatency: replicatedLatency, DualAuthorityLatency: dualLatency,
		SingleOverRawPercent:      (singleLatency.Mean/rawLatency.Mean - 1) * 100,
		ReplicationOverSinglePct:  (replicatedLatency.Mean/singleLatency.Mean - 1) * 100,
		CertificateOverReplicaPct: (dualLatency.Mean/replicatedLatency.Mean - 1) * 100,
		ReplicationAddedMeanMs:    replicatedLatency.Mean - singleLatency.Mean,
		CertificateAddedMeanMs:    dualLatency.Mean - replicatedLatency.Mean,
		ScrubMillis:               scrubMillis, Scrub: scrub, PrimaryRestore: primaryRestore, SecondaryRestore: secondaryRestore,
		SingleWriter: singleWriter.Stats(), ReplicatedBlobWriter: replicatedWriter.Stats(),
		DualWriter: dualWriter.Stats(), CleanupComplete: cleanupComplete,
	}
	if keep {
		result.PrimaryIsolatedPrefix = primaryRun
		result.SecondaryIsolatedPrefix = secondaryRun
		result.BenchmarkFilesDirectory = root
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

func isolatedBenchmarkPrefixes(primaryBase, secondaryBase string) (string, string, error) {
	random := make([]byte, 16)
	if _, err := cryptorand.Read(random); err != nil {
		return "", "", err
	}
	segment := fmt.Sprintf("walgit-benchmark-%s-%x", time.Now().UTC().Format("20060102T150405Z"), random)
	return strings.TrimRight(primaryBase, "/") + "/" + segment, strings.TrimRight(secondaryBase, "/") + "/" + segment, nil
}

func setSingleAuthorityEnvironment() {
	_ = os.Unsetenv("WALGIT_BLOB_SECONDARY_STORE")
	_ = os.Unsetenv("WALGIT_REQUIRE_BLOB_REPLICATION")
	_ = os.Unsetenv("WALGIT_REQUIRE_DUAL_AUTHORITY")
	_ = os.Unsetenv("WALGIT_ALLOW_UNPROTECTED_S3")
	_ = os.Unsetenv(writerSocketEnvironment)
}

func setDualAuthorityEnvironment(secondary string, allowUnprotected bool) error {
	for key, value := range map[string]string{
		"WALGIT_BLOB_SECONDARY_STORE": secondary, "WALGIT_REQUIRE_BLOB_REPLICATION": "true",
		"WALGIT_REQUIRE_DUAL_AUTHORITY": "true", "WALGIT_S3_DISABLE_CONDITIONAL_WRITES": "true",
	} {
		if err := os.Setenv(key, value); err != nil {
			return err
		}
	}
	if allowUnprotected {
		return os.Setenv("WALGIT_ALLOW_UNPROTECTED_S3", "true")
	}
	return os.Unsetenv("WALGIT_ALLOW_UNPROTECTED_S3")
}

func setReplicatedBlobEnvironment(secondary string) error {
	if err := os.Setenv("WALGIT_BLOB_SECONDARY_STORE", secondary); err != nil {
		return err
	}
	if err := os.Setenv("WALGIT_REQUIRE_BLOB_REPLICATION", "true"); err != nil {
		return err
	}
	_ = os.Unsetenv("WALGIT_REQUIRE_DUAL_AUTHORITY")
	_ = os.Unsetenv("WALGIT_ALLOW_UNPROTECTED_S3")
	_ = os.Unsetenv(writerSocketEnvironment)
	return nil
}

func preserveEnvironment(names ...string) func() {
	type priorValue struct {
		value string
		set   bool
	}
	prior := make(map[string]priorValue, len(names))
	for _, name := range names {
		value, set := os.LookupEnv(name)
		prior[name] = priorValue{value: value, set: set}
	}
	return func() {
		for _, name := range names {
			value := prior[name]
			if value.set {
				_ = os.Setenv(name, value.value)
			} else {
				_ = os.Unsetenv(name)
			}
		}
	}
}
