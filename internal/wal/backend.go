package wal

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"
)

type Backend interface {
	Initialize(repoID, head, objectFormat string) error
	Stage(repoID, objectDir string, updates []RefUpdate) error
	Commit(repoID string, updates []RefUpdate) (ManifestEntry, Manifest, error)
	Finalize(repoID string, updates []RefUpdate) error
	Abort(repoID string, updates []RefUpdate) error
	Load(repoID string) (Manifest, error)
	ReplayFrom(repoID string, generation uint64, gitObjects string, apply func(ManifestEntry, EntryMeta) error) (Manifest, error)
	CreateCheckpoint(repoID, gitObjects string, expectedGeneration uint64) (Checkpoint, error)
	RestoreCheckpoint(repoID, gitObjects string) (CheckpointMeta, error)
	CleanupPending(repoID string, olderThan time.Time) (int, error)
	GarbageCollect(repoID string, olderThan time.Time) (GCResult, error)
}

// BatchCommitter publishes already-staged reference transactions in one
// authoritative manifest update. The writer coordinator falls back to Commit
// when a backend does not implement this optimization.
type BatchCommitter interface {
	CommitBatch(repoID string, batches [][]RefUpdate) ([]ManifestEntry, Manifest, error)
}

// ManifestReplayer applies entries from a manifest that a trusted coordinator
// already loaded, avoiding redundant manifest reads on a catching-up replica.
type ManifestReplayer interface {
	ReplayManifest(repoID string, generation uint64, manifest Manifest, gitObjects string, apply func(ManifestEntry, EntryMeta) error) (Manifest, error)
}

func Open(location string) (Backend, error) {
	if location == "" {
		return nil, fmt.Errorf("storage location is required")
	}
	if strings.HasPrefix(location, "s3://") {
		return OpenS3(location)
	}
	if strings.HasPrefix(location, "file://") {
		u, err := url.Parse(location)
		if err != nil {
			return nil, err
		}
		if u.Host != "" && u.Host != "localhost" {
			return nil, fmt.Errorf("file storage URI cannot have host %q", u.Host)
		}
		location = u.Path
	}
	root, err := filepath.Abs(location)
	if err != nil {
		return nil, err
	}
	return Store{Root: root}, nil
}
