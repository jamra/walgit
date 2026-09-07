package app

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"walgit/internal/wal"
)

type MaintenanceResult struct {
	Compacted        bool           `json:"compacted"`
	Checkpoint       wal.Checkpoint `json:"checkpoint,omitempty"`
	Repacked         bool           `json:"repacked"`
	GarbageCollected wal.GCResult   `json:"garbage_collected"`
	Evicted          bool           `json:"evicted"`
}

func (h *gitHTTPHandler) runMaintenance() (result MaintenanceResult, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.metrics.mu.Lock()
	h.metrics.maintenanceRuns++
	h.metrics.mu.Unlock()
	defer func() {
		if err != nil {
			h.metrics.mu.Lock()
			h.metrics.maintenanceFailures++
			h.metrics.mu.Unlock()
		}
	}()
	if err := h.metrics.scrubHealthError(); err != nil {
		return result, err
	}
	if err := recoverCacheEviction(h.options.Repository, h.options.Store, h.options.RepositoryID); err != nil {
		return result, err
	}
	backend, err := wal.Open(h.options.Store)
	if err != nil {
		return result, err
	}
	manifest, err := backend.Load(h.options.RepositoryID)
	if err != nil {
		return result, err
	}
	entryBytes := int64(0)
	for _, entry := range manifest.Entries {
		entryBytes += entry.Bytes + entry.PayloadBytes
	}
	compact := h.options.CompactAfterEntries > 0 && len(manifest.Entries) >= h.options.CompactAfterEntries
	compact = compact || h.options.CompactAfterBytes > 0 && entryBytes >= h.options.CompactAfterBytes
	if compact {
		checkpoint, compactErr := Compact(h.options.Repository, h.options.Store, h.options.RepositoryID)
		if compactErr != nil {
			return result, compactErr
		}
		result.Compacted = true
		result.Checkpoint = checkpoint
		if repackErr := run("", "git", "-C", h.options.Repository, "repack", "-Ad"); repackErr != nil {
			return result, fmt.Errorf("repack serving cache: %w", repackErr)
		}
		if pruneErr := run("", "git", "-C", h.options.Repository, "prune-packed"); pruneErr != nil {
			return result, fmt.Errorf("prune packed cache objects: %w", pruneErr)
		}
		result.Repacked = true
		h.metrics.mu.Lock()
		h.metrics.compactions++
		h.metrics.mu.Unlock()
	}
	gc, err := GarbageCollect(h.options.Store, h.options.RepositoryID, h.options.GCGrace)
	if err != nil {
		return result, err
	}
	result.GarbageCollected = gc
	h.metrics.mu.Lock()
	h.metrics.garbageCollections++
	h.metrics.mu.Unlock()

	if h.options.IdleCacheAfter > 0 {
		lastActivity := time.Unix(0, h.metrics.lastActivity.Load())
		if time.Since(lastActivity) >= h.options.IdleCacheAfter {
			if err := evictCache(h.options.Repository, h.options.Store, h.options.RepositoryID); err != nil {
				return result, err
			}
			result.Evicted = true
			h.metrics.lastActivity.Store(time.Now().UnixNano())
			h.metrics.mu.Lock()
			h.metrics.evictions++
			h.metrics.mu.Unlock()
		}
	}
	return result, nil
}

func evictCache(repo, store, id string) error {
	if err := recoverCacheEviction(repo, store, id); err != nil {
		return err
	}
	backup := evictionBackupPath(repo)
	if err := os.Rename(repo, backup); err != nil {
		return fmt.Errorf("move cache aside for eviction: %w", err)
	}
	if err := Init(repo, store, id); err != nil {
		_ = os.RemoveAll(repo)
		if rollbackErr := os.Rename(backup, repo); rollbackErr != nil {
			return fmt.Errorf("initialize empty cache: %v; restore old cache: %w", err, rollbackErr)
		}
		return fmt.Errorf("initialize empty cache: %w", err)
	}
	if err := os.RemoveAll(backup); err != nil {
		return fmt.Errorf("remove evicted cache: %w", err)
	}
	return nil
}

func recoverCacheEviction(repo, store, id string) error {
	backup := evictionBackupPath(repo)
	_, backupErr := os.Stat(backup)
	if errors.Is(backupErr, os.ErrNotExist) {
		return nil
	}
	if backupErr != nil {
		return backupErr
	}
	_, repoErr := os.Stat(filepath.Join(repo, "HEAD"))
	if errors.Is(repoErr, os.ErrNotExist) {
		if err := os.RemoveAll(repo); err != nil {
			return err
		}
		return os.Rename(backup, repo)
	}
	if repoErr != nil {
		return repoErr
	}
	// Complete an initialization interrupted after the old cache was renamed.
	if err := Init(repo, store, id); err != nil {
		return err
	}
	return os.RemoveAll(backup)
}

func evictionBackupPath(repo string) string {
	return filepath.Clean(repo) + ".walgit-evicting"
}
