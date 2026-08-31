package app

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"walgit/internal/wal"
)

const stateFileName = "walgit-state.json"
const preparedTransactionTimeout = 5 * time.Minute

type localState struct {
	RepositoryID string `json:"repository_id"`
	Generation   uint64 `json:"generation"`
}

func Reconcile(repo, storeRoot, id string) error {
	repo, err := filepath.Abs(repo)
	if err != nil {
		return err
	}
	lock, err := os.OpenFile(filepath.Join(repo, "walgit-reconcile.lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) //nolint:errcheck

	state, err := readLocalState(repo, id)
	if err != nil {
		return err
	}
	store, err := wal.Open(storeRoot)
	if err != nil {
		return err
	}
	m, err := store.Load(id)
	if err != nil {
		return err
	}
	resolved := false
	for _, prepared := range m.Prepared {
		if prepared.CreatedAt.IsZero() || time.Since(prepared.CreatedAt) < preparedTransactionTimeout {
			continue
		}
		if err := store.Finalize(id, prepared.Updates); err != nil {
			return fmt.Errorf("resolve stale prepared transaction %s: %w", prepared.TransactionID, err)
		}
		resolved = true
	}
	if resolved {
		m, err = store.Load(id)
		if err != nil {
			return err
		}
	}
	format, err := commandOutput("", "git", "-C", repo, "rev-parse", "--show-object-format")
	if err != nil {
		return err
	}
	if format != m.ObjectFormat {
		return fmt.Errorf("object format mismatch: local %s, manifest %s", format, m.ObjectFormat)
	}
	if state.Generation == m.Generation {
		return nil
	}
	if m.Checkpoint != nil && state.Generation < m.Checkpoint.Generation {
		checkpoint, err := store.RestoreCheckpoint(id, filepath.Join(repo, "objects"))
		if err != nil {
			return err
		}
		if checkpoint.ObjectFormat != m.ObjectFormat {
			return fmt.Errorf("checkpoint object format %s does not match manifest %s", checkpoint.ObjectFormat, m.ObjectFormat)
		}
		if err := replaceRefs(repo, checkpoint.Refs); err != nil {
			return err
		}
		state.Generation = checkpoint.Generation
	}
	_, err = store.ReplayFrom(id, state.Generation, filepath.Join(repo, "objects"), func(entry wal.ManifestEntry, meta wal.EntryMeta) error {
		if err := applyUpdatesIdempotently(repo, meta.Updates); err != nil {
			return err
		}
		state.Generation = entry.Generation
		return nil
	})
	if err != nil {
		return err
	}
	if m.Head != "" {
		emptyHooks := filepath.Join(repo, "walgit-no-hooks")
		if err := os.MkdirAll(emptyHooks, 0o755); err != nil {
			return err
		}
		if err := run("", "git", "-c", "core.hooksPath="+emptyHooks, "-C", repo, "symbolic-ref", "HEAD", m.Head); err != nil {
			return err
		}
	}
	if err := verifyRefs(repo, m.Refs); err != nil {
		return err
	}
	state.Generation = m.Generation
	return writeLocalState(repo, state)
}

func Compact(repo, storeRoot, id string) (wal.Checkpoint, error) {
	for attempt := 0; attempt < 3; attempt++ {
		if err := Reconcile(repo, storeRoot, id); err != nil {
			return wal.Checkpoint{}, err
		}
		store, err := wal.Open(storeRoot)
		if err != nil {
			return wal.Checkpoint{}, err
		}
		m, err := store.Load(id)
		if err != nil {
			return wal.Checkpoint{}, err
		}
		if err := verifyRefs(repo, m.Refs); err != nil {
			continue
		}
		tmp, err := os.MkdirTemp("", "walgit-checkpoint-")
		if err != nil {
			return wal.Checkpoint{}, err
		}
		snapshot := filepath.Join(tmp, "snapshot.git")
		cloneErr := run("", "git", "clone", "--bare", "--no-hardlinks", repo, snapshot)
		if cloneErr == nil {
			cloneErr = verifyRefs(snapshot, m.Refs)
		}
		if cloneErr == nil {
			cloneErr = run("", "git", "-C", snapshot, "fsck", "--strict")
		}
		var checkpoint wal.Checkpoint
		if cloneErr == nil {
			checkpoint, cloneErr = store.CreateCheckpoint(id, filepath.Join(snapshot, "objects"), m.Generation)
		}
		removeErr := os.RemoveAll(tmp)
		if cloneErr == nil && removeErr == nil {
			return checkpoint, nil
		}
		if cloneErr != nil && !strings.Contains(cloneErr.Error(), "manifest advanced during checkpoint") {
			return wal.Checkpoint{}, cloneErr
		}
		if removeErr != nil {
			return wal.Checkpoint{}, removeErr
		}
	}
	return wal.Checkpoint{}, errors.New("repository remained too busy to create a consistent checkpoint")
}

func GarbageCollect(storeRoot, id string, grace time.Duration) (wal.GCResult, error) {
	if grace < 0 {
		return wal.GCResult{}, errors.New("garbage-collection grace period cannot be negative")
	}
	store, err := wal.Open(storeRoot)
	if err != nil {
		return wal.GCResult{}, err
	}
	return store.GarbageCollect(id, time.Now().Add(-grace))
}

func replaceRefs(repo string, desired map[string]string) error {
	out, err := commandOutput("", "git", "-C", repo, "for-each-ref", "--format=%(refname) %(objectname)")
	if err != nil {
		return err
	}
	actual := make(map[string]string)
	if out != "" {
		for _, line := range strings.Split(out, "\n") {
			parts := strings.Fields(line)
			if len(parts) != 2 {
				return fmt.Errorf("invalid for-each-ref output %q", line)
			}
			actual[parts[0]] = parts[1]
		}
	}
	oidLength := 40
	for _, oid := range desired {
		oidLength = len(oid)
		break
	}
	var updates []wal.RefUpdate
	for ref, old := range actual {
		if _, ok := desired[ref]; !ok {
			updates = append(updates, wal.RefUpdate{Old: old, New: strings.Repeat("0", oidLength), Ref: ref})
		}
	}
	for ref, oid := range desired {
		old, ok := actual[ref]
		if !ok {
			old = strings.Repeat("0", len(oid))
		}
		if old != oid {
			updates = append(updates, wal.RefUpdate{Old: old, New: oid, Ref: ref})
		}
	}
	sort.Slice(updates, func(i, j int) bool { return updates[i].Ref < updates[j].Ref })
	return applyUpdatesIdempotently(repo, updates)
}

func applyUpdatesIdempotently(repo string, updates []wal.RefUpdate) error {
	var pending []wal.RefUpdate
	for _, update := range updates {
		current, err := currentRef(repo, update.Ref, len(update.Old))
		if err != nil {
			return err
		}
		if current == update.New {
			continue
		}
		if current != update.Old {
			return fmt.Errorf("local ref conflict for %s: expected %s or %s, found %s", update.Ref, update.Old, update.New, current)
		}
		pending = append(pending, update)
	}
	if len(pending) == 0 {
		return nil
	}
	emptyHooks := filepath.Join(repo, "walgit-no-hooks")
	if err := os.MkdirAll(emptyHooks, 0o755); err != nil {
		return err
	}
	var in strings.Builder
	in.WriteString("start\n")
	for _, update := range pending {
		if isZeroOID(update.New) {
			fmt.Fprintf(&in, "delete %s %s\n", update.Ref, update.Old)
		} else {
			fmt.Fprintf(&in, "update %s %s %s\n", update.Ref, update.New, update.Old)
		}
	}
	in.WriteString("prepare\ncommit\n")
	cmd := exec.Command("git", "-c", "core.hooksPath="+emptyHooks, "-C", repo, "update-ref", "--stdin")
	cmd.Stdin = strings.NewReader(in.String())
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git update-ref: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func currentRef(repo, ref string, oidLength int) (string, error) {
	cmd := exec.Command("git", "-C", repo, "rev-parse", "--verify", ref)
	out, err := cmd.CombinedOutput()
	if err != nil {
		if cmd.ProcessState != nil && cmd.ProcessState.ExitCode() == 128 {
			return strings.Repeat("0", oidLength), nil
		}
		return "", fmt.Errorf("read %s: %w: %s", ref, err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}

func markCurrentIfManifestMatches(repo, storeRoot, id string) error {
	store, err := wal.Open(storeRoot)
	if err != nil {
		return err
	}
	m, err := store.Load(id)
	if err != nil {
		return err
	}
	if err := verifyRefs(repo, m.Refs); err != nil {
		return nil // Another disjoint transaction may still be prepared.
	}
	return writeLocalState(repo, localState{RepositoryID: id, Generation: m.Generation})
}

func verifyRefs(repo string, expected map[string]string) error {
	out, err := commandOutput("", "git", "-C", repo, "for-each-ref", "--format=%(refname) %(objectname)")
	if err != nil {
		return err
	}
	actual := make(map[string]string)
	if out != "" {
		for _, line := range strings.Split(out, "\n") {
			parts := strings.Fields(line)
			if len(parts) != 2 {
				return fmt.Errorf("invalid for-each-ref output %q", line)
			}
			actual[parts[0]] = parts[1]
		}
	}
	if len(actual) != len(expected) {
		return fmt.Errorf("ref count mismatch: local %d, manifest %d", len(actual), len(expected))
	}
	for ref, oid := range expected {
		if actual[ref] != oid {
			return fmt.Errorf("ref mismatch for %s: local %s, manifest %s", ref, actual[ref], oid)
		}
	}
	return nil
}

func readLocalState(repo, id string) (localState, error) {
	data, err := os.ReadFile(filepath.Join(repo, stateFileName))
	if errors.Is(err, os.ErrNotExist) {
		return localState{RepositoryID: id}, nil
	}
	if err != nil {
		return localState{}, err
	}
	var state localState
	if err := json.Unmarshal(data, &state); err != nil {
		return localState{}, err
	}
	if state.RepositoryID != id {
		return localState{}, fmt.Errorf("local state belongs to repository %q, not %q", state.RepositoryID, id)
	}
	return state, nil
}

func writeLocalState(repo string, state localState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tmp, err := os.CreateTemp(repo, ".walgit-state-*.tmp")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, filepath.Join(repo, stateFileName)); err != nil {
		return err
	}
	dir, err := os.Open(repo)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func isZeroOID(oid string) bool { return strings.Trim(oid, "0") == "" }
