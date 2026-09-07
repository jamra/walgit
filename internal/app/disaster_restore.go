package app

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"walgit/internal/wal"
)

// DisasterRestoreReport proves that a single authority can independently
// reconstruct a Git repository from an empty directory.
type DisasterRestoreReport struct {
	Version              int                      `json:"version"`
	Repository           string                   `json:"repository"`
	Source               string                   `json:"source"`
	Generation           uint64                   `json:"generation"`
	CertificateSHA256    string                   `json:"certificate_sha256"`
	References           int                      `json:"references"`
	VerifiedBytes        int64                    `json:"verified_bytes"`
	InspectionMillis     float64                  `json:"inspection_millis"`
	RestoreMillis        float64                  `json:"restore_millis"`
	FSCKMillis           float64                  `json:"fsck_millis"`
	TotalMillis          float64                  `json:"total_millis"`
	IndependentAuthority wal.AuthorityScrubReport `json:"independent_authority"`
	RestoredRepository   string                   `json:"restored_repository,omitempty"`
}

func DisasterRestoreDrill(store, id, source string, keep bool) (report DisasterRestoreReport, returnErr error) {
	report = DisasterRestoreReport{Version: 1, Repository: id, Source: source}
	if store == "" || id == "" || source == "" {
		return report, errors.New("drill requires -store, -id, and -source")
	}
	totalStarted := time.Now()
	inspectStarted := time.Now()
	manifest, authority, err := wal.InspectRestoreAuthority(store, id, source)
	report.InspectionMillis = milliseconds(time.Since(inspectStarted))
	report.IndependentAuthority = authority
	if err != nil {
		return report, err
	}

	root, err := os.MkdirTemp("", "walgit-disaster-drill-")
	if err != nil {
		return report, err
	}
	if !keep {
		defer func() {
			if err := os.RemoveAll(root); err != nil && returnErr == nil {
				returnErr = fmt.Errorf("remove drill repository: %w", err)
			}
		}()
	}
	repo := filepath.Join(root, "restored.git")
	args := []string{"init", "--bare", "--initial-branch=main"}
	if manifest.ObjectFormat == "sha256" {
		args = append(args, "--object-format=sha256")
	}
	args = append(args, repo)
	if err := run("", "git", args...); err != nil {
		return report, err
	}

	restoreStarted := time.Now()
	replayed, replayReport, err := wal.ReplayRestoreAuthority(store, id, source, filepath.Join(repo, "objects"), func(_ wal.ManifestEntry, meta wal.EntryMeta) error {
		return applyUpdatesIdempotently(repo, meta.Updates)
	})
	report.RestoreMillis = milliseconds(time.Since(restoreStarted))
	report.IndependentAuthority = replayReport
	if err != nil {
		return report, err
	}
	if replayed.CertificateSHA256 != manifest.CertificateSHA256 || replayed.Generation != manifest.Generation {
		return report, errors.New("authority changed during disaster restore")
	}
	if replayed.Head != "" {
		if err := run("", "git", "-C", repo, "symbolic-ref", "HEAD", replayed.Head); err != nil {
			return report, err
		}
	}
	if err := verifyRefs(repo, replayed.Refs); err != nil {
		return report, err
	}
	if err := writeLocalState(repo, localState{RepositoryID: id, Generation: replayed.Generation}); err != nil {
		return report, err
	}
	fsckStarted := time.Now()
	if err := run("", "git", "-C", repo, "fsck", "--strict"); err != nil {
		return report, fmt.Errorf("restored repository failed git fsck --strict: %w", err)
	}
	report.FSCKMillis = milliseconds(time.Since(fsckStarted))
	report.Generation = replayed.Generation
	report.CertificateSHA256 = replayed.CertificateSHA256
	report.References = len(replayed.Refs)
	report.VerifiedBytes = replayReport.VerifiedBytes
	report.TotalMillis = milliseconds(time.Since(totalStarted))
	if keep {
		report.RestoredRepository = repo
	}
	return report, nil
}

func milliseconds(duration time.Duration) float64 {
	return float64(duration.Microseconds()) / 1000
}
