package app

import (
	"context"
	"sync"
	"time"

	"walgit/internal/wal"
)

func (h *gitHTTPHandler) runScrub() (wal.ScrubReport, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	started := time.Now()
	report, err := wal.Scrub(h.options.Store, h.options.RepositoryID)
	h.metrics.recordScrub(report, time.Since(started), err)
	return report, err
}

func (h *gitHTTPHandler) runScrubLoop(ctx context.Context, group *sync.WaitGroup) {
	defer group.Done()
	ticker := time.NewTicker(h.options.ScrubInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = h.runScrub()
		}
	}
}
