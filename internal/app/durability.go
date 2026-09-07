package app

import "walgit/internal/wal"

func Scrub(store, repoID string) (wal.ScrubReport, error) {
	return wal.Scrub(store, repoID)
}

func Repair(store, repoID, source string) (wal.RepairReport, error) {
	return wal.Repair(store, repoID, source)
}
