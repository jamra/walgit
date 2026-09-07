package wal

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func (s filesystemBlobStore) RepairChunk(ref ChunkRef, data []byte) error {
	if err := validateChunk(ref, data); err != nil {
		return err
	}
	return s.replaceRepairObject(s.path(ref.SHA256), data)
}

func (s filesystemBlobStore) RepairCertificate(repoID string, generation uint64, digest string, data []byte) error {
	if err := validateID(repoID); err != nil {
		return err
	}
	if err := validateCertificateData(digest, data); err != nil {
		return err
	}
	return s.replaceRepairObject(s.certificatePath(repoID, generation, digest), data)
}

func (s filesystemBlobStore) PutRepairAudit(repoID, name, digest string, data []byte) error {
	if err := validateID(repoID); err != nil {
		return err
	}
	if err := validateRepairAudit(name, digest, data); err != nil {
		return err
	}
	path := filepath.Join(s.store.Root, ".walgit-repair-audit", repoID, name)
	if existing, err := os.ReadFile(path); err == nil {
		if !bytes.Equal(existing, data) {
			return fmt.Errorf("repair audit collision for %s", name)
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return s.replaceRepairObject(path, data)
}

func (s filesystemBlobStore) replaceRepairObject(path string, data []byte) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(directory, ".repair-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := (&durabilityWriter{store: s.store, writer: tmp, operation: "write repaired durable object"}).Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := s.store.syncFile(tmp, "sync repaired durable object"); err != nil {
		tmp.Close()
		return err
	}
	if err := s.store.closeFile(tmp, "close repaired durable object"); err != nil {
		return err
	}
	if err := s.store.rename(tmpName, path, "publish repaired durable object"); err != nil {
		return err
	}
	for current := directory; ; current = filepath.Dir(current) {
		if err := s.store.syncDirectory(current, "sync repaired durable object directory"); err != nil {
			return err
		}
		if current == s.store.Root {
			break
		}
	}
	return nil
}

func validateRepairAudit(name, digest string, data []byte) error {
	if name == "" || filepath.Base(name) != name || !strings.HasSuffix(name, ".json") {
		return fmt.Errorf("invalid repair audit name %q", name)
	}
	if err := validateSHA256(digest); err != nil {
		return err
	}
	if actual := sha256Hex(data); actual != digest {
		return fmt.Errorf("repair audit checksum mismatch: got %s, want %s", actual, digest)
	}
	return nil
}

var _ maintenanceAuthority = filesystemBlobStore{}
