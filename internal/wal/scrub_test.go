package wal

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestScrubVerifiesMatchingCertificateChainsAndPayloads(t *testing.T) {
	store, primary, secondary := dualFilesystemStore(t)
	id, _, entry := committedCertificateTransaction(t, store)
	t.Setenv("WALGIT_BLOB_SECONDARY_STORE", secondary.store.Root)

	report, err := Scrub(primary.store.Root, id)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Healthy || report.Generation != 1 || report.CertificateSHA256 == "" {
		t.Fatalf("unexpected scrub report: %#v", report)
	}
	for _, authority := range report.Authorities {
		if !authority.Healthy || authority.Certificates != 2 || authority.Descriptors != 1 || authority.Chunks < 2 || authority.VerifiedBytes <= entry.Descriptor.Bytes {
			t.Fatalf("authority did not verify its complete payload: %#v", authority)
		}
	}
	for _, root := range []string{primary.store.Root, secondary.store.Root} {
		if _, err := os.Stat(filepath.Join(root, ".walgit-repair-audit")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("read-only scrub created an audit directory in %s: %v", root, err)
		}
	}
}

func TestRepairRestoresMissingChunkAndWritesReplicatedAudit(t *testing.T) {
	store, primary, secondary := dualFilesystemStore(t)
	id, _, entry := committedCertificateTransaction(t, store)
	meta := descriptorMeta(t, primary, entry)
	missing := meta.Objects[0].Blob.Chunks[0]
	if err := os.Remove(secondary.path(missing.SHA256)); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WALGIT_BLOB_SECONDARY_STORE", secondary.store.Root)

	before, err := Scrub(primary.store.Root, id)
	if err == nil || before.Healthy || before.Authorities[1].Healthy {
		t.Fatalf("scrub accepted missing secondary payload: report=%#v err=%v", before, err)
	}
	repair, err := Repair(primary.store.Root, id, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if repair.RepairedChunks != 1 || repair.RepairedCertificates != 0 || repair.AuditSHA256 == "" || !repair.After.Healthy {
		t.Fatalf("unexpected repair report: %#v", repair)
	}
	if _, err := secondary.Get(missing); err != nil {
		t.Fatalf("repaired chunk is not readable: %v", err)
	}
	var audit []byte
	for _, root := range []string{primary.store.Root, secondary.store.Root} {
		files := regularFiles(t, filepath.Join(root, ".walgit-repair-audit", id))
		if len(files) != 1 {
			t.Fatalf("repair audit count in %s = %d, want 1", root, len(files))
		}
		data, err := os.ReadFile(files[0])
		if err != nil {
			t.Fatal(err)
		}
		if audit == nil {
			audit = data
		} else if string(audit) != string(data) {
			t.Fatal("authorities contain different repair audit records")
		}
	}
	if sha256Hex(audit) != repair.AuditSHA256 {
		t.Fatal("repair report does not identify the replicated audit record")
	}
}

func TestRepairReplacesCorruptCertificate(t *testing.T) {
	store, primary, secondary := dualFilesystemStore(t)
	id, _, _ := committedCertificateTransaction(t, store)
	files := regularFiles(t, filepath.Join(secondary.store.Root, ".walgit-certificates", id))
	certificate := files[len(files)-1]
	data, err := os.ReadFile(certificate)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)/2] ^= 0xff
	if err := os.WriteFile(certificate, data, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WALGIT_BLOB_SECONDARY_STORE", secondary.store.Root)

	repair, err := Repair(primary.store.Root, id, "primary")
	if err != nil {
		t.Fatal(err)
	}
	if repair.RepairedCertificates != 1 || !repair.After.Healthy {
		t.Fatalf("corrupt certificate was not repaired: %#v", repair)
	}
}

func TestRepairRefusesValidDivergentHistories(t *testing.T) {
	store, primary, secondary := dualFilesystemStore(t)
	id, objects, updates := certificateTransaction(t, store)
	if err := store.Stage(id, objects, updates); err != nil {
		t.Fatal(err)
	}
	store.certificates = replicatedCertificateStore{authorities: []certificateAuthority{
		primary, failingPutCertificateAuthority{certificateAuthority: secondary},
	}}
	if _, _, err := store.Commit(id, updates); err == nil {
		t.Fatal("one-sided certificate injection unexpectedly succeeded")
	}
	t.Setenv("WALGIT_BLOB_SECONDARY_STORE", secondary.store.Root)

	report, err := Scrub(primary.store.Root, id)
	if err == nil || report.Healthy || len(report.Issues) == 0 || report.Issues[0].Kind != "divergence" {
		t.Fatalf("scrub did not identify divergent valid chains: report=%#v err=%v", report, err)
	}
	if _, err := Repair(primary.store.Root, id, "primary"); err == nil || !strings.Contains(err.Error(), "divergent histories") {
		t.Fatalf("repair chose a divergent history: %v", err)
	}
}

func TestRepairRefusesUnverifiedSource(t *testing.T) {
	store, primary, secondary := dualFilesystemStore(t)
	id, _, entry := committedCertificateTransaction(t, store)
	meta := descriptorMeta(t, primary, entry)
	sourceChunk := primary.path(meta.Objects[0].Blob.Chunks[0].SHA256)
	data, err := os.ReadFile(sourceChunk)
	if err != nil {
		t.Fatal(err)
	}
	data[0] ^= 0xff
	if err := os.WriteFile(sourceChunk, data, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WALGIT_BLOB_SECONDARY_STORE", secondary.store.Root)

	if _, err := Repair(primary.store.Root, id, "primary"); err == nil || !strings.Contains(err.Error(), "source is not fully verified") {
		t.Fatalf("repair accepted a corrupt source: %v", err)
	}
}

func TestS3RepairReplacesCorruptChunk(t *testing.T) {
	client := newMemoryS3()
	store := &S3Store{client: client, bucket: "bucket", prefix: "walgit"}
	authority := s3BlobStore{store: store}
	data := []byte("verified repair payload")
	ref := chunkRef(data)
	if err := authority.Put(ref, data); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	object := client.objects[store.blobKey(ref.SHA256)]
	object.data[0] ^= 0xff
	client.objects[store.blobKey(ref.SHA256)] = object
	client.mu.Unlock()
	if _, err := authority.Get(ref); err == nil {
		t.Fatal("corrupt S3 chunk passed validation")
	}
	if err := authority.RepairChunk(ref, data); err != nil {
		t.Fatal(err)
	}
	got, err := authority.Get(ref)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("repaired S3 chunk = %q, want %q", got, data)
	}
}

func TestS3RepairRequiresDedicatedCredentials(t *testing.T) {
	t.Setenv("WALGIT_REPAIR_PRIMARY_ACCESS_KEY_ID", "")
	t.Setenv("WALGIT_REPAIR_PRIMARY_SECRET_ACCESS_KEY", "")
	if _, err := openS3Repair("s3://bucket/prefix", false); err == nil || !strings.Contains(err.Error(), "WALGIT_REPAIR_PRIMARY_ACCESS_KEY_ID") {
		t.Fatalf("S3 repair accepted foreground credentials: %v", err)
	}
}

func committedCertificateTransaction(t *testing.T, store Store) (string, string, ManifestEntry) {
	t.Helper()
	id, objects, updates := certificateTransaction(t, store)
	if err := store.Stage(id, objects, updates); err != nil {
		t.Fatal(err)
	}
	entry, _, err := store.Commit(id, updates)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Finalize(id, updates); err != nil {
		t.Fatal(err)
	}
	return id, objects, entry
}

func descriptorMeta(t *testing.T, authority filesystemBlobStore, entry ManifestEntry) EntryMeta {
	t.Helper()
	if entry.Descriptor == nil {
		t.Fatal("committed entry has no descriptor")
	}
	data, err := authority.Get(*entry.Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	var meta EntryMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		t.Fatal(err)
	}
	if len(meta.Objects) == 0 || len(meta.Objects[0].Blob.Chunks) == 0 {
		t.Fatalf("descriptor has no payload chunks: %#v", meta)
	}
	return meta
}
