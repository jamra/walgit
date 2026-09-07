package wal

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// ScrubIssue identifies data that could not be proven from one authority.
type ScrubIssue struct {
	Kind    string `json:"kind"`
	Object  string `json:"object,omitempty"`
	Message string `json:"message"`
}

// AuthorityScrubReport is the independently verified state of one durable
// authority. Healthy is true only after the full certificate chain and every
// reachable descriptor and payload chunk have been read and hashed.
type AuthorityScrubReport struct {
	Name              string       `json:"name"`
	Location          string       `json:"location"`
	Healthy           bool         `json:"healthy"`
	Generation        uint64       `json:"generation"`
	CertificateSHA256 string       `json:"certificate_sha256,omitempty"`
	CertificateFloor  uint64       `json:"certificate_floor,omitempty"`
	Certificates      int          `json:"certificates"`
	Descriptors       int          `json:"descriptors"`
	Chunks            int          `json:"chunks"`
	VerifiedBytes     int64        `json:"verified_bytes"`
	Issues            []ScrubIssue `json:"issues,omitempty"`
}

// ScrubReport compares the complete independently verified histories of both
// authorities. Matching only a manifest or a head key is intentionally not
// sufficient.
type ScrubReport struct {
	Version           int                    `json:"version"`
	Repository        string                 `json:"repository"`
	Healthy           bool                   `json:"healthy"`
	Generation        uint64                 `json:"generation"`
	CertificateSHA256 string                 `json:"certificate_sha256,omitempty"`
	Authorities       []AuthorityScrubReport `json:"authorities"`
	Retention         *RetentionReport       `json:"retention,omitempty"`
	Issues            []ScrubIssue           `json:"issues,omitempty"`
}

// RepairReport records a conservative copy operation from one explicitly
// selected, fully verified authority to its damaged peer.
type RepairReport struct {
	Version              int         `json:"version"`
	Repository           string      `json:"repository"`
	Source               string      `json:"source"`
	Target               string      `json:"target"`
	RepairedCertificates int         `json:"repaired_certificates"`
	RepairedChunks       int         `json:"repaired_chunks"`
	AuditSHA256          string      `json:"audit_sha256,omitempty"`
	Before               ScrubReport `json:"before"`
	After                ScrubReport `json:"after"`
}

type maintenanceAuthority interface {
	durableAuthority
	RepairChunk(ChunkRef, []byte) error
	RepairCertificate(repoID string, generation uint64, digest string, data []byte) error
	PutRepairAudit(repoID, name, digest string, data []byte) error
}

type namedMaintenanceAuthority struct {
	name      string
	location  string
	authority maintenanceAuthority
}

type verifiedSnapshot struct {
	headDigest   string
	generation   uint64
	floor        uint64
	manifest     Manifest
	certificates []certificateObject
	chunks       map[string]ChunkRef
	descriptors  map[string]struct{}
}

const scrubReportVersion = 1

// Scrub verifies both configured authorities without mutating either one.
func Scrub(location, repoID string) (ScrubReport, error) {
	authorities, err := openMaintenanceAuthorities(location, false)
	if err != nil {
		return ScrubReport{Version: scrubReportVersion, Repository: repoID}, err
	}
	report, _ := scrubAuthorities(repoID, authorities)
	retention, retentionErr := checkMaintenanceRetention(authorities)
	report.Retention = retention
	if retentionErr != nil {
		report.Healthy = false
		report.Issues = append(report.Issues, ScrubIssue{Kind: "retention", Message: retentionErr.Error()})
	}
	if !report.Healthy {
		return report, errors.New("durability scrub failed")
	}
	return report, nil
}

// Repair copies a verified certificate chain and its reachable content into a
// damaged peer. It never chooses between two valid divergent histories.
func Repair(location, repoID, sourceName string) (RepairReport, error) {
	report := RepairReport{Version: scrubReportVersion, Repository: repoID, Source: sourceName}
	authorities, err := openMaintenanceAuthorities(location, true)
	if err != nil {
		return report, err
	}
	if sourceName != "primary" && sourceName != "secondary" {
		return report, errors.New("repair source must be primary or secondary")
	}
	sourceIndex := 0
	if sourceName == "secondary" {
		sourceIndex = 1
	}
	targetIndex := 1 - sourceIndex
	report.Target = authorities[targetIndex].name
	retention, retentionErr := checkMaintenanceRetention(authorities)
	if retentionErr != nil {
		return report, fmt.Errorf("refusing repair: %w", retentionErr)
	}
	if retention != nil {
		if err := configureRetentionFromReport(authorities, *retention); err != nil {
			return report, fmt.Errorf("refusing repair: %w", err)
		}
	}

	before, snapshots := scrubAuthorities(repoID, authorities)
	before.Retention = retention
	report.Before = before
	sourceReport := before.Authorities[sourceIndex]
	targetReport := before.Authorities[targetIndex]
	if !sourceReport.Healthy {
		return report, fmt.Errorf("refusing repair: selected %s source is not fully verified", sourceName)
	}
	if targetReport.Healthy {
		if targetReport.CertificateSHA256 != sourceReport.CertificateSHA256 {
			return report, errors.New("refusing repair: authorities contain valid divergent histories")
		}
		report.After = before
		if err := writeRepairAudit(authorities, &report); err != nil {
			return report, err
		}
		return report, nil
	}
	snapshot := snapshots[sourceName]
	if snapshot == nil {
		return report, fmt.Errorf("refusing repair: selected %s source has no verified snapshot", sourceName)
	}

	// Publish payloads before certificate links so an interrupted repair never
	// creates a newly readable chain that points at unavailable data.
	chunkDigests := make([]string, 0, len(snapshot.chunks))
	for digest := range snapshot.chunks {
		chunkDigests = append(chunkDigests, digest)
	}
	sort.Strings(chunkDigests)
	target := authorities[targetIndex].authority
	for _, digest := range chunkDigests {
		ref := snapshot.chunks[digest]
		sourceData, err := authorities[sourceIndex].authority.Get(ref)
		if err != nil {
			return report, fmt.Errorf("source changed while reading chunk %s: %w", digest, err)
		}
		data, getErr := target.Get(ref)
		if getErr == nil && bytes.Equal(data, sourceData) {
			continue
		}
		if err := target.RepairChunk(ref, sourceData); err != nil {
			return report, fmt.Errorf("repair chunk %s: %w", digest, err)
		}
		verified, err := target.Get(ref)
		if err != nil || !bytes.Equal(verified, sourceData) {
			if err == nil {
				err = errors.New("repaired bytes differ from verified source")
			}
			return report, fmt.Errorf("verify repaired chunk %s: %w", digest, err)
		}
		report.RepairedChunks++
	}
	for _, certificate := range snapshot.certificates {
		data, getErr := target.GetCertificate(repoID, certificate.Generation, certificate.SHA256)
		if getErr == nil && bytes.Equal(data, certificate.Data) {
			continue
		}
		if err := target.RepairCertificate(repoID, certificate.Generation, certificate.SHA256, certificate.Data); err != nil {
			return report, fmt.Errorf("repair certificate %s: %w", certificate.SHA256, err)
		}
		verified, err := target.GetCertificate(repoID, certificate.Generation, certificate.SHA256)
		if err != nil || !bytes.Equal(verified, certificate.Data) {
			if err == nil {
				err = errors.New("repaired bytes differ from verified source")
			}
			return report, fmt.Errorf("verify repaired certificate %s: %w", certificate.SHA256, err)
		}
		report.RepairedCertificates++
	}

	after, _ := scrubAuthorities(repoID, authorities)
	after.Retention = retention
	report.After = after
	if !after.Healthy || after.CertificateSHA256 != snapshot.headDigest {
		return report, errors.New("repair did not produce two matching fully verified authorities")
	}
	if err := writeRepairAudit(authorities, &report); err != nil {
		return report, err
	}
	return report, nil
}

func writeRepairAudit(authorities []namedMaintenanceAuthority, report *RepairReport) error {
	audit := struct {
		Version              int       `json:"version"`
		Repository           string    `json:"repository"`
		CompletedAt          time.Time `json:"completed_at"`
		Source               string    `json:"source"`
		Target               string    `json:"target"`
		Generation           uint64    `json:"generation"`
		CertificateSHA256    string    `json:"certificate_sha256"`
		RepairedCertificates int       `json:"repaired_certificates"`
		RepairedChunks       int       `json:"repaired_chunks"`
	}{
		Version: scrubReportVersion, Repository: report.Repository, CompletedAt: time.Now().UTC(),
		Source: report.Source, Target: report.Target, Generation: report.After.Generation,
		CertificateSHA256:    report.After.CertificateSHA256,
		RepairedCertificates: report.RepairedCertificates, RepairedChunks: report.RepairedChunks,
	}
	auditData, err := json.Marshal(audit)
	if err != nil {
		return err
	}
	auditDigest := sha256Hex(auditData)
	auditName := audit.CompletedAt.Format("20060102T150405.000000000Z") + "-" + auditDigest + ".json"
	for _, authority := range authorities {
		if err := authority.authority.PutRepairAudit(report.Repository, auditName, auditDigest, auditData); err != nil {
			return fmt.Errorf("write repair audit to %s: %w", authority.name, err)
		}
	}
	report.AuditSHA256 = auditDigest
	return nil
}

func openMaintenanceAuthorities(location string, repair bool) ([]namedMaintenanceAuthority, error) {
	if strings.TrimSpace(location) == "" {
		return nil, errors.New("storage location is required")
	}
	secondaryLocation := strings.TrimSpace(os.Getenv("WALGIT_BLOB_SECONDARY_STORE"))
	if secondaryLocation == "" {
		return nil, errors.New("scrub and repair require WALGIT_BLOB_SECONDARY_STORE")
	}
	primaryIdentity, err := normalizeBlobLocation(location)
	if err != nil {
		return nil, err
	}
	secondaryIdentity, err := normalizeBlobLocation(secondaryLocation)
	if err != nil {
		return nil, err
	}
	if primaryIdentity == secondaryIdentity {
		return nil, errors.New("primary and secondary stores must be different locations")
	}
	opener := openAuthority
	if repair {
		opener = openRepairAuthority
	}
	primary, err := opener(location, false)
	if err != nil {
		return nil, fmt.Errorf("open primary authority: %w", err)
	}
	secondary, err := opener(secondaryLocation, true)
	if err != nil {
		return nil, fmt.Errorf("open secondary authority: %w", err)
	}
	primaryMaintenance, ok := primary.(maintenanceAuthority)
	if !ok {
		return nil, errors.New("primary authority does not support scrub and repair")
	}
	secondaryMaintenance, ok := secondary.(maintenanceAuthority)
	if !ok {
		return nil, errors.New("secondary authority does not support scrub and repair")
	}
	return []namedMaintenanceAuthority{
		{name: "primary", location: primaryIdentity, authority: primaryMaintenance},
		{name: "secondary", location: secondaryIdentity, authority: secondaryMaintenance},
	}, nil
}

func scrubAuthorities(repoID string, authorities []namedMaintenanceAuthority) (ScrubReport, map[string]*verifiedSnapshot) {
	report := ScrubReport{Version: scrubReportVersion, Repository: repoID}
	snapshots := make(map[string]*verifiedSnapshot, len(authorities))
	if err := validateID(repoID); err != nil {
		report.Issues = append(report.Issues, ScrubIssue{Kind: "configuration", Message: err.Error()})
		return report, snapshots
	}
	type result struct {
		index    int
		report   AuthorityScrubReport
		snapshot *verifiedSnapshot
	}
	results := make(chan result, len(authorities))
	for index, named := range authorities {
		go func() {
			authorityReport, snapshot := inspectAuthority(repoID, named)
			results <- result{index: index, report: authorityReport, snapshot: snapshot}
		}()
	}
	report.Authorities = make([]AuthorityScrubReport, len(authorities))
	for range authorities {
		inspected := <-results
		report.Authorities[inspected.index] = inspected.report
		if inspected.snapshot != nil {
			snapshots[authorities[inspected.index].name] = inspected.snapshot
		}
	}
	if len(report.Authorities) != 2 {
		report.Issues = append(report.Issues, ScrubIssue{Kind: "configuration", Message: "exactly two authorities are required"})
		return report, snapshots
	}
	left, right := report.Authorities[0], report.Authorities[1]
	if !left.Healthy || !right.Healthy {
		report.Issues = append(report.Issues, ScrubIssue{Kind: "authority", Message: "both authorities are not fully verified"})
		return report, snapshots
	}
	if left.CertificateSHA256 != right.CertificateSHA256 || left.Generation != right.Generation {
		report.Issues = append(report.Issues, ScrubIssue{
			Kind: "divergence", Message: "authorities contain valid but different certificate heads",
		})
		return report, snapshots
	}
	report.Healthy = true
	report.Generation = left.Generation
	report.CertificateSHA256 = left.CertificateSHA256
	return report, snapshots
}

func inspectAuthority(repoID string, named namedMaintenanceAuthority) (AuthorityScrubReport, *verifiedSnapshot) {
	report := AuthorityScrubReport{Name: named.name, Location: named.location}
	objects, err := named.authority.ListCertificates(repoID)
	if err != nil {
		report.Issues = append(report.Issues, ScrubIssue{Kind: "certificate-list", Message: err.Error()})
		return report, nil
	}
	states, err := validateCertificateObjects(repoID, objects)
	if err != nil {
		report.Issues = append(report.Issues, ScrubIssue{Kind: "certificate-chain", Message: err.Error()})
		return report, nil
	}
	state, digest, err := newestCertificateState(states)
	if err != nil {
		report.Issues = append(report.Issues, ScrubIssue{Kind: "certificate-head", Message: err.Error()})
		return report, nil
	}
	objectByDigest := make(map[string]certificateObject, len(objects))
	for _, object := range objects {
		objectByDigest[object.SHA256] = object
	}
	chain := make([]certificateObject, 0, len(objects))
	for current := digest; current != ""; {
		object, ok := objectByDigest[current]
		if !ok {
			report.Issues = append(report.Issues, ScrubIssue{Kind: "certificate-chain", Object: current, Message: "certificate is absent from listing"})
			return report, nil
		}
		chain = append(chain, object)
		current = states[current].certificate.PreviousSHA256
	}
	for left, right := 0, len(chain)-1; left < right; left, right = left+1, right-1 {
		chain[left], chain[right] = chain[right], chain[left]
	}
	if len(chain) != len(objects) {
		report.Issues = append(report.Issues, ScrubIssue{
			Kind: "certificate-fork", Message: "certificate authority contains a valid noncanonical branch",
		})
		return report, nil
	}
	snapshot := &verifiedSnapshot{
		headDigest: digest, generation: state.certificate.Generation, floor: state.floor,
		manifest: Manifest{
			Version: manifestVersion, Generation: state.certificate.Generation,
			Head: state.certificate.Head, ObjectFormat: state.certificate.ObjectFormat,
			Refs: cloneRefs(state.certificate.Refs), Entries: append([]ManifestEntry(nil), state.entries...),
			CertificateSHA256: digest, CertificateFloor: state.floor,
		},
		certificates: chain, chunks: make(map[string]ChunkRef), descriptors: make(map[string]struct{}),
	}
	for _, object := range chain {
		certificate := states[object.SHA256].certificate
		for _, entry := range certificate.Entries {
			if err := verifyCertificateEntry(named.authority, entry, snapshot); err != nil {
				report.Issues = append(report.Issues, ScrubIssue{
					Kind: "descriptor", Object: entry.Entry.TransactionID, Message: err.Error(),
				})
				return report, nil
			}
		}
	}
	report.Healthy = true
	report.Generation = snapshot.generation
	report.CertificateSHA256 = snapshot.headDigest
	report.CertificateFloor = snapshot.floor
	report.Certificates = len(snapshot.certificates)
	report.Descriptors = len(snapshot.descriptors)
	report.Chunks = len(snapshot.chunks)
	for _, chunk := range snapshot.chunks {
		report.VerifiedBytes += chunk.Bytes
	}
	return report, snapshot
}

func verifyCertificateEntry(authority blobStore, certified CertificateEntry, snapshot *verifiedSnapshot) error {
	if certified.Entry.Descriptor == nil {
		return errors.New("certificate entry has no descriptor")
	}
	data, err := readVerifiedChunk(authority, *certified.Entry.Descriptor, snapshot)
	if err != nil {
		return fmt.Errorf("read transaction descriptor: %w", err)
	}
	snapshot.descriptors[certified.Entry.Descriptor.SHA256] = struct{}{}
	var meta EntryMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return fmt.Errorf("decode transaction descriptor: %w", err)
	}
	if meta.Descriptor != nil {
		return errors.New("transaction descriptor recursively references a descriptor")
	}
	if meta.TransactionID != certified.Entry.TransactionID || !transactionIdentityMatches(meta.TransactionID, meta.Updates) {
		return errors.New("transaction descriptor identity does not match certificate")
	}
	if !equalUpdates(meta.Updates, certified.Updates) {
		return errors.New("transaction descriptor updates do not match certificate")
	}
	var payloadBytes int64
	for _, object := range meta.Objects {
		clean := filepath.Clean(filepath.FromSlash(object.Path))
		if clean == "." || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			return fmt.Errorf("unsafe blob object path %q", object.Path)
		}
		if object.Blob.Version != blobFormatVersion {
			return fmt.Errorf("unsupported blob descriptor version %d", object.Blob.Version)
		}
		whole := sha256.New()
		var objectBytes int64
		for _, ref := range object.Blob.Chunks {
			chunk, err := readVerifiedChunk(authority, ref, snapshot)
			if err != nil {
				return fmt.Errorf("read object %s chunk: %w", object.Path, err)
			}
			_, _ = whole.Write(chunk)
			objectBytes += int64(len(chunk))
		}
		if objectBytes != object.Blob.Bytes {
			return fmt.Errorf("object %s has %d bytes, want %d", object.Path, objectBytes, object.Blob.Bytes)
		}
		if actual := hex.EncodeToString(whole.Sum(nil)); actual != object.Blob.SHA256 {
			return fmt.Errorf("object %s checksum mismatch: got %s, want %s", object.Path, actual, object.Blob.SHA256)
		}
		payloadBytes += objectBytes
	}
	if payloadBytes != certified.Entry.PayloadBytes {
		return fmt.Errorf("entry payload has %d bytes, certificate records %d", payloadBytes, certified.Entry.PayloadBytes)
	}
	return nil
}

func readVerifiedChunk(authority blobStore, ref ChunkRef, snapshot *verifiedSnapshot) ([]byte, error) {
	if err := validateChunkRef(ref); err != nil {
		return nil, err
	}
	if existing, ok := snapshot.chunks[ref.SHA256]; ok {
		if existing.Bytes != ref.Bytes {
			return nil, fmt.Errorf("chunk %s has conflicting lengths", ref.SHA256)
		}
	}
	data, err := authority.Get(ref)
	if err != nil {
		return nil, fmt.Errorf("chunk %s: %w", ref.SHA256, err)
	}
	if err := validateChunk(ref, data); err != nil {
		return nil, err
	}
	snapshot.chunks[ref.SHA256] = ref
	return data, nil
}

func equalUpdates(left, right []RefUpdate) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
