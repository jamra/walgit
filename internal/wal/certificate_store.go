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
	"strconv"
	"strings"
)

const (
	certificateFormatVersion = 1
	maximumCertificateBytes  = 16 << 20
)

// CertificateEntry contains everything needed to validate and replay a
// committed manifest entry without the primary WAL object. Descriptor is
// required for certificates created by the dual-authority protocol.
type CertificateEntry struct {
	Entry   ManifestEntry `json:"entry"`
	Updates []RefUpdate   `json:"updates"`
}

// CommitCertificate is an immutable link in the repository's authoritative
// history. A certificate is committed only after the identical JSON bytes are
// durably present in every configured authority.
type CommitCertificate struct {
	Version        int                `json:"version"`
	Repository     string             `json:"repository"`
	Generation     uint64             `json:"generation"`
	PreviousSHA256 string             `json:"previous_sha256,omitempty"`
	Head           string             `json:"head"`
	ObjectFormat   string             `json:"object_format"`
	Refs           map[string]string  `json:"refs"`
	Entries        []CertificateEntry `json:"entries,omitempty"`
	LegacyAnchor   bool               `json:"legacy_anchor,omitempty"`
}

type certificateObject struct {
	Generation uint64
	SHA256     string
	Data       []byte
}

type certificateAuthority interface {
	PutCertificate(repoID string, generation uint64, digest string, data []byte) error
	GetCertificate(repoID string, generation uint64, digest string) ([]byte, error)
	ListCertificates(repoID string) ([]certificateObject, error)
}

type certificateStore interface {
	Publish(repoID string, certificate CommitCertificate) (string, error)
	Recover(repoID string) (Manifest, string, error)
}

type replicatedCertificateStore struct {
	authorities []certificateAuthority
}

func (s replicatedCertificateStore) Publish(repoID string, certificate CommitCertificate) (string, error) {
	if len(s.authorities) < 2 {
		return "", errors.New("certificate store requires at least two authorities")
	}
	if certificate.Repository != repoID {
		return "", fmt.Errorf("certificate repository %q does not match %q", certificate.Repository, repoID)
	}
	data, err := json.Marshal(certificate)
	if err != nil {
		return "", err
	}
	if len(data) > maximumCertificateBytes {
		return "", fmt.Errorf("certificate is %d bytes, limit is %d", len(data), maximumCertificateBytes)
	}
	digest := sha256Hex(data)
	if certificate.PreviousSHA256 != "" {
		if err := validateSHA256(certificate.PreviousSHA256); err != nil {
			return "", err
		}
		if uint64(len(certificate.Entries)) > certificate.Generation {
			return "", errors.New("certificate entries exceed its generation")
		}
	}
	parentGeneration := certificate.Generation - uint64(len(certificate.Entries))
	errs := make(chan error, len(s.authorities))
	for _, authority := range s.authorities {
		authority := authority
		go func() {
			if certificate.PreviousSHA256 != "" {
				parent, err := authority.GetCertificate(repoID, parentGeneration, certificate.PreviousSHA256)
				if err != nil {
					errs <- fmt.Errorf("verify parent certificate: %w", err)
					return
				}
				if err := validateCertificateData(certificate.PreviousSHA256, parent); err != nil {
					errs <- fmt.Errorf("verify parent certificate: %w", err)
					return
				}
				var parentCertificate CommitCertificate
				if err := json.Unmarshal(parent, &parentCertificate); err != nil {
					errs <- fmt.Errorf("decode parent certificate: %w", err)
					return
				}
				if parentCertificate.Version != certificateFormatVersion || parentCertificate.Repository != repoID || parentCertificate.Generation != parentGeneration {
					errs <- errors.New("parent certificate identity does not match the child")
					return
				}
				if _, err := validateCertificateChild(certificateState{certificate: parentCertificate}, certificate); err != nil {
					errs <- fmt.Errorf("validate child against parent certificate: %w", err)
					return
				}
			}
			errs <- authority.PutCertificate(repoID, certificate.Generation, digest, data)
		}()
	}
	var failures []string
	for range s.authorities {
		if err := <-errs; err != nil {
			failures = append(failures, err.Error())
		}
	}
	if len(failures) > 0 {
		return "", fmt.Errorf("certificate did not reach every durable authority: %s", strings.Join(failures, "; "))
	}
	return digest, nil
}

func (s replicatedCertificateStore) Recover(repoID string) (Manifest, string, error) {
	if len(s.authorities) < 2 {
		return Manifest{}, "", errors.New("certificate store requires at least two authorities")
	}
	type authorityResult struct {
		states map[string]certificateState
		err    error
	}
	results := make(chan authorityResult, len(s.authorities))
	for _, authority := range s.authorities {
		authority := authority
		go func() {
			objects, err := authority.ListCertificates(repoID)
			if err != nil {
				results <- authorityResult{err: err}
				return
			}
			states, err := validateCertificateObjects(repoID, objects)
			results <- authorityResult{states: states, err: err}
		}()
	}
	var available []map[string]certificateState
	var failures []string
	for range s.authorities {
		result := <-results
		if result.err != nil {
			failures = append(failures, result.err.Error())
			continue
		}
		available = append(available, result.states)
	}
	if len(available) == 0 {
		return Manifest{}, "", fmt.Errorf("no certificate authority is readable: %s", strings.Join(failures, "; "))
	}
	states := available[0]
	if len(available) > 1 {
		common := make(map[string]certificateState)
		for digest, state := range states {
			present := true
			for _, authorityStates := range available[1:] {
				if _, ok := authorityStates[digest]; !ok {
					present = false
					break
				}
			}
			if present {
				common[digest] = state
			}
		}
		states = common
	}
	state, digest, err := newestCertificateState(states)
	if err != nil {
		return Manifest{}, "", err
	}
	manifest := Manifest{
		Version: manifestVersion, Generation: state.certificate.Generation,
		Head: state.certificate.Head, ObjectFormat: state.certificate.ObjectFormat,
		Refs: cloneRefs(state.certificate.Refs), Entries: append([]ManifestEntry(nil), state.entries...),
		CertificateSHA256: digest,
		CertificateFloor:  state.floor,
	}
	return manifest, digest, nil
}

type certificateState struct {
	certificate CommitCertificate
	entries     []ManifestEntry
	floor       uint64
}

func validateCertificateObjects(repoID string, objects []certificateObject) (map[string]certificateState, error) {
	if len(objects) == 0 {
		return nil, errors.New("certificate authority contains no certificates")
	}
	certificates := make(map[string]CommitCertificate, len(objects))
	for _, object := range objects {
		if len(object.Data) == 0 || len(object.Data) > maximumCertificateBytes {
			return nil, fmt.Errorf("certificate %s has invalid size %d", object.SHA256, len(object.Data))
		}
		if sha256Hex(object.Data) != object.SHA256 {
			return nil, fmt.Errorf("certificate checksum mismatch for %s", object.SHA256)
		}
		if err := validateSHA256(object.SHA256); err != nil {
			return nil, err
		}
		var certificate CommitCertificate
		if err := json.Unmarshal(object.Data, &certificate); err != nil {
			return nil, fmt.Errorf("decode certificate %s: %w", object.SHA256, err)
		}
		if certificate.Version != certificateFormatVersion || certificate.Repository != repoID {
			return nil, fmt.Errorf("invalid certificate %s identity", object.SHA256)
		}
		if certificate.Generation != object.Generation {
			return nil, fmt.Errorf("certificate %s generation does not match its key", object.SHA256)
		}
		if certificate.Head == "" || (certificate.ObjectFormat != "sha1" && certificate.ObjectFormat != "sha256") {
			return nil, fmt.Errorf("invalid certificate %s repository configuration", object.SHA256)
		}
		if certificate.Refs == nil {
			certificate.Refs = map[string]string{}
		}
		if _, exists := certificates[object.SHA256]; exists {
			return nil, fmt.Errorf("duplicate certificate %s", object.SHA256)
		}
		certificates[object.SHA256] = certificate
	}

	states := make(map[string]certificateState, len(certificates))
	for digest, certificate := range certificates {
		if certificate.PreviousSHA256 != "" {
			continue
		}
		if len(certificate.Entries) != 0 || (certificate.Generation != 0 && !certificate.LegacyAnchor) {
			return nil, fmt.Errorf("invalid root certificate %s", digest)
		}
		floor := uint64(0)
		if certificate.LegacyAnchor {
			floor = certificate.Generation
		}
		states[digest] = certificateState{certificate: certificate, floor: floor}
	}
	for progress := true; progress; {
		progress = false
		for digest, certificate := range certificates {
			if _, ok := states[digest]; ok || certificate.PreviousSHA256 == "" {
				continue
			}
			parent, ok := states[certificate.PreviousSHA256]
			if !ok {
				continue
			}
			state, err := validateCertificateChild(parent, certificate)
			if err != nil {
				return nil, fmt.Errorf("validate certificate %s: %w", digest, err)
			}
			states[digest] = state
			progress = true
		}
	}
	if len(states) != len(certificates) {
		return nil, errors.New("certificate set contains an unlinked or cyclic certificate")
	}
	return states, nil
}

func validateCertificateChild(parent certificateState, certificate CommitCertificate) (certificateState, error) {
	if certificate.Head != parent.certificate.Head || certificate.ObjectFormat != parent.certificate.ObjectFormat {
		return certificateState{}, errors.New("repository configuration changed")
	}
	if len(certificate.Entries) == 0 || certificate.Generation != parent.certificate.Generation+uint64(len(certificate.Entries)) {
		return certificateState{}, errors.New("certificate generation is not contiguous")
	}
	refs := cloneRefs(parent.certificate.Refs)
	entries := append([]ManifestEntry(nil), parent.entries...)
	for index, certified := range certificate.Entries {
		expectedGeneration := parent.certificate.Generation + uint64(index) + 1
		if certified.Entry.Generation != expectedGeneration || certified.Entry.TransactionID == "" {
			return certificateState{}, fmt.Errorf("invalid entry at generation %d", expectedGeneration)
		}
		if certified.Entry.Descriptor == nil {
			return certificateState{}, fmt.Errorf("entry %d has no replicated descriptor", expectedGeneration)
		}
		if err := validateChunkRef(*certified.Entry.Descriptor); err != nil {
			return certificateState{}, err
		}
		if err := validateUpdates(Manifest{Refs: refs}, certified.Updates); err != nil {
			return certificateState{}, err
		}
		if !transactionIdentityMatches(certified.Entry.TransactionID, certified.Updates) {
			return certificateState{}, errors.New("entry transaction identity does not match updates")
		}
		applyRefUpdates(refs, certified.Updates)
		entries = append(entries, certified.Entry)
	}
	if !equalRefs(refs, certificate.Refs) {
		return certificateState{}, errors.New("certificate resulting refs do not match its entries")
	}
	return certificateState{certificate: certificate, entries: entries, floor: parent.floor}, nil
}

func newestCertificateState(states map[string]certificateState) (certificateState, string, error) {
	var selected certificateState
	var digest string
	for candidateDigest, candidate := range states {
		if digest == "" || candidate.certificate.Generation > selected.certificate.Generation {
			selected, digest = candidate, candidateDigest
			continue
		}
		if candidate.certificate.Generation == selected.certificate.Generation && candidateDigest != digest {
			return certificateState{}, "", fmt.Errorf("ambiguous certificate forks at generation %d", candidate.certificate.Generation)
		}
	}
	if digest == "" {
		return certificateState{}, "", errors.New("certificate authority contains no valid chain")
	}
	return selected, digest, nil
}

func anchorCertificate(repoID string, manifest Manifest) CommitCertificate {
	return CommitCertificate{
		Version: certificateFormatVersion, Repository: repoID, Generation: manifest.Generation,
		Head: manifest.Head, ObjectFormat: manifest.ObjectFormat, Refs: cloneRefs(manifest.Refs),
		LegacyAnchor: manifest.Generation > 0,
	}
}

func transitionCertificate(repoID string, previous, next Manifest, entries []CertificateEntry) CommitCertificate {
	return CommitCertificate{
		Version: certificateFormatVersion, Repository: repoID, Generation: next.Generation,
		PreviousSHA256: previous.CertificateSHA256,
		Head:           next.Head, ObjectFormat: next.ObjectFormat, Refs: cloneRefs(next.Refs), Entries: entries,
	}
}

func publishAnchor(store certificateStore, repoID string, manifest Manifest) (Manifest, error) {
	if store == nil || manifest.CertificateSHA256 != "" {
		return manifest, nil
	}
	digest, err := store.Publish(repoID, anchorCertificate(repoID, manifest))
	if err != nil {
		return Manifest{}, err
	}
	manifest.CertificateSHA256 = digest
	if manifest.Generation > 0 {
		manifest.CertificateFloor = manifest.Generation
	}
	return manifest, nil
}

func publishTransition(store certificateStore, repoID string, previous, next Manifest, entries []CertificateEntry) (Manifest, error) {
	if store == nil {
		return next, nil
	}
	if previous.CertificateSHA256 == "" {
		return Manifest{}, errors.New("dual-authority manifest has no parent certificate")
	}
	if err := validateSHA256(previous.CertificateSHA256); err != nil {
		return Manifest{}, err
	}
	certificate := transitionCertificate(repoID, previous, next, entries)
	parent := certificateState{certificate: CommitCertificate{
		Generation: previous.Generation, Head: previous.Head,
		ObjectFormat: previous.ObjectFormat, Refs: cloneRefs(previous.Refs),
	}}
	if _, err := validateCertificateChild(parent, certificate); err != nil {
		return Manifest{}, err
	}
	digest, err := store.Publish(repoID, certificate)
	if err != nil {
		return Manifest{}, err
	}
	next.CertificateSHA256 = digest
	return next, nil
}

func certificateObjectFor(certificate CommitCertificate) certificateObject {
	data, _ := json.Marshal(certificate)
	return certificateObject{Generation: certificate.Generation, SHA256: sha256Hex(data), Data: data}
}

func applyRefUpdates(refs map[string]string, updates []RefUpdate) {
	for _, update := range updates {
		if isZeroOID(update.New) {
			delete(refs, update.Ref)
		} else {
			refs[update.Ref] = update.New
		}
	}
}

func equalRefs(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for ref, oid := range left {
		if right[ref] != oid {
			return false
		}
	}
	return true
}

func validateSHA256(digest string) error {
	if len(digest) != sha256.Size*2 || strings.ToLower(digest) != digest {
		return fmt.Errorf("invalid SHA-256 %q", digest)
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return fmt.Errorf("invalid SHA-256 %q: %w", digest, err)
	}
	return nil
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (s filesystemBlobStore) PutCertificate(repoID string, generation uint64, digest string, data []byte) error {
	if err := validateID(repoID); err != nil {
		return err
	}
	if err := validateCertificateData(digest, data); err != nil {
		return err
	}
	if err := s.store.checkHealthy(); err != nil {
		return err
	}
	path := s.certificatePath(repoID, generation, digest)
	if existing, err := os.ReadFile(path); err == nil {
		if !bytes.Equal(existing, data) {
			return fmt.Errorf("certificate collision for %s", digest)
		}
		return validateCertificateData(digest, existing)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(directory, ".certificate-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := (&durabilityWriter{store: s.store, writer: tmp, operation: "write commit certificate"}).Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := s.store.syncFile(tmp, "sync commit certificate"); err != nil {
		tmp.Close()
		return err
	}
	if err := s.store.closeFile(tmp, "close commit certificate"); err != nil {
		return err
	}
	if err := s.store.rename(tmpName, path, "publish commit certificate"); err != nil {
		return err
	}
	for current := directory; ; current = filepath.Dir(current) {
		if err := s.store.syncDirectory(current, "sync commit certificate directory"); err != nil {
			return err
		}
		if current == s.store.Root {
			break
		}
	}
	return nil
}

func (s filesystemBlobStore) ListCertificates(repoID string) ([]certificateObject, error) {
	if err := validateID(repoID); err != nil {
		return nil, err
	}
	if err := s.store.checkHealthy(); err != nil {
		return nil, err
	}
	directory := filepath.Join(s.store.Root, ".walgit-certificates", repoID)
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	objects := make([]certificateObject, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".cert") {
			continue
		}
		generation, digest, ok := certificateIdentityFromName(entry.Name())
		if !ok {
			return nil, fmt.Errorf("invalid certificate filename %q", entry.Name())
		}
		data, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			return nil, err
		}
		objects = append(objects, certificateObject{Generation: generation, SHA256: digest, Data: data})
	}
	return objects, nil
}

func (s filesystemBlobStore) GetCertificate(repoID string, generation uint64, digest string) ([]byte, error) {
	if err := validateID(repoID); err != nil {
		return nil, err
	}
	if err := validateSHA256(digest); err != nil {
		return nil, err
	}
	if err := s.store.checkHealthy(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(s.certificatePath(repoID, generation, digest))
	if err != nil {
		return nil, err
	}
	if err := validateCertificateData(digest, data); err != nil {
		return nil, err
	}
	return data, nil
}

func (s filesystemBlobStore) certificatePath(repoID string, generation uint64, digest string) string {
	return filepath.Join(s.store.Root, ".walgit-certificates", repoID, certificateName(generation, digest))
}

func certificateName(generation uint64, digest string) string {
	return fmt.Sprintf("%020d-%s.cert", generation, digest)
}

func certificateIdentityFromName(name string) (uint64, string, bool) {
	if !strings.HasSuffix(name, ".cert") {
		return 0, "", false
	}
	base := strings.TrimSuffix(name, ".cert")
	separator := strings.IndexByte(base, '-')
	if separator != 20 || len(base) != 20+1+sha256.Size*2 {
		return 0, "", false
	}
	generation, err := strconv.ParseUint(base[:separator], 10, 64)
	if err != nil {
		return 0, "", false
	}
	digest := base[separator+1:]
	return generation, digest, validateSHA256(digest) == nil
}

func validateCertificateData(digest string, data []byte) error {
	if err := validateSHA256(digest); err != nil {
		return err
	}
	if len(data) == 0 || len(data) > maximumCertificateBytes {
		return fmt.Errorf("certificate has invalid size %d", len(data))
	}
	if actual := sha256Hex(data); actual != digest {
		return fmt.Errorf("certificate checksum mismatch: got %s, want %s", actual, digest)
	}
	return nil
}

var _ certificateStore = replicatedCertificateStore{}
var _ certificateAuthority = filesystemBlobStore{}
