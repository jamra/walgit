package wal

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	awshttp "github.com/aws/smithy-go/transport/http"
)

type s3Client interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

type S3Store struct {
	client              s3Client
	bucket              string
	prefix              string
	timeout             time.Duration
	unconditionalWrites bool
	stateMu             sync.Mutex
	staged              map[string]stagedS3Transaction
	cachedManifest      *Manifest
	cachedETag          string
	blobs               blobStore
	certificates        certificateStore
}

type stagedS3Transaction struct {
	digest       string
	bytes        int64
	payloadBytes int64
	descriptor   *ChunkRef
}

type streamedArchiveResult struct {
	object stagedS3Transaction
	rawSum []byte
	err    error
}

type countingWriter struct {
	writer io.Writer
	bytes  int64
}

func (w *countingWriter) Write(data []byte) (int, error) {
	n, err := w.writer.Write(data)
	w.bytes += int64(n)
	return n, err
}

// putStreamedObject generates an archive exactly once while the S3 client is
// uploading it. io.Pipe provides bounded backpressure; the SHA-256 is updated
// from the same byte slices sent to the client, so the normal path has no
// durable local spool and no complete-file reread.
func (s *S3Store) putStreamedObject(repoID, relative string, produce func(io.Writer) error, validate func(io.Reader) error) (stagedS3Transaction, error) {
	reader, writer := io.Pipe()
	completed := make(chan streamedArchiveResult, 1)
	go func() {
		hash := sha256.New()
		counted := &countingWriter{writer: io.MultiWriter(writer, hash)}
		err := produce(counted)
		if err != nil {
			_ = writer.CloseWithError(err)
		} else {
			err = writer.Close()
		}
		sum := hash.Sum(nil)
		completed <- streamedArchiveResult{
			object: stagedS3Transaction{digest: hex.EncodeToString(sum), bytes: counted.bytes},
			rawSum: sum,
			err:    err,
		}
	}()

	ctx, cancel := s.context()
	out, putErr := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(s.key(repoID, relative)),
		Body: reader, IfNoneMatch: aws.String("*"), ContentType: aws.String("application/octet-stream"),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	cancel()
	if putErr != nil {
		_ = reader.CloseWithError(putErr)
	} else {
		_ = reader.Close()
	}
	result := <-completed

	if putErr != nil {
		// A timeout can happen after the provider durably accepted the object.
		// The immutable key makes an existence+checksum read a safe resolution
		// for both ambiguous failures and ordinary idempotent retries.
		existing, inspectErr := s.inspectStreamedObject(repoID, relative, validate)
		if inspectErr == nil {
			return existing, nil
		}
		if isPrecondition(putErr) {
			return stagedS3Transaction{}, fmt.Errorf("inspect existing immutable object after precondition failure: %w", inspectErr)
		}
		return stagedS3Transaction{}, putErr
	}
	if result.err != nil {
		return stagedS3Transaction{}, fmt.Errorf("stream archive: %w", result.err)
	}
	if out != nil && out.ChecksumSHA256 != nil {
		expected := base64.StdEncoding.EncodeToString(result.rawSum)
		if actual := aws.ToString(out.ChecksumSHA256); actual != expected {
			return stagedS3Transaction{}, fmt.Errorf("S3 checksum mismatch for %s: got %s, want %s", relative, actual, expected)
		}
	}
	return result.object, nil
}

func (s *S3Store) inspectStreamedObject(repoID, relative string, validate func(io.Reader) error) (stagedS3Transaction, error) {
	head, err := s.head(repoID, relative)
	if err != nil {
		return stagedS3Transaction{}, err
	}
	if digest := head.Metadata["walgit-sha256"]; digest != "" && validate == nil {
		return stagedS3Transaction{digest: digest, bytes: aws.ToInt64(head.ContentLength)}, nil
	}
	body, err := s.get(repoID, relative)
	if err != nil {
		return stagedS3Transaction{}, err
	}
	hash := sha256.New()
	hashed := &countingWriter{writer: hash}
	var readErr error
	if validate == nil {
		_, readErr = io.Copy(hashed, body)
	} else {
		readErr = validate(io.TeeReader(body, hashed))
		if readErr == nil {
			_, readErr = io.Copy(hashed, body)
		}
	}
	closeErr := body.Close()
	if readErr != nil {
		return stagedS3Transaction{}, readErr
	}
	if closeErr != nil {
		return stagedS3Transaction{}, closeErr
	}
	if expected := aws.ToInt64(head.ContentLength); expected != 0 && hashed.bytes != expected {
		return stagedS3Transaction{}, fmt.Errorf("S3 object length mismatch for %s: got %d, want %d", relative, hashed.bytes, expected)
	}
	return stagedS3Transaction{digest: hex.EncodeToString(hash.Sum(nil)), bytes: hashed.bytes}, nil
}

func OpenS3(location string) (Backend, error) {
	store, err := openS3Single(location, false)
	if err != nil {
		return nil, err
	}
	primary := s3BlobStore{store: store}
	blobs, certificates, err := configureDurability(primary, location)
	if err != nil {
		return nil, err
	}
	if certificates != nil && !store.unconditionalWrites {
		return nil, errors.New("dual-authority certificates require WALGIT_S3_DISABLE_CONDITIONAL_WRITES=true and one repository writer")
	}
	store.blobs = blobs
	store.certificates = certificates
	return store, nil
}

func openS3Single(location string, secondary bool) (*S3Store, error) {
	return openS3Configured(location, secondary, false)
}

func openS3Repair(location string, secondary bool) (*S3Store, error) {
	return openS3Configured(location, secondary, true)
}

func openS3Configured(location string, secondary, repair bool) (*S3Store, error) {
	withoutScheme := strings.TrimPrefix(location, "s3://")
	bucket, prefix, _ := strings.Cut(withoutScheme, "/")
	if bucket == "" {
		return nil, fmt.Errorf("S3 storage URI requires a bucket")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	options := []func(*config.LoadOptions) error{}
	region := os.Getenv("AWS_REGION")
	endpoint := os.Getenv("WALGIT_S3_ENDPOINT")
	pathStyle, _ := strconv.ParseBool(os.Getenv("WALGIT_S3_PATH_STYLE"))
	if secondary {
		if value := os.Getenv("WALGIT_BLOB_SECONDARY_REGION"); value != "" {
			region = value
		}
		if value := os.Getenv("WALGIT_BLOB_SECONDARY_ENDPOINT"); value != "" {
			endpoint = value
		}
		if value := os.Getenv("WALGIT_BLOB_SECONDARY_PATH_STYLE"); value != "" {
			pathStyle, _ = strconv.ParseBool(value)
		}
		accessKey := os.Getenv("WALGIT_BLOB_SECONDARY_ACCESS_KEY_ID")
		secretKey := os.Getenv("WALGIT_BLOB_SECONDARY_SECRET_ACCESS_KEY")
		if (accessKey == "") != (secretKey == "") {
			return nil, errors.New("secondary S3 access key and secret key must be configured together")
		}
		if accessKey != "" {
			options = append(options, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				accessKey, secretKey, os.Getenv("WALGIT_BLOB_SECONDARY_SESSION_TOKEN"),
			)))
		}
	}
	if repair {
		role := "PRIMARY"
		if secondary {
			role = "SECONDARY"
		}
		prefix := "WALGIT_REPAIR_" + role + "_"
		accessKey := os.Getenv(prefix + "ACCESS_KEY_ID")
		secretKey := os.Getenv(prefix + "SECRET_ACCESS_KEY")
		if accessKey == "" || secretKey == "" {
			return nil, fmt.Errorf("S3 repair requires %sACCESS_KEY_ID and %sSECRET_ACCESS_KEY", prefix, prefix)
		}
		options = append(options, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			accessKey, secretKey, os.Getenv(prefix+"SESSION_TOKEN"),
		)))
	}
	if region != "" {
		options = append(options, config.WithRegion(region))
	}
	cfg, err := config.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}
	if endpoint != "" {
		cfg.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	}
	client := s3.NewFromConfig(cfg, func(options *s3.Options) {
		if endpoint != "" {
			options.BaseEndpoint = aws.String(endpoint)
		}
		if pathStyle {
			options.UsePathStyle = true
		}
	})
	unconditional, _ := strconv.ParseBool(os.Getenv("WALGIT_S3_DISABLE_CONDITIONAL_WRITES"))
	return &S3Store{
		client: client, bucket: bucket, prefix: strings.Trim(prefix, "/"), timeout: 2 * time.Minute,
		unconditionalWrites: unconditional, staged: make(map[string]stagedS3Transaction),
	}, nil
}

func (s *S3Store) Initialize(repoID, head, objectFormat string) error {
	if err := validateID(repoID); err != nil {
		return err
	}
	if head == "" {
		head = "refs/heads/main"
	}
	if objectFormat != "sha1" && objectFormat != "sha256" {
		return fmt.Errorf("unsupported object format %q", objectFormat)
	}
	existing, etag, err := s.loadPrimaryWithETag(repoID)
	if err == nil {
		if existing.ObjectFormat != objectFormat || existing.Head != head {
			return fmt.Errorf("existing S3 manifest configuration does not match repository")
		}
		anchored, err := publishAnchor(s.certificates, repoID, existing)
		if err != nil {
			return err
		}
		if anchored.CertificateSHA256 == existing.CertificateSHA256 {
			return nil
		}
		return s.putManifest(repoID, anchored, etag, false)
	}
	if !isNotFound(err) {
		return err
	}
	m := Manifest{Version: manifestVersion, Head: head, ObjectFormat: objectFormat, Refs: map[string]string{}}
	m, err = publishAnchor(s.certificates, repoID, m)
	if err != nil {
		return err
	}
	err = s.putManifest(repoID, m, "", true)
	if isPrecondition(err) {
		existing, _, loadErr := s.loadPrimaryWithETag(repoID)
		if loadErr != nil {
			return loadErr
		}
		if existing.ObjectFormat != objectFormat || existing.Head != head {
			return fmt.Errorf("existing S3 manifest configuration does not match repository")
		}
		return nil
	}
	return err
}

func (s *S3Store) Stage(repoID, objectDir string, updates []RefUpdate) error {
	if err := validateID(repoID); err != nil {
		return err
	}
	txID, err := transactionID(updates)
	if err != nil {
		return err
	}
	var objects []ObjectBlob
	if s.certificates != nil {
		objects, err = storeObjectBlobs(objectDir, s.blobStorage())
	} else {
		objects, err = maybeStoreObjectBlobs(objectDir, s.blobStorage())
	}
	if err != nil {
		return err
	}
	meta := EntryMeta{TransactionID: txID, CreatedAt: time.Now().UTC(), Updates: updates, Objects: objects}
	if len(objects) > 0 || s.certificates != nil {
		meta, err = externalizeEntryMeta(meta, s.blobStorage())
		if err != nil {
			return err
		}
	}
	usedDescriptor := meta.Descriptor
	payloadBytes := objectBlobBytes(objects)
	object, err := s.putStreamedObject(repoID, "transactions/"+txID+".wal", func(w io.Writer) error {
		return writeArchive(w, meta, objectDir)
	}, func(r io.Reader) error {
		recovered, validateErr := readEntryArchiveMeta(r)
		if validateErr != nil {
			return validateErr
		}
		usedDescriptor = recovered.Descriptor
		recovered, validateErr = resolveEntryMeta(recovered, s.blobStorage())
		if validateErr != nil {
			return validateErr
		}
		if recovered.TransactionID != txID {
			return fmt.Errorf("transaction mismatch: got %s, want %s", recovered.TransactionID, txID)
		}
		payloadBytes = objectBlobBytes(recovered.Objects)
		return nil
	})
	if err != nil {
		return err
	}
	object.payloadBytes = payloadBytes
	object.descriptor = usedDescriptor
	s.stateMu.Lock()
	if s.staged == nil {
		s.staged = make(map[string]stagedS3Transaction)
	}
	s.staged[repoID+"/"+txID] = object
	s.stateMu.Unlock()
	return nil
}

func (s *S3Store) Commit(repoID string, updates []RefUpdate) (ManifestEntry, Manifest, error) {
	entries, manifest, err := s.CommitBatch(repoID, [][]RefUpdate{updates})
	if err != nil {
		return ManifestEntry{}, Manifest{}, err
	}
	return entries[0], manifest, nil
}

func (s *S3Store) CommitBatch(repoID string, batches [][]RefUpdate) ([]ManifestEntry, Manifest, error) {
	if len(batches) == 0 {
		return nil, Manifest{}, errors.New("commit batch is empty")
	}
	type stagedBatch struct {
		updates []RefUpdate
		txID    string
		file    string
		object  stagedS3Transaction
	}
	staged := make([]stagedBatch, len(batches))
	for i, updates := range batches {
		txID, err := transactionID(updates)
		if err != nil {
			return nil, Manifest{}, err
		}
		staged[i] = stagedBatch{updates: updates, txID: txID, file: "transactions/" + txID + ".wal"}
	}

	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	for i := range staged {
		if object, ok := s.staged[repoID+"/"+staged[i].txID]; ok {
			staged[i].object = object
			continue
		}
		var recovered EntryMeta
		var descriptor *ChunkRef
		object, err := s.inspectStreamedObject(repoID, staged[i].file, func(r io.Reader) error {
			var validateErr error
			recovered, validateErr = readEntryArchiveMeta(r)
			if validateErr != nil {
				return validateErr
			}
			descriptor = recovered.Descriptor
			recovered, validateErr = resolveEntryMeta(recovered, s.blobStorage())
			if validateErr != nil {
				return validateErr
			}
			if recovered.TransactionID != staged[i].txID {
				return fmt.Errorf("transaction mismatch: got %s, want %s", recovered.TransactionID, staged[i].txID)
			}
			return nil
		})
		if err != nil {
			return nil, Manifest{}, fmt.Errorf("read staged transaction: %w", err)
		}
		staged[i].object = object
		staged[i].object.payloadBytes = objectBlobBytes(recovered.Objects)
		staged[i].object.descriptor = descriptor
		if staged[i].object.digest == "" {
			return nil, Manifest{}, errors.New("staged transaction is missing its SHA-256 metadata")
		}
	}

	for attempt := 0; attempt < 32; attempt++ {
		var m Manifest
		var etag string
		var err error
		if s.unconditionalWrites && s.cachedManifest != nil {
			m, etag = cloneManifest(*s.cachedManifest), s.cachedETag
		} else {
			m, etag, err = s.loadWithETag(repoID)
			if err != nil {
				return nil, Manifest{}, err
			}
		}
		previous := cloneManifest(m)
		entries := make([]ManifestEntry, len(staged))
		var certified []CertificateEntry
		changed := false
		for batchIndex, batch := range staged {
			if updatesAlreadyApplied(m, batch.updates) {
				entry := ManifestEntry{Generation: m.Generation, TransactionID: batch.txID}
				for i := len(m.Entries) - 1; i >= 0; i-- {
					if m.Entries[i].TransactionID == batch.txID {
						entry = m.Entries[i]
						break
					}
				}
				entries[batchIndex] = entry
				continue
			}
			if err := validatePreparedLocks(m, batch.txID, batch.updates); err != nil {
				return nil, Manifest{}, err
			}
			if err := validateUpdates(m, batch.updates); err != nil {
				return nil, Manifest{}, err
			}
			generation := m.Generation + 1
			entry := ManifestEntry{
				Generation: generation, TransactionID: batch.txID, File: batch.file,
				SHA256: batch.object.digest, Bytes: batch.object.bytes, PayloadBytes: batch.object.payloadBytes,
				Descriptor: batch.object.descriptor,
			}
			for _, update := range batch.updates {
				if isZeroOID(update.New) {
					delete(m.Refs, update.Ref)
				} else {
					m.Refs[update.Ref] = update.New
				}
			}
			m.Generation = generation
			m.Entries = append(m.Entries, entry)
			if !s.unconditionalWrites {
				if m.Prepared == nil {
					m.Prepared = make(map[string]PreparedTransaction)
				}
				m.Prepared[batch.txID] = PreparedTransaction{
					TransactionID: batch.txID, Generation: generation, CreatedAt: time.Now().UTC(), Updates: batch.updates,
				}
			}
			entries[batchIndex] = entry
			certified = append(certified, CertificateEntry{Entry: entry, Updates: batch.updates})
			changed = true
		}
		if !changed {
			if s.unconditionalWrites {
				cached := cloneManifest(m)
				s.cachedManifest = &cached
				s.cachedETag = etag
			}
			return entries, cloneManifest(m), nil
		}
		m, err = publishTransition(s.certificates, repoID, previous, m, certified)
		if err != nil {
			return nil, Manifest{}, err
		}
		if err := s.putManifest(repoID, m, etag, false); isPrecondition(err) || isConflict(err) {
			s.cachedManifest = nil
			continue
		} else if err != nil {
			return nil, Manifest{}, err
		}
		if s.unconditionalWrites {
			cached := cloneManifest(m)
			s.cachedManifest = &cached
			s.cachedETag = etag
		}
		return entries, cloneManifest(m), nil
	}
	return nil, Manifest{}, errors.New("S3 manifest remained contended after 32 CAS attempts")
}

func (s *S3Store) Finalize(repoID string, updates []RefUpdate) error {
	if s.unconditionalWrites {
		return nil // Coordinated manifest publication is the durable commit point.
	}
	txID, err := transactionID(updates)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 32; attempt++ {
		m, etag, err := s.loadWithETag(repoID)
		if err != nil {
			return err
		}
		if _, ok := m.Prepared[txID]; !ok {
			return nil
		}
		if err := failpoint("finalize.before_manifest"); err != nil {
			return err
		}
		delete(m.Prepared, txID)
		if err := s.putManifest(repoID, m, etag, false); isPrecondition(err) || isConflict(err) {
			continue
		} else if err != nil {
			return err
		}
		return nil
	}
	return errors.New("S3 manifest remained contended while finalizing transaction")
}

func (s *S3Store) Abort(repoID string, updates []RefUpdate) error {
	if s.unconditionalWrites {
		return s.abortCoordinated(repoID, updates)
	}
	txID, err := transactionID(updates)
	if err != nil {
		return err
	}
	rollbackID := "rollback-" + txID
	transactionFile := "transactions/" + rollbackID + ".wal"
	var rollbackObject *stagedS3Transaction
	for attempt := 0; attempt < 32; attempt++ {
		m, etag, err := s.loadWithETag(repoID)
		if err != nil {
			return err
		}
		if _, ok := m.Prepared[txID]; !ok {
			return nil
		}
		before, after := transactionState(m, updates)
		if before {
			delete(m.Prepared, txID)
			if err := s.putManifest(repoID, m, etag, false); isPrecondition(err) || isConflict(err) {
				continue
			} else {
				return err
			}
		}
		if !after {
			return fmt.Errorf("cannot roll back prepared transaction %s because a touched ref advanced", txID)
		}
		previous := cloneManifest(m)
		inverse := invertUpdates(updates)
		if rollbackObject == nil {
			object, writeErr := s.putMetadataTransaction(repoID, transactionFile, EntryMeta{
				TransactionID: rollbackID, CreatedAt: time.Now().UTC(), Updates: inverse,
			})
			if writeErr != nil {
				return writeErr
			}
			rollbackObject = &object
		}
		if rollbackObject.digest == "" {
			return errors.New("rollback transaction is missing its SHA-256 metadata")
		}
		generation := m.Generation + 1
		for _, update := range inverse {
			if isZeroOID(update.New) {
				delete(m.Refs, update.Ref)
			} else {
				m.Refs[update.Ref] = update.New
			}
		}
		m.Generation = generation
		entry := ManifestEntry{
			Generation: generation, TransactionID: rollbackID, File: transactionFile,
			SHA256: rollbackObject.digest, Bytes: rollbackObject.bytes, Descriptor: rollbackObject.descriptor,
		}
		m.Entries = append(m.Entries, entry)
		delete(m.Prepared, txID)
		m, err = publishTransition(s.certificates, repoID, previous, m, []CertificateEntry{{Entry: entry, Updates: inverse}})
		if err != nil {
			return err
		}
		if err := s.putManifest(repoID, m, etag, false); isPrecondition(err) || isConflict(err) {
			continue
		} else if err != nil {
			return err
		}
		return nil
	}
	return errors.New("S3 manifest remained contended while rolling back transaction")
}

func (s *S3Store) abortCoordinated(repoID string, updates []RefUpdate) error {
	txID, err := transactionID(updates)
	if err != nil {
		return err
	}
	rollbackID := "rollback-" + txID
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	var m Manifest
	var etag string
	if s.cachedManifest != nil {
		m, etag = cloneManifest(*s.cachedManifest), s.cachedETag
	} else {
		m, etag, err = s.loadWithETag(repoID)
		if err != nil {
			return err
		}
	}
	originalFound := false
	for _, entry := range m.Entries {
		if entry.TransactionID == txID {
			originalFound = true
		}
	}
	if !originalFound {
		return nil
	}
	before, after := transactionState(m, updates)
	if before {
		return nil
	}
	if !after {
		return fmt.Errorf("cannot roll back transaction %s because a touched ref advanced", txID)
	}
	previous := cloneManifest(m)
	inverse := invertUpdates(updates)
	transactionFile := "transactions/" + rollbackID + ".wal"
	object, err := s.putMetadataTransaction(repoID, transactionFile, EntryMeta{
		TransactionID: rollbackID, CreatedAt: time.Now().UTC(), Updates: inverse,
	})
	if err != nil {
		return err
	}
	if object.digest == "" {
		return errors.New("rollback transaction is missing its SHA-256 metadata")
	}
	generation := m.Generation + 1
	for _, update := range inverse {
		if isZeroOID(update.New) {
			delete(m.Refs, update.Ref)
		} else {
			m.Refs[update.Ref] = update.New
		}
	}
	m.Generation = generation
	entry := ManifestEntry{
		Generation: generation, TransactionID: rollbackID, File: transactionFile,
		SHA256: object.digest, Bytes: object.bytes, Descriptor: object.descriptor,
	}
	m.Entries = append(m.Entries, entry)
	delete(m.Prepared, txID)
	m, err = publishTransition(s.certificates, repoID, previous, m, []CertificateEntry{{Entry: entry, Updates: inverse}})
	if err != nil {
		return err
	}
	if err := s.putManifest(repoID, m, etag, false); err != nil {
		return err
	}
	cached := cloneManifest(m)
	s.cachedManifest = &cached
	s.cachedETag = etag
	return nil
}

func (s *S3Store) putMetadataTransaction(repoID, transactionFile string, meta EntryMeta) (stagedS3Transaction, error) {
	var err error
	if s.certificates != nil {
		meta, err = externalizeEntryMeta(meta, s.blobStorage())
		if err != nil {
			return stagedS3Transaction{}, err
		}
	}
	usedDescriptor := meta.Descriptor
	object, err := s.putStreamedObject(repoID, transactionFile, func(w io.Writer) error {
		return writeMetadataArchive(w, meta)
	}, func(r io.Reader) error {
		recovered, validateErr := readEntryArchiveMeta(r)
		if validateErr != nil {
			return validateErr
		}
		usedDescriptor = recovered.Descriptor
		recovered, validateErr = resolveEntryMeta(recovered, s.blobStorage())
		if validateErr != nil {
			return validateErr
		}
		if recovered.TransactionID != meta.TransactionID {
			return fmt.Errorf("transaction mismatch: got %s, want %s", recovered.TransactionID, meta.TransactionID)
		}
		return nil
	})
	if err != nil {
		return stagedS3Transaction{}, err
	}
	object.descriptor = usedDescriptor
	return object, nil
}

func (s *S3Store) Load(repoID string) (Manifest, error) {
	if s.unconditionalWrites {
		s.stateMu.Lock()
		defer s.stateMu.Unlock()
		if s.cachedManifest != nil {
			return cloneManifest(*s.cachedManifest), nil
		}
		m, etag, err := s.loadWithETag(repoID)
		if err != nil {
			if s.certificates == nil {
				return Manifest{}, err
			}
			m, _, err = s.certificates.Recover(repoID)
			if err != nil {
				return Manifest{}, fmt.Errorf("primary manifest unavailable and certificate recovery failed: %w", err)
			}
			// Recovery from certificates is sufficient for reads and restore,
			// but it is not a writable primary. Do not cache it as coordinator
			// state or a later commit could bypass primary repair.
			return cloneManifest(m), nil
		}
		cached := cloneManifest(m)
		s.cachedManifest = &cached
		s.cachedETag = etag
		return cloneManifest(m), nil
	}
	m, _, err := s.loadWithETag(repoID)
	if err == nil || s.certificates == nil {
		return m, err
	}
	recovered, _, certificateErr := s.certificates.Recover(repoID)
	if certificateErr != nil {
		return Manifest{}, fmt.Errorf("primary manifest unavailable and certificate recovery failed: %w", certificateErr)
	}
	return recovered, nil
}

func (s *S3Store) ReplayFrom(repoID string, generation uint64, gitObjects string, apply func(ManifestEntry, EntryMeta) error) (Manifest, error) {
	m, err := s.Load(repoID)
	if err != nil {
		return Manifest{}, err
	}
	return s.ReplayManifest(repoID, generation, m, gitObjects, apply)
}

func (s *S3Store) ReplayManifest(repoID string, generation uint64, m Manifest, gitObjects string, apply func(ManifestEntry, EntryMeta) error) (Manifest, error) {
	if generation > m.Generation {
		return Manifest{}, fmt.Errorf("local generation %d is ahead of manifest generation %d", generation, m.Generation)
	}
	if generation < m.CertificateFloor {
		return Manifest{}, fmt.Errorf("local generation %d predates recoverable certificate floor %d", generation, m.CertificateFloor)
	}
	for _, entry := range m.Entries {
		if entry.Generation <= generation {
			continue
		}
		if entry.Descriptor != nil {
			meta, err := replayEntryDescriptor(entry, gitObjects, s.blobStorage())
			if err != nil {
				return Manifest{}, fmt.Errorf("replay descriptor for generation %d: %w", entry.Generation, err)
			}
			if err := apply(entry, meta); err != nil {
				return Manifest{}, fmt.Errorf("apply %s: %w", entry.File, err)
			}
			continue
		}
		body, err := s.get(repoID, entry.File)
		if err != nil {
			return Manifest{}, err
		}
		hash := sha256.New()
		meta, replayErr := readArchive(io.TeeReader(body, hash), gitObjects, s.blobStorage())
		closeErr := body.Close()
		if replayErr != nil {
			return Manifest{}, fmt.Errorf("replay %s: %w", entry.File, replayErr)
		}
		if closeErr != nil {
			return Manifest{}, closeErr
		}
		if got := hex.EncodeToString(hash.Sum(nil)); got != entry.SHA256 {
			return Manifest{}, fmt.Errorf("checksum mismatch for %s", entry.File)
		}
		if meta.TransactionID != entry.TransactionID {
			return Manifest{}, fmt.Errorf("transaction mismatch for %s", entry.File)
		}
		if err := apply(entry, meta); err != nil {
			return Manifest{}, fmt.Errorf("apply %s: %w", entry.File, err)
		}
	}
	return m, nil
}

func (s *S3Store) CreateCheckpoint(repoID, gitObjects string, expectedGeneration uint64) (Checkpoint, error) {
	m, _, err := s.loadWithETag(repoID)
	if err != nil {
		return Checkpoint{}, err
	}
	if m.Generation != expectedGeneration {
		return Checkpoint{}, fmt.Errorf("manifest advanced during checkpoint: expected generation %d, found %d", expectedGeneration, m.Generation)
	}
	if len(m.Prepared) > 0 {
		return Checkpoint{}, errors.New("cannot checkpoint while reference transactions are prepared")
	}
	objects, err := maybeStoreObjectBlobs(gitObjects, s.blobStorage())
	if err != nil {
		return Checkpoint{}, err
	}
	meta := CheckpointMeta{
		Generation: m.Generation, CreatedAt: time.Now().UTC(), Head: m.Head,
		ObjectFormat: m.ObjectFormat, Refs: cloneRefs(m.Refs), Objects: objects,
	}
	if len(objects) > 0 {
		meta, err = externalizeCheckpointMeta(meta, s.blobStorage())
		if err != nil {
			return Checkpoint{}, err
		}
	}
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return Checkpoint{}, fmt.Errorf("generate checkpoint object name: %w", err)
	}
	checkpointFile := fmt.Sprintf("checkpoints/%020d-%s.checkpoint", m.Generation, hex.EncodeToString(nonce[:]))
	object, err := s.putStreamedObject(repoID, checkpointFile, func(w io.Writer) error {
		return writeCheckpointArchive(w, meta, gitObjects)
	}, func(r io.Reader) error {
		return validateCheckpointArchive(r, meta.Generation)
	})
	if err != nil {
		return Checkpoint{}, err
	}
	checkpoint := Checkpoint{
		Generation: m.Generation, File: checkpointFile,
		SHA256: object.digest, Bytes: object.bytes, PayloadBytes: objectBlobBytes(objects),
	}
	for attempt := 0; attempt < 32; attempt++ {
		latest, etag, err := s.loadWithETag(repoID)
		if err != nil {
			return Checkpoint{}, err
		}
		if latest.Checkpoint != nil && latest.Checkpoint.Generation >= checkpoint.Generation {
			checkpoint = *latest.Checkpoint
		} else {
			latest.Checkpoint = &checkpoint
		}
		kept := latest.Entries[:0]
		for _, entry := range latest.Entries {
			if entry.Generation > checkpoint.Generation {
				kept = append(kept, entry)
			}
		}
		latest.Entries = kept
		if err := s.putManifest(repoID, latest, etag, false); isPrecondition(err) || isConflict(err) {
			continue
		} else if err != nil {
			return Checkpoint{}, err
		}
		return checkpoint, nil
	}
	return Checkpoint{}, errors.New("S3 manifest remained contended while publishing checkpoint")
}

func (s *S3Store) RestoreCheckpoint(repoID, gitObjects string) (CheckpointMeta, error) {
	m, err := s.Load(repoID)
	if err != nil {
		return CheckpointMeta{}, err
	}
	if m.Checkpoint == nil {
		return CheckpointMeta{}, errors.New("manifest has no checkpoint")
	}
	body, err := s.get(repoID, m.Checkpoint.File)
	if err != nil {
		return CheckpointMeta{}, err
	}
	hash := sha256.New()
	meta, restoreErr := readCheckpointArchive(io.TeeReader(body, hash), gitObjects, s.blobStorage())
	closeErr := body.Close()
	if restoreErr != nil {
		return CheckpointMeta{}, restoreErr
	}
	if closeErr != nil {
		return CheckpointMeta{}, closeErr
	}
	if got := hex.EncodeToString(hash.Sum(nil)); got != m.Checkpoint.SHA256 {
		return CheckpointMeta{}, fmt.Errorf("checksum mismatch for checkpoint %s", m.Checkpoint.File)
	}
	if meta.Generation != m.Checkpoint.Generation {
		return CheckpointMeta{}, fmt.Errorf("checkpoint generation mismatch: metadata %d, manifest %d", meta.Generation, m.Checkpoint.Generation)
	}
	return meta, nil
}

func (s *S3Store) CleanupPending(string, time.Time) (int, error) { return 0, nil }

func (s *S3Store) GarbageCollect(repoID string, olderThan time.Time) (GCResult, error) {
	m, err := s.Load(repoID)
	if err != nil {
		return GCResult{}, err
	}
	referenced := map[string]bool{s.key(repoID, "manifest.json"): true}
	for _, entry := range m.Entries {
		referenced[s.key(repoID, entry.File)] = true
	}
	if m.Checkpoint != nil {
		referenced[s.key(repoID, m.Checkpoint.File)] = true
	}
	var result GCResult
	var token *string
	for {
		ctx, cancel := s.context()
		out, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(s.bucket), Prefix: aws.String(s.repoPrefix(repoID)), ContinuationToken: token,
		})
		cancel()
		if err != nil {
			return result, err
		}
		for _, object := range out.Contents {
			key := aws.ToString(object.Key)
			if referenced[key] || object.LastModified == nil || !object.LastModified.Before(olderThan) {
				continue
			}
			ctx, cancel := s.context()
			_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
			cancel()
			if err != nil {
				return result, err
			}
			if strings.Contains(key, "/checkpoints/") {
				result.Checkpoints++
			} else if strings.Contains(key, "/transactions/") {
				result.Entries++
			}
		}
		if !aws.ToBool(out.IsTruncated) || out.NextContinuationToken == nil {
			break
		}
		token = out.NextContinuationToken
	}
	return result, nil
}

func (s *S3Store) loadWithETag(repoID string) (Manifest, string, error) {
	m, etag, err := s.loadPrimaryWithETag(repoID)
	if err != nil || s.certificates == nil {
		return m, etag, err
	}
	recovered, _, err := s.certificates.Recover(repoID)
	if err != nil {
		return Manifest{}, "", fmt.Errorf("validate dual-authority certificate chain: %w", err)
	}
	if recovered.Generation > m.Generation {
		return recovered, etag, nil
	}
	if recovered.Generation < m.Generation {
		return Manifest{}, "", errors.New("primary manifest is ahead of the dual-authority certificate chain")
	}
	if m.CertificateSHA256 != recovered.CertificateSHA256 {
		return Manifest{}, "", errors.New("primary manifest and dual-authority certificate chain disagree")
	}
	return m, etag, nil
}

func (s *S3Store) loadPrimaryWithETag(repoID string) (Manifest, string, error) {
	ctx, cancel := s.context()
	defer cancel()
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(s.key(repoID, "manifest.json")),
	})
	if err != nil {
		return Manifest{}, "", err
	}
	defer out.Body.Close()
	data, err := io.ReadAll(io.LimitReader(out.Body, 64<<20))
	if err != nil {
		return Manifest{}, "", err
	}
	m, err := decodeManifest(data)
	return m, aws.ToString(out.ETag), err
}

func (s *S3Store) putManifest(repoID string, m Manifest, etag string, create bool) error {
	data, err := marshalManifest(m)
	if err != nil {
		return err
	}
	input := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(s.key(repoID, "manifest.json")),
		Body: strings.NewReader(string(data)), ContentLength: aws.Int64(int64(len(data))), ContentType: aws.String("application/json"),
	}
	if !s.unconditionalWrites {
		if create {
			input.IfNoneMatch = aws.String("*")
		} else {
			input.IfMatch = aws.String(etag)
		}
	}
	ctx, cancel := s.context()
	defer cancel()
	_, err = s.client.PutObject(ctx, input)
	return err
}

func (s *S3Store) get(repoID, relative string) (io.ReadCloser, error) {
	ctx, cancel := s.context()
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(s.key(repoID, relative)),
	})
	if err != nil {
		cancel()
		return nil, err
	}
	return &cancelReadCloser{ReadCloser: out.Body, cancel: cancel}, nil
}

func (s *S3Store) head(repoID, relative string) (*s3.HeadObjectOutput, error) {
	ctx, cancel := s.context()
	defer cancel()
	return s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(s.key(repoID, relative)),
	})
}

func (s *S3Store) key(repoID, relative string) string {
	return path.Join(s.prefix, repoID, filepath.ToSlash(relative))
}

func (s *S3Store) repoPrefix(repoID string) string { return s.key(repoID, "") + "/" }

func DeleteS3Repository(location, repoID string) error {
	if !strings.HasPrefix(location, "s3://") {
		return errors.New("S3 cleanup requires an s3:// location")
	}
	_, prefix, ok := strings.Cut(strings.TrimPrefix(location, "s3://"), "/")
	if !ok {
		return errors.New("refusing S3 cleanup without an isolated prefix")
	}
	isolated := false
	for _, segment := range strings.Split(prefix, "/") {
		if strings.HasPrefix(segment, "walgit-benchmark-") {
			isolated = true
			break
		}
	}
	if !isolated {
		return errors.New("refusing S3 cleanup outside a walgit-benchmark-* prefix")
	}
	backend, err := OpenS3(location)
	if err != nil {
		return err
	}
	store, ok := backend.(*S3Store)
	if !ok {
		return errors.New("S3 cleanup opened an unexpected backend")
	}
	return store.deleteRepository(repoID)
}

func (s *S3Store) deleteRepository(repoID string) error {
	if err := validateID(repoID); err != nil {
		return err
	}
	var token *string
	var keys []string
	for {
		ctx, cancel := s.context()
		out, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(s.bucket), Prefix: aws.String(s.repoPrefix(repoID)), ContinuationToken: token,
		})
		cancel()
		if err != nil {
			return err
		}
		for _, object := range out.Contents {
			keys = append(keys, aws.ToString(object.Key))
		}
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		token = out.NextContinuationToken
	}
	for _, key := range keys {
		ctx, cancel := s.context()
		_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *S3Store) context() (context.Context, context.CancelFunc) {
	timeout := s.timeout
	if timeout == 0 {
		timeout = 2 * time.Minute
	}
	return context.WithTimeout(context.Background(), timeout)
}

func isPrecondition(err error) bool { return apiErrorCode(err, "PreconditionFailed", 412) }
func isConflict(err error) bool     { return apiErrorCode(err, "ConditionalRequestConflict", 409) }
func isNotFound(err error) bool {
	return apiErrorCode(err, "NoSuchKey", 404) || apiErrorCode(err, "NotFound", 404)
}

func apiErrorCode(err error, code string, status int) bool {
	if err == nil {
		return false
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == code {
		return true
	}
	var responseErr *awshttp.ResponseError
	return errors.As(err, &responseErr) && responseErr.HTTPStatusCode() == status
}

func marshalManifest(m Manifest) ([]byte, error) {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func decodeManifest(data []byte) (Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return Manifest{}, err
	}
	if m.Version != manifestVersion {
		return Manifest{}, fmt.Errorf("unsupported manifest version %d", m.Version)
	}
	if m.Refs == nil {
		m.Refs = map[string]string{}
	}
	return m, nil
}

type cancelReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (r *cancelReadCloser) Close() error {
	err := r.ReadCloser.Close()
	r.cancel()
	return err
}
