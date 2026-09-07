package wal

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type s3BlobStore struct{ store *S3Store }

func (s s3BlobStore) Put(ref ChunkRef, data []byte) error {
	if err := validateChunkRef(ref); err != nil {
		return err
	}
	if int64(len(data)) != ref.Bytes {
		return fmt.Errorf("chunk %s has %d bytes, want %d", ref.SHA256, len(data), ref.Bytes)
	}
	decoded, _ := hex.DecodeString(ref.SHA256)
	checksum := base64.StdEncoding.EncodeToString(decoded)
	ctx, cancel := s.store.context()
	input := &s3.PutObjectInput{
		Bucket: aws.String(s.store.bucket), Key: aws.String(s.store.blobKey(ref.SHA256)),
		Body: bytes.NewReader(data), ContentLength: aws.Int64(ref.Bytes), IfNoneMatch: aws.String("*"),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256, ChecksumSHA256: aws.String(checksum),
		Metadata: map[string]string{"walgit-sha256": ref.SHA256}, ContentType: aws.String("application/octet-stream"),
	}
	s.store.applyRetention(input)
	out, putErr := s.store.client.PutObject(ctx, input)
	cancel()
	if putErr != nil {
		existing, getErr := s.Get(ref)
		if getErr == nil && len(existing) == len(data) {
			return nil
		}
		if isPrecondition(putErr) {
			return fmt.Errorf("existing immutable chunk failed validation: %w", getErr)
		}
		return putErr
	}
	if out != nil && out.ChecksumSHA256 != nil && aws.ToString(out.ChecksumSHA256) != checksum {
		return fmt.Errorf("S3 checksum mismatch for chunk %s", ref.SHA256)
	}
	return nil
}

func (s s3BlobStore) Get(ref ChunkRef) ([]byte, error) {
	if err := validateChunkRef(ref); err != nil {
		return nil, err
	}
	ctx, cancel := s.store.context()
	out, err := s.store.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.store.bucket), Key: aws.String(s.store.blobKey(ref.SHA256)),
	})
	if err != nil {
		cancel()
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(out.Body, ref.Bytes+1))
	closeErr := out.Body.Close()
	cancel()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if err := validateChunk(ref, data); err != nil {
		return nil, err
	}
	return data, nil
}

func (s *S3Store) blobStorage() blobStore {
	if s.blobs != nil {
		return s.blobs
	}
	return s3BlobStore{store: s}
}

func (s *S3Store) blobKey(digest string) string {
	prefix := "invalid"
	if len(digest) >= 2 {
		prefix = digest[:2]
	}
	return path.Join(s.prefix, ".walgit-blobs", "sha256", prefix, digest)
}
