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

func (s s3BlobStore) RepairChunk(ref ChunkRef, data []byte) error {
	if err := validateChunk(ref, data); err != nil {
		return err
	}
	return s.putRepairObject(s.store.blobKey(ref.SHA256), ref.SHA256, data, "application/octet-stream", false)
}

func (s s3BlobStore) RepairCertificate(repoID string, generation uint64, digest string, data []byte) error {
	if err := validateID(repoID); err != nil {
		return err
	}
	if err := validateCertificateData(digest, data); err != nil {
		return err
	}
	return s.putRepairObject(s.certificateKey(repoID, generation, digest), digest, data, "application/json", false)
}

func (s s3BlobStore) PutRepairAudit(repoID, name, digest string, data []byte) error {
	if err := validateID(repoID); err != nil {
		return err
	}
	if err := validateRepairAudit(name, digest, data); err != nil {
		return err
	}
	key := path.Join(s.store.prefix, ".walgit-repair-audit", repoID, name)
	if err := s.putRepairObject(key, digest, data, "application/json", true); err != nil {
		return err
	}
	ctx, cancel := s.store.context()
	existing, err := s.store.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.store.bucket), Key: aws.String(key),
	})
	if err != nil {
		cancel()
		return err
	}
	verified, readErr := io.ReadAll(io.LimitReader(existing.Body, int64(len(data))+1))
	closeErr := existing.Body.Close()
	cancel()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	if !bytes.Equal(verified, data) || sha256Hex(verified) != digest {
		return fmt.Errorf("S3 repair audit verification failed for %s", key)
	}
	return nil
}

func (s s3BlobStore) putRepairObject(key, digest string, data []byte, contentType string, immutable bool) error {
	decoded, _ := hex.DecodeString(digest)
	checksum := base64.StdEncoding.EncodeToString(decoded)
	input := &s3.PutObjectInput{
		Bucket: aws.String(s.store.bucket), Key: aws.String(key), Body: bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))), ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
		ChecksumSHA256: aws.String(checksum), Metadata: map[string]string{"walgit-sha256": digest},
		ContentType: aws.String(contentType),
	}
	if immutable {
		input.IfNoneMatch = aws.String("*")
	}
	s.store.applyRetention(input)
	ctx, cancel := s.store.context()
	out, err := s.store.client.PutObject(ctx, input)
	cancel()
	if err != nil {
		if immutable {
			// Resolve an idempotent retry without replacing an audit record.
			ctx, cancel := s.store.context()
			existing, getErr := s.store.client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(s.store.bucket), Key: aws.String(key),
			})
			if getErr == nil {
				// Audit records are small; the generic certificate limit is ample.
				buf, readErr := io.ReadAll(io.LimitReader(existing.Body, int64(len(data))+1))
				closeErr := existing.Body.Close()
				cancel()
				if readErr == nil && closeErr == nil && bytes.Equal(buf, data) {
					return nil
				}
			} else {
				cancel()
			}
		}
		return err
	}
	if out != nil && out.ChecksumSHA256 != nil && aws.ToString(out.ChecksumSHA256) != checksum {
		return fmt.Errorf("S3 checksum mismatch for repaired object %s", key)
	}
	return nil
}

var _ maintenanceAuthority = s3BlobStore{}
