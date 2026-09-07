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

func (s s3BlobStore) PutCertificate(repoID string, generation uint64, digest string, data []byte) error {
	if err := validateID(repoID); err != nil {
		return err
	}
	if err := validateCertificateData(digest, data); err != nil {
		return err
	}
	decoded, _ := hex.DecodeString(digest)
	checksum := base64.StdEncoding.EncodeToString(decoded)
	key := s.certificateKey(repoID, generation, digest)
	ctx, cancel := s.store.context()
	out, putErr := s.store.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.store.bucket), Key: aws.String(key), Body: bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))), IfNoneMatch: aws.String("*"),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256, ChecksumSHA256: aws.String(checksum),
		Metadata: map[string]string{"walgit-sha256": digest}, ContentType: aws.String("application/json"),
	})
	cancel()
	if putErr != nil {
		existing, getErr := s.getCertificate(key, digest)
		if getErr == nil && bytes.Equal(existing, data) {
			return nil
		}
		if isPrecondition(putErr) {
			return fmt.Errorf("existing immutable certificate failed validation: %w", getErr)
		}
		return putErr
	}
	if out != nil && out.ChecksumSHA256 != nil && aws.ToString(out.ChecksumSHA256) != checksum {
		return fmt.Errorf("S3 checksum mismatch for certificate %s", digest)
	}
	return nil
}

func (s s3BlobStore) ListCertificates(repoID string) ([]certificateObject, error) {
	if err := validateID(repoID); err != nil {
		return nil, err
	}
	prefix := s.certificatePrefix(repoID)
	var objects []certificateObject
	var token *string
	for {
		ctx, cancel := s.store.context()
		out, err := s.store.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(s.store.bucket), Prefix: aws.String(prefix), ContinuationToken: token,
		})
		cancel()
		if err != nil {
			return nil, err
		}
		for _, item := range out.Contents {
			key := aws.ToString(item.Key)
			generation, digest, ok := certificateIdentityFromName(path.Base(key))
			if !ok {
				return nil, fmt.Errorf("invalid certificate key %q", key)
			}
			data, err := s.getCertificate(key, digest)
			if err != nil {
				return nil, err
			}
			objects = append(objects, certificateObject{Generation: generation, SHA256: digest, Data: data})
		}
		if !aws.ToBool(out.IsTruncated) || out.NextContinuationToken == nil {
			break
		}
		token = out.NextContinuationToken
	}
	return objects, nil
}

func (s s3BlobStore) GetCertificate(repoID string, generation uint64, digest string) ([]byte, error) {
	if err := validateID(repoID); err != nil {
		return nil, err
	}
	if err := validateSHA256(digest); err != nil {
		return nil, err
	}
	return s.getCertificate(s.certificateKey(repoID, generation, digest), digest)
}

func (s s3BlobStore) getCertificate(key, digest string) ([]byte, error) {
	ctx, cancel := s.store.context()
	out, err := s.store.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.store.bucket), Key: aws.String(key),
	})
	if err != nil {
		cancel()
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(out.Body, maximumCertificateBytes+1))
	closeErr := out.Body.Close()
	cancel()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if err := validateCertificateData(digest, data); err != nil {
		return nil, err
	}
	return data, nil
}

func (s s3BlobStore) certificatePrefix(repoID string) string {
	return path.Join(s.store.prefix, ".walgit-certificates", repoID) + "/"
}

func (s s3BlobStore) certificateKey(repoID string, generation uint64, digest string) string {
	return s.certificatePrefix(repoID) + certificateName(generation, digest)
}

var _ certificateAuthority = s3BlobStore{}
