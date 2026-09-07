package wal

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func BenchmarkS3StageFourMiB(b *testing.B) {
	benchmarkS3StageFourMiB(b, 1)
}

func BenchmarkS3StageFourMiBContentAddressed(b *testing.B) {
	benchmarkS3StageFourMiB(b, 2)
}

func BenchmarkS3StageFourMiBDualAuthority(b *testing.B) {
	benchmarkS3StageFourMiB(b, 3)
}

func benchmarkS3StageFourMiB(b *testing.B, mode int) {
	client := &discardPutS3{memoryS3: newMemoryS3()}
	store := &S3Store{client: client, bucket: "bucket", prefix: "benchmark", timeout: time.Minute}
	if mode >= 2 {
		store.externalizeTransactions = true
		store.blobs = s3BlobStore{store: store}
	}
	if mode == 3 {
		secondaryClient := &discardPutS3{memoryS3: newMemoryS3()}
		secondary := &S3Store{client: secondaryClient, bucket: "secondary", prefix: "benchmark", timeout: time.Minute}
		store.blobs = replicatedBlobStore{stores: []blobStore{
			s3BlobStore{store: store},
			s3BlobStore{store: secondary},
		}}
	}
	objects := filepath.Join(b.TempDir(), "objects")
	if err := os.MkdirAll(filepath.Join(objects, "pack"), 0o755); err != nil {
		b.Fatal(err)
	}
	const objectBytes = 4 << 20
	if err := os.WriteFile(filepath.Join(objects, "pack", "objects.pack"), bytes.Repeat([]byte("x"), objectBytes), 0o444); err != nil {
		b.Fatal(err)
	}
	updates := []RefUpdate{{Old: strings.Repeat("0", 40), New: strings.Repeat("a", 40), Ref: "refs/heads/main"}}
	b.SetBytes(objectBytes)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := store.Stage("repo", objects, updates); err != nil {
			b.Fatal(err)
		}
	}
}

type discardPutS3 struct{ *memoryS3 }

func (s *discardPutS3) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if _, err := io.Copy(io.Discard, input.Body); err != nil {
		return nil, err
	}
	return &s3.PutObjectOutput{}, nil
}
