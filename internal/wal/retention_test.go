package wal

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type retentionMemoryS3 struct {
	*memoryS3
	versioning types.BucketVersioningStatus
	lock       *types.ObjectLockConfiguration
	versionErr error
	lockErr    error
}

type retentionCapturingS3 struct {
	*memoryS3
	modes map[string]types.ObjectLockMode
	until map[string]time.Time
}

func (s *retentionCapturingS3) PutObject(ctx context.Context, input *s3.PutObjectInput, options ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	key := aws.ToString(input.Key)
	s.modes[key] = input.ObjectLockMode
	if input.ObjectLockRetainUntilDate != nil {
		s.until[key] = *input.ObjectLockRetainUntilDate
	}
	return s.memoryS3.PutObject(ctx, input, options...)
}

func (s *retentionMemoryS3) GetBucketVersioning(context.Context, *s3.GetBucketVersioningInput, ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error) {
	if s.versionErr != nil {
		return nil, s.versionErr
	}
	return &s3.GetBucketVersioningOutput{Status: s.versioning}, nil
}

func (s *retentionMemoryS3) GetObjectLockConfiguration(context.Context, *s3.GetObjectLockConfigurationInput, ...func(*s3.Options)) (*s3.GetObjectLockConfigurationOutput, error) {
	if s.lockErr != nil {
		return nil, s.lockErr
	}
	return &s3.GetObjectLockConfigurationOutput{ObjectLockConfiguration: s.lock}, nil
}

func TestRetentionCheckAcceptsMatchingCompliancePolicy(t *testing.T) {
	policy, err := newRetentionPolicy(30*24*time.Hour, "compliance")
	if err != nil {
		t.Fatal(err)
	}
	authorities := retentionTestAuthorities(45, types.ObjectLockRetentionModeCompliance)
	report := checkRetentionAuthorities(authorities, policy)
	if !report.Healthy || report.MinimumDays != 30 || len(report.Authorities) != 2 {
		t.Fatalf("unexpected retention report: %#v", report)
	}
	for _, authority := range report.Authorities {
		if !authority.Healthy || authority.Versioning != "Enabled" || authority.ObjectLock != "Enabled" || authority.DefaultDays != 45 {
			t.Fatalf("unexpected authority policy: %#v", authority)
		}
	}
}

func TestRetentionCheckRejectsWeakOrUnavailablePolicy(t *testing.T) {
	policy, err := newRetentionPolicy(30*24*time.Hour, "compliance")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*retentionMemoryS3)
		want   string
	}{
		{name: "versioning suspended", mutate: func(client *retentionMemoryS3) {
			client.versioning = types.BucketVersioningStatusSuspended
		}, want: "versioning is not enabled"},
		{name: "object lock unavailable", mutate: func(client *retentionMemoryS3) {
			client.lockErr = errors.New("not implemented")
		}, want: "not implemented"},
		{name: "governance mode", mutate: func(client *retentionMemoryS3) {
			client.lock.Rule.DefaultRetention.Mode = types.ObjectLockRetentionModeGovernance
		}, want: "want COMPLIANCE"},
		{name: "too short", mutate: func(client *retentionMemoryS3) {
			days := int32(29)
			client.lock.Rule.DefaultRetention.Days = &days
		}, want: "want at least 30"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			authorities := retentionTestAuthorities(45, types.ObjectLockRetentionModeCompliance)
			client := authorities[1].authority.(s3BlobStore).store.client.(*retentionMemoryS3)
			testCase.mutate(client)
			report := checkRetentionAuthorities(authorities, policy)
			if report.Healthy || report.Authorities[1].Healthy {
				t.Fatalf("weak policy passed: %#v", report)
			}
			messages := ""
			for _, issue := range report.Authorities[1].Issues {
				messages += issue.Message
			}
			if !strings.Contains(messages, testCase.want) {
				t.Fatalf("issues %q do not contain %q", messages, testCase.want)
			}
		})
	}
}

func TestConfiguredRetentionRequiresExplicitMinimum(t *testing.T) {
	authorities := retentionTestAuthorities(45, types.ObjectLockRetentionModeCompliance)
	certificates := replicatedCertificateStore{authorities: []certificateAuthority{
		authorities[0].authority, authorities[1].authority,
	}}
	t.Setenv("WALGIT_MIN_RETENTION", "")
	if err := enforceConfiguredRetention(certificates); err == nil || !strings.Contains(err.Error(), "WALGIT_MIN_RETENTION") {
		t.Fatalf("retention enforcement accepted an implicit window: %v", err)
	}
	t.Setenv("WALGIT_MIN_RETENTION", "720h")
	t.Setenv("WALGIT_RETENTION_MODE", "compliance")
	if err := enforceConfiguredRetention(certificates); err != nil {
		t.Fatal(err)
	}
	for _, authority := range authorities {
		store := authority.authority.(s3BlobStore).store
		if store.retentionMode != types.ObjectLockModeCompliance || store.retentionFor != 45*24*time.Hour {
			t.Fatalf("retention was not applied to immutable writes: mode=%s duration=%s", store.retentionMode, store.retentionFor)
		}
		input := &s3.PutObjectInput{}
		before := time.Now().UTC().Add(45 * 24 * time.Hour)
		store.applyRetention(input)
		if input.ObjectLockMode != types.ObjectLockModeCompliance || input.ObjectLockRetainUntilDate == nil || input.ObjectLockRetainUntilDate.Before(before) {
			t.Fatalf("immutable PutObject has no enforced retention: %#v", input)
		}
	}
}

func TestRetentionMinimumRoundsUpPartialDays(t *testing.T) {
	if got := retentionMinimumDays(24*time.Hour + time.Nanosecond); got != 2 {
		t.Fatalf("minimum retention days = %d, want 2", got)
	}
}

func TestComplianceSatisfiesGovernanceRequirement(t *testing.T) {
	policy, err := newRetentionPolicy(24*time.Hour, "governance")
	if err != nil {
		t.Fatal(err)
	}
	if report := checkRetentionAuthorities(retentionTestAuthorities(2, types.ObjectLockRetentionModeCompliance), policy); !report.Healthy {
		t.Fatalf("stronger compliance retention did not satisfy governance policy: %#v", report)
	}
}

func TestImmutableS3ObjectsCarryExplicitRetention(t *testing.T) {
	client := &retentionCapturingS3{memoryS3: newMemoryS3(), modes: make(map[string]types.ObjectLockMode), until: make(map[string]time.Time)}
	store := &S3Store{client: client, bucket: "bucket", prefix: "walgit", timeout: time.Second}
	store.configureRetention(RetentionPolicy{Minimum: 30 * 24 * time.Hour, Mode: types.ObjectLockRetentionModeCompliance})
	authority := s3BlobStore{store: store}
	payload := []byte("retained payload")
	ref := chunkRef(payload)
	if err := authority.Put(ref, payload); err != nil {
		t.Fatal(err)
	}
	certificate := certificateObjectFor(anchorCertificate("repo", Manifest{Head: "refs/heads/main", ObjectFormat: "sha1", Refs: map[string]string{}}))
	if err := authority.PutCertificate("repo", certificate.Generation, certificate.SHA256, certificate.Data); err != nil {
		t.Fatal(err)
	}
	minimum := time.Now().UTC().Add(30*24*time.Hour - time.Minute)
	for _, key := range []string{store.blobKey(ref.SHA256), authority.certificateKey("repo", certificate.Generation, certificate.SHA256)} {
		if client.modes[key] != types.ObjectLockModeCompliance || client.until[key].Before(minimum) {
			t.Fatalf("immutable object %s has mode=%s until=%s", key, client.modes[key], client.until[key])
		}
	}
}

func retentionTestAuthorities(days int32, mode types.ObjectLockRetentionMode) []namedMaintenanceAuthority {
	makeAuthority := func(name string) namedMaintenanceAuthority {
		retentionDays := days
		client := &retentionMemoryS3{
			memoryS3: newMemoryS3(), versioning: types.BucketVersioningStatusEnabled,
			lock: &types.ObjectLockConfiguration{
				ObjectLockEnabled: types.ObjectLockEnabledEnabled,
				Rule: &types.ObjectLockRule{DefaultRetention: &types.DefaultRetention{
					Days: &retentionDays, Mode: mode,
				}},
			},
		}
		store := &S3Store{client: client, bucket: name, prefix: "walgit", timeout: time.Second}
		return namedMaintenanceAuthority{name: name, location: "s3://" + name + "/walgit", authority: s3BlobStore{store: store}}
	}
	return []namedMaintenanceAuthority{makeAuthority("primary"), makeAuthority("secondary")}
}
