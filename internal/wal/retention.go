package wal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const retentionReportVersion = 1

type RetentionPolicy struct {
	Minimum time.Duration
	Mode    types.ObjectLockRetentionMode
}

type AuthorityRetentionReport struct {
	Name        string       `json:"name"`
	Location    string       `json:"location"`
	Healthy     bool         `json:"healthy"`
	Versioning  string       `json:"versioning,omitempty"`
	ObjectLock  string       `json:"object_lock,omitempty"`
	DefaultMode string       `json:"default_mode,omitempty"`
	DefaultDays int64        `json:"default_days,omitempty"`
	MinimumDays int64        `json:"minimum_days"`
	Issues      []ScrubIssue `json:"issues,omitempty"`
}

type RetentionReport struct {
	Version     int                        `json:"version"`
	Healthy     bool                       `json:"healthy"`
	PolicyMode  string                     `json:"policy_mode"`
	MinimumDays int64                      `json:"minimum_days"`
	Authorities []AuthorityRetentionReport `json:"authorities"`
	Issues      []ScrubIssue               `json:"issues,omitempty"`
}

type s3RetentionClient interface {
	GetBucketVersioning(context.Context, *s3.GetBucketVersioningInput, ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error)
	GetObjectLockConfiguration(context.Context, *s3.GetObjectLockConfigurationInput, ...func(*s3.Options)) (*s3.GetObjectLockConfigurationOutput, error)
}

func CheckRetention(location string, minimum time.Duration, mode string) (RetentionReport, error) {
	policy, err := newRetentionPolicy(minimum, mode)
	if err != nil {
		return RetentionReport{Version: retentionReportVersion}, err
	}
	authorities, err := openMaintenanceAuthorities(location, false)
	if err != nil {
		return newRetentionReport(policy), err
	}
	report := checkRetentionAuthorities(authorities, policy)
	if !report.Healthy {
		return report, errors.New("retention policy check failed")
	}
	return report, nil
}

func checkMaintenanceRetention(authorities []namedMaintenanceAuthority) (*RetentionReport, error) {
	hasS3 := false
	for _, authority := range authorities {
		if _, ok := authority.authority.(s3BlobStore); ok {
			hasS3 = true
			break
		}
	}
	if !hasS3 {
		return nil, nil
	}
	allowUnprotected, _ := strconv.ParseBool(os.Getenv("WALGIT_ALLOW_UNPROTECTED_S3"))
	if allowUnprotected {
		return nil, nil
	}
	policy, err := retentionPolicyFromEnvironment()
	if err != nil {
		return nil, err
	}
	report := checkRetentionAuthorities(authorities, policy)
	if !report.Healthy {
		return &report, errors.New("dual-authority retention policy is not satisfied")
	}
	return &report, nil
}

func enforceConfiguredRetention(certificates certificateStore) error {
	replicated, ok := certificates.(replicatedCertificateStore)
	if !ok || len(replicated.authorities) != 2 {
		return errors.New("retention enforcement requires exactly two certificate authorities")
	}
	policy, err := retentionPolicyFromEnvironment()
	if err != nil {
		return err
	}
	authorities := make([]namedMaintenanceAuthority, 0, len(replicated.authorities))
	for index, certificateAuthority := range replicated.authorities {
		authority, ok := certificateAuthority.(maintenanceAuthority)
		if !ok {
			return errors.New("certificate authority cannot be checked for retention")
		}
		name := "primary"
		if index == 1 {
			name = "secondary"
		}
		location := name
		if s3Authority, ok := authority.(s3BlobStore); ok {
			location = "s3://" + s3Authority.store.bucket
			if s3Authority.store.prefix != "" {
				location += "/" + s3Authority.store.prefix
			}
		}
		authorities = append(authorities, namedMaintenanceAuthority{name: name, location: location, authority: authority})
	}
	report := checkRetentionAuthorities(authorities, policy)
	if !report.Healthy {
		var failures []string
		for _, authority := range report.Authorities {
			for _, issue := range authority.Issues {
				failures = append(failures, authority.Name+": "+issue.Message)
			}
		}
		return fmt.Errorf("dual-authority retention policy is not satisfied: %s", strings.Join(failures, "; "))
	}
	return configureRetentionFromReport(authorities, report)
}

func configureRetentionFromReport(authorities []namedMaintenanceAuthority, report RetentionReport) error {
	if len(authorities) != len(report.Authorities) {
		return errors.New("retention report does not match configured authorities")
	}
	for index, authority := range authorities {
		s3Authority, ok := authority.authority.(s3BlobStore)
		if !ok {
			return fmt.Errorf("%s authority cannot enforce S3 retention", authority.name)
		}
		duration, err := retentionDaysDuration(report.Authorities[index].DefaultDays)
		if err != nil {
			return fmt.Errorf("%s authority: %w", authority.name, err)
		}
		s3Authority.store.configureRetention(RetentionPolicy{
			Minimum: duration, Mode: types.ObjectLockRetentionMode(report.Authorities[index].DefaultMode),
		})
	}
	return nil
}

func retentionPolicyFromEnvironment() (RetentionPolicy, error) {
	minimumText := strings.TrimSpace(os.Getenv("WALGIT_MIN_RETENTION"))
	if minimumText == "" {
		return RetentionPolicy{}, errors.New("dual-authority S3 requires WALGIT_MIN_RETENTION, for example 720h")
	}
	minimum, err := time.ParseDuration(minimumText)
	if err != nil {
		return RetentionPolicy{}, fmt.Errorf("parse WALGIT_MIN_RETENTION: %w", err)
	}
	mode := strings.TrimSpace(os.Getenv("WALGIT_RETENTION_MODE"))
	if mode == "" {
		mode = "compliance"
	}
	return newRetentionPolicy(minimum, mode)
}

func newRetentionPolicy(minimum time.Duration, mode string) (RetentionPolicy, error) {
	if minimum <= 0 {
		return RetentionPolicy{}, errors.New("minimum retention must be positive")
	}
	var retentionMode types.ObjectLockRetentionMode
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case "COMPLIANCE":
		retentionMode = types.ObjectLockRetentionModeCompliance
	case "GOVERNANCE":
		retentionMode = types.ObjectLockRetentionModeGovernance
	default:
		return RetentionPolicy{}, errors.New("retention mode must be compliance or governance")
	}
	return RetentionPolicy{Minimum: minimum, Mode: retentionMode}, nil
}

func newRetentionReport(policy RetentionPolicy) RetentionReport {
	return RetentionReport{
		Version: retentionReportVersion, PolicyMode: string(policy.Mode),
		MinimumDays: retentionMinimumDays(policy.Minimum),
	}
}

func checkRetentionAuthorities(authorities []namedMaintenanceAuthority, policy RetentionPolicy) RetentionReport {
	report := newRetentionReport(policy)
	report.Authorities = make([]AuthorityRetentionReport, len(authorities))
	type result struct {
		index  int
		report AuthorityRetentionReport
	}
	results := make(chan result, len(authorities))
	for index, authority := range authorities {
		go func() {
			results <- result{index: index, report: checkRetentionAuthority(authority, policy)}
		}()
	}
	for range authorities {
		checked := <-results
		report.Authorities[checked.index] = checked.report
	}
	if len(authorities) != 2 {
		report.Issues = append(report.Issues, ScrubIssue{Kind: "configuration", Message: "exactly two authorities are required"})
		return report
	}
	for _, authority := range report.Authorities {
		if !authority.Healthy {
			report.Issues = append(report.Issues, ScrubIssue{Kind: "retention", Object: authority.Name, Message: "authority does not satisfy retention policy"})
			return report
		}
	}
	report.Healthy = true
	return report
}

func checkRetentionAuthority(named namedMaintenanceAuthority, policy RetentionPolicy) AuthorityRetentionReport {
	report := AuthorityRetentionReport{
		Name: named.name, Location: named.location, MinimumDays: retentionMinimumDays(policy.Minimum),
	}
	authority, ok := named.authority.(s3BlobStore)
	if !ok {
		report.Issues = append(report.Issues, ScrubIssue{Kind: "unsupported", Message: "authority is not S3-compatible and has no enforceable Object Lock policy"})
		return report
	}
	client, ok := authority.store.client.(s3RetentionClient)
	if !ok {
		report.Issues = append(report.Issues, ScrubIssue{Kind: "unsupported", Message: "S3 client does not expose retention policy APIs"})
		return report
	}
	ctx, cancel := authority.store.context()
	versioning, err := client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(authority.store.bucket)})
	cancel()
	if err != nil {
		report.Issues = append(report.Issues, ScrubIssue{Kind: "versioning", Message: err.Error()})
		return report
	}
	report.Versioning = string(versioning.Status)
	if versioning.Status != types.BucketVersioningStatusEnabled {
		report.Issues = append(report.Issues, ScrubIssue{Kind: "versioning", Message: "bucket versioning is not enabled"})
		return report
	}
	ctx, cancel = authority.store.context()
	lock, err := client.GetObjectLockConfiguration(ctx, &s3.GetObjectLockConfigurationInput{Bucket: aws.String(authority.store.bucket)})
	cancel()
	if err != nil {
		report.Issues = append(report.Issues, ScrubIssue{Kind: "object-lock", Message: err.Error()})
		return report
	}
	configuration := lock.ObjectLockConfiguration
	if configuration == nil {
		report.Issues = append(report.Issues, ScrubIssue{Kind: "object-lock", Message: "bucket has no Object Lock configuration"})
		return report
	}
	report.ObjectLock = string(configuration.ObjectLockEnabled)
	if configuration.ObjectLockEnabled != types.ObjectLockEnabledEnabled {
		report.Issues = append(report.Issues, ScrubIssue{Kind: "object-lock", Message: "Object Lock is not enabled"})
		return report
	}
	if configuration.Rule == nil || configuration.Rule.DefaultRetention == nil {
		report.Issues = append(report.Issues, ScrubIssue{Kind: "default-retention", Message: "bucket has no default retention rule"})
		return report
	}
	retention := configuration.Rule.DefaultRetention
	report.DefaultMode = string(retention.Mode)
	days, err := defaultRetentionDays(retention)
	if err != nil {
		report.Issues = append(report.Issues, ScrubIssue{Kind: "default-retention", Message: err.Error()})
		return report
	}
	report.DefaultDays = days
	if _, err := retentionDaysDuration(days); err != nil {
		report.Issues = append(report.Issues, ScrubIssue{Kind: "default-retention", Message: err.Error()})
		return report
	}
	if !retentionModeSatisfies(retention.Mode, policy.Mode) {
		report.Issues = append(report.Issues, ScrubIssue{
			Kind: "default-retention", Message: fmt.Sprintf("default mode is %s, want %s", retention.Mode, policy.Mode),
		})
		return report
	}
	if days < report.MinimumDays {
		report.Issues = append(report.Issues, ScrubIssue{
			Kind: "default-retention", Message: fmt.Sprintf("default retention is %d days, want at least %d", days, report.MinimumDays),
		})
		return report
	}
	report.Healthy = true
	return report
}

func retentionModeSatisfies(actual, required types.ObjectLockRetentionMode) bool {
	return actual == required || (required == types.ObjectLockRetentionModeGovernance && actual == types.ObjectLockRetentionModeCompliance)
}

func defaultRetentionDays(retention *types.DefaultRetention) (int64, error) {
	if retention == nil || (retention.Days == nil) == (retention.Years == nil) {
		return 0, errors.New("default retention must specify exactly one of days or years")
	}
	if retention.Days != nil {
		if *retention.Days <= 0 {
			return 0, errors.New("default retention days must be positive")
		}
		return int64(*retention.Days), nil
	}
	if *retention.Years <= 0 {
		return 0, errors.New("default retention years must be positive")
	}
	return int64(*retention.Years) * 365, nil
}

func retentionMinimumDays(minimum time.Duration) int64 {
	const day = 24 * time.Hour
	days := int64(minimum / day)
	if minimum%day != 0 {
		days++
	}
	return days
}

func retentionDaysDuration(days int64) (time.Duration, error) {
	const day = 24 * time.Hour
	const maximumDays = int64(^uint64(0)>>1) / int64(day)
	if days <= 0 || days > maximumDays {
		return 0, fmt.Errorf("retention duration of %d days cannot be represented safely", days)
	}
	return time.Duration(days) * day, nil
}
