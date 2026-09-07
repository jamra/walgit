# S3 retention policy

Dual copies only protect data while failures are detected and repaired before
they overlap. Strict dual-authority S3 therefore fails closed unless both
buckets prove all of the following:

- bucket versioning is enabled;
- Object Lock is enabled;
- a default retention rule exists;
- its mode satisfies the configured policy; and
- its duration is at least the configured detection and recovery window.

Set the policy explicitly before opening a strict S3 repository:

```sh
export WALGIT_MIN_RETENTION=720h
export WALGIT_RETENTION_MODE=compliance
```

`compliance` is the default mode when `WALGIT_RETENTION_MODE` is omitted.
Governance mode must be requested explicitly and is weaker because sufficiently
privileged identities can bypass it. A compliance rule satisfies a governance
requirement, but the reverse does not.

Check a deployment without writing any object:

```sh
export WALGIT_BLOB_SECONDARY_STORE=s3://independent-b/repositories
walgit retention-check \
  -store s3://independent-a/repositories \
  -minimum 720h \
  -mode compliance
```

The command returns a versioned JSON report and exits nonzero unless both
authorities satisfy the policy. Its credentials need permission for
`GetBucketVersioning` and `GetObjectLockConfiguration` in addition to the
object permissions already required by walgit.

After a successful startup check, every immutable chunk and certificate upload
also carries the authority's checked default Object Lock mode and duration as
an explicit retain-until time. This preserves a bucket default that is stronger
than the requested minimum rather than shortening it. Repair objects and
replicated repair audits receive the same protection. Mutable manifests and
disposable WAL archives are not part of the independently replayable
certificate root and do not receive per-object retention.

For local experiments against an S3-compatible service without Object Lock,
`WALGIT_ALLOW_UNPROTECTED_S3=true` bypasses startup enforcement. This setting is
unsafe: a deployment using it must not claim provider-loss durability. It is
intended only for benchmarks and compatibility testing.

## What the check cannot prove

The API can verify bucket configuration, but it cannot prove that the two
accounts are independently administered, that no human can change a policy,
or that a provider will honor its contract through every catastrophe. Put the
authorities in separate administrative and credential domains, deny policy
changes to foreground and repair identities, alert on configuration drift, and
keep offline disaster-recovery evidence.

Retention is deliberately rounded up to whole days when compared with an S3
default rule. Years are conservatively counted as 365 days. The explicit
per-object timestamp uses the checked authority default.
