# walgit

`walgit` is a benchmark implementation of a WAL-first Git storage engine,
inspired by Cursor's Continuity architecture. Normal bare Git repositories are
disposable serving caches; one versioned WAL index and its immutable WAL
entries are the authoritative repository state.

Licensed under the [MIT License](LICENSE).

The enforced crash-safety guarantees and exact failure behavior are described
in [the reliability model](docs/reliability.md). The protocol intentionally has
no database, witness, or cross-provider consensus operation in the normal push
path.

The filesystem backend provides a local correctness and performance baseline.
The S3-compatible backend streams each push into one immutable WAL object, then
linearizes the WAL-index update with ETag compare-and-swap. Stores without
conditional `PutObject` must put one explicitly fenced writer in front of each
repository. Content-addressed chunk replication remains an opt-in experiment;
it is no longer imposed on the default push path.

For AWS deployment and S3 Express One Zone benchmarking, see
[Fast S3 on AWS](docs/aws-fast-s3.md). S3 Standard is the current multi-AZ
recommendation; Express is the low-latency experiment until acknowledged
commits are mirrored into a second AZ.

## Build and benchmark

```sh
go build -o ./bin/walgit ./cmd/walgit
./bin/walgit bench -pushes 100 -blob-bytes 65536
./bin/walgit bench -nodes 2 -pushes 100 -blob-bytes 65536
```

To benchmark a live S3-compatible store, pass only a base prefix. The benchmark
adds a cryptographically random `walgit-benchmark-*` child prefix and deletes
that child when it finishes:

```sh
./bin/walgit bench \
  -nodes 2 \
  -pushes 100 \
  -blob-bytes 65536 \
  -store s3://company-git/benchmarks \
  -persistent-writer
```

Cleanup is deliberately refused unless the generated path contains a
`walgit-benchmark-*` segment. Cleanup includes repository-local data and the
global content-addressed blobs and certificates inside that generated child.
Use `-keep` only when the isolated objects should remain for inspection.

The benchmark reports mean, p50, p90, p99, and maximum latency for plain Git
and WAL-backed Git. It then:

1. Restores an empty repository from the complete WAL.
2. Verifies it with `git fsck --strict`.
3. Creates a compacted, independently verified checkpoint.
4. Restores another repository from that checkpoint.
5. Confirms that all branch tips match the serving repository.

With `-nodes 2` or greater, the benchmark instead creates disposable cache
nodes over one authoritative store. By default, sequential pushes rotate
across nodes. `-persistent-writer` starts a protected local coordinator, keeps
pushes on one primary, caches staged metadata and manifest state, and batches
concurrent manifest commits. Every result is immediately read through a
different node. The benchmark reconciles every cache, runs `git fsck --strict`,
and verifies the final manifest generation and ref set. Its report separates
push latency, cross-node read-after-write latency, concurrent latency, and
writer batch effectiveness.

## Benchmark results

These are development measurements, not service-level guarantees. Raw Git is
included as a latency floor; its default local configuration does **not** offer
the same acknowledged-commit durability as walgit's fsynced WAL path.

### Raw Git versus walgit

One run on 2026-09-07 on Apple Silicon (`arm64`, macOS 26.2) against the
corrected single-publication build used 100 sequential pushes with 64 KiB
changed per push. Both destinations were local; walgit used its fsynced
filesystem WAL backend. The retired result is retained to make the regression
and correction visible.

| Local push target | Mean | p50 | p90 | p99 | Worst | Pushes/s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Raw bare Git, default configuration | **50.30 ms** | **50.01 ms** | **51.61 ms** | **55.67 ms** | **56.74 ms** | **19.88** |
| walgit, corrected one-publication path | 147.18 ms | 142.00 ms | 147.93 ms | 158.53 ms | 603.82 ms | 6.79 |
| walgit, corrected path + persistent writer | 149.87 ms | 144.24 ms | 151.38 ms | 163.59 ms | 687.36 ms | 6.67 |
| walgit, retired two-publication path (`2bed065`) | 166.96 ms | 157.16 ms | 168.09 ms | 276.47 ms | 739.66 ms | 5.99 |

The corrected direct path is 2.93x the raw-Git mean, or 192.6% overhead. It is
11.9% faster by mean and 9.6% faster at p50 than the retired path. A persistent
writer did not help this sequential local workload; it was 1.8% slower by mean.
Its purpose is S3 connection/metadata reuse and concurrent group commit, not
speeding up one-at-a-time filesystem pushes. Reconstructing and strictly
verifying the corrected 100-push repository took 3.78 s; checkpoint creation
took 4.46 s and checkpoint restoration took 2.20 s. The restored and serving
heads matched.

Reproduce this comparison with:

```sh
go build -o ./bin/walgit ./cmd/walgit
./bin/walgit bench -pushes 100 -blob-bytes 65536
./bin/walgit bench -pushes 100 -blob-bytes 65536 -persistent-writer
```

Additional measured results appear below:

- [4 MiB in-memory S3 stage benchmark](#4-mib-in-memory-s3-stage-benchmark),
  isolating streaming, hashing, chunking, and fanout costs.
- [DigitalOcean Spaces end-to-end benchmark](#digitalocean-spaces-end-to-end-benchmark),
  including same-region push and cross-node latency.

The historical [dual-authority benchmark](docs/disaster-drills.md#dual-authority-latency-benchmark)
is retained for experimental comparison. Its certificate protocol is not the
normal Cursor-style architecture and its results must not be presented as
normal walgit performance.

## Repository lifecycle

Create a repository and durable store:

```sh
walgit init -repo /srv/git/origin.git -store /srv/walgit -id origin
```

Every push is staged by `pre-receive` while Git's objects are quarantined. The
`reference-transaction` hook publishes it only in Git's `prepared` phase, after
the refs are locked but before they are committed or acknowledged. That one
index publication is the durable commit point. The later `committed` hook only
marks the local cache current and performs no durable-store write.

The gateway reconciles a local cache to the authoritative manifest before
starting Git. It can be used by SSH as a forced command, or directly with Git:

```sh
git push \
  --receive-pack="/usr/local/bin/walgit gateway -store /srv/walgit -id origin -service receive-pack" \
  /srv/git/origin.git HEAD:refs/heads/main

git clone \
  --upload-pack="/usr/local/bin/walgit gateway -store /srv/walgit -id origin -service upload-pack" \
  /srv/git/origin.git
```

A cache can also be reconciled or reconstructed explicitly:

```sh
walgit reconcile -repo /srv/git/origin.git -store /srv/walgit -id origin
walgit restore -repo /srv/git/restored.git -store /srv/walgit -id origin
```

### Historical certificate operations

Dual-authority repositories can be verified and conservatively repaired with
machine-readable commands. These commands apply only to the historical
certificate experiment. Repair requires an explicit, fully verified source:

```sh
export WALGIT_BLOB_SECONDARY_STORE=/mnt/independent-b/walgit
walgit scrub -store /mnt/independent-a/walgit -id origin
walgit repair -store /mnt/independent-a/walgit -id origin -source primary
walgit drill -store /mnt/independent-a/walgit -id origin -source secondary
```

For S3, repair additionally requires the dedicated
`WALGIT_REPAIR_PRIMARY_*` and `WALGIT_REPAIR_SECONDARY_*` credentials described
in the [runbook](docs/scrub-repair.md). Stop the repository writer throughout
repair.

Strict dual-authority S3 also requires an explicit retention window and checks
that both buckets have versioning and a sufficient Object Lock default. It adds
the configured retention directly to immutable certificate and blob writes:

```sh
export WALGIT_MIN_RETENTION=720h
export WALGIT_RETENTION_MODE=compliance
walgit retention-check \
  -store s3://independent-a/repositories \
  -minimum 720h \
  -mode compliance
```

See the [retention policy](docs/retention.md). The explicit
`WALGIT_ALLOW_UNPROTECTED_S3=true` escape hatch is for benchmarks only and
invalidates provider-loss durability claims.

To measure live durability cost rather than the filesystem implementation,
`bench-dual` compares local raw Git, one S3 authority, replicated blobs, and
full dual-authority certificate commits. It then scrubs both authorities and
independently restores and fscks from each:

```sh
walgit bench-dual \
  -primary s3://independent-a/benchmarks \
  -secondary s3://independent-b/benchmarks \
  -pushes 100 \
  -blob-bytes 65536 \
  -allow-unprotected
```

The explicit unprotected mode is disposable performance testing only. A real
Object Lock run requires the retention environment above plus `-keep`, because
the benchmark must not pretend it can delete retention-locked versions. See
the [disaster drill and benchmark guide](docs/disaster-drills.md).

## S3-compatible storage

Use an `s3://bucket/prefix` store URI. Credentials and region use the standard
AWS SDK configuration chain:

```sh
export AWS_REGION=us-west-2
walgit init \
  -repo /srv/git/origin.git \
  -store s3://company-git/walgit \
  -id origin
```

For MinIO and other S3-compatible services:

```sh
export WALGIT_S3_ENDPOINT=http://minio.internal:9000
export WALGIT_S3_PATH_STYLE=true
```

The bucket identity needs `GetObject`, `PutObject`, `HeadObject`,
`ListBucket`, and `DeleteObject` for the configured prefix. Strict retention
checks additionally need `GetBucketVersioning` and
`GetObjectLockConfiguration`, and immutable uploads must be allowed to set
Object Lock retention. Disposable benchmark cleanup also needs
`ListBucketVersions` and permission to delete versions under its generated
prefix. S3 conditional writes must be supported; a losing
manifest writer receives a precondition failure, reloads the winner, and
retries against the new ETag.

Every normal WAL transaction is generated directly into one bounded stream
while its SHA-256 digest is calculated from the same bytes:

```text
Git refs locked and objects quarantined
                 │
                 ▼
       immutable WAL object in S3
                 │
                 ▼
 GET index + ETag ──► validate expected refs
                 │
                 ▼
       conditional PUT index (If-Match)
                 │
                 ▼
      acknowledge; local repo is a cache
```

Concurrent writers race the same conditional PUT. One wins; each loser reloads
the index and retries against its new ETag. A persistent coordinator can append
several consecutive entries with one conditional index update. There is no
second finalize publication and no external database or witness.

Neither S3 path uses a local temporary archive or local `fsync`. Uploads ask
S3 to validate SHA-256 and compare a returned checksum when the provider
supplies one. If an upload times out after the provider may have accepted it,
walgit reads and verifies the immutable object before continuing; it never
blindly overwrites it. A restarted writer performs the same recovery when its
in-memory staged-object metadata is absent. Replay verifies every chunk and
the reconstructed whole-file hash.

To experiment with content-addressed payload replication, configure both an
explicit secondary and the replication requirement:

```sh
export WALGIT_BLOB_SECONDARY_STORE=s3://independent-account/walgit-blobs
export WALGIT_REQUIRE_BLOB_REPLICATION=true
```

The secondary uses the normal AWS configuration unless these overrides are
set: `WALGIT_BLOB_SECONDARY_REGION`, `WALGIT_BLOB_SECONDARY_ENDPOINT`,
`WALGIT_BLOB_SECONDARY_PATH_STYLE`,
`WALGIT_BLOB_SECONDARY_ACCESS_KEY_ID`,
`WALGIT_BLOB_SECONDARY_SECRET_ACCESS_KEY`, and
`WALGIT_BLOB_SECONDARY_SESSION_TOKEN`. The same secondary can instead be a
filesystem path or `file://` URI. A configured one-sided write fails the push;
reads try the primary, verify the hash, and fall back to the secondary on a
missing or corrupt chunk.

This option externalizes every transaction into 8 MiB chunks and a descriptor;
it is not automatic in the normal path. It protects the payload data plane but
does not replicate the authoritative index, so it is not by itself a complete
provider-loss design. See [the blob-store experiment](docs/blob-store.md).

### Historical dual-authority experiment

The earlier hash-chained certificate implementation and `bench-dual` command
remain available for comparison and disaster-recovery research:

```sh
export WALGIT_BLOB_SECONDARY_STORE=s3://independent-account/walgit
export WALGIT_REQUIRE_DUAL_AUTHORITY=true
export WALGIT_S3_DISABLE_CONDITIONAL_WRITES=true
export WALGIT_WRITER_SOCKET=/run/walgit/origin.sock
export WALGIT_MIN_RETENTION=720h
export WALGIT_RETENTION_MODE=compliance
```

This is not the recommended or default architecture. It replaces S3 CAS with a
single externally serialized writer and adds both providers to the commit path;
it therefore cannot demonstrate Cursor-style any-node writes and has materially
higher latency. Each experimental commit certificate
contains its generation, previous certificate hash, certified transaction
entries, resulting refs, repository configuration, and descriptor hashes. The
identical immutable certificate must reach both authorities before the primary
manifest can publish and Git can receive success. Certificates live under
`.walgit-certificates/<repo-id>/<generation>-<sha256>.cert`.

The mutable manifest is then an acceleration cache rather than the only record
of ordering. If it and the primary WAL namespace disappear, walgit rebuilds the
manifest from the surviving validated certificate chain and replays objects
directly from certified descriptors. When both authorities are readable, only
their highest common valid chain is accepted, preventing a one-sided write from
becoming authoritative. S3 certificate mode requires the included single
repository writer because two unrelated object stores cannot atomically CAS one
shared chain. See [the certificate protocol](docs/certificates.md) and
[the reliability model](docs/reliability.md). New performance work must use the
single-authority CAS path unless a benchmark explicitly studies this
experiment.

Existing repositories receive an explicit legacy anchor at their current
generation. Fresh disaster restore before that generation is refused because
older objects were not retrospectively replicated; start new strict repositories
in dual-authority mode or retain a verified migration checkpoint. Blob and
certificate garbage collection remain disabled. Use a separate store prefix per
tenant or security domain; cross-tenant deduplication can leak whether content
already exists.

### 4 MiB in-memory S3 stage benchmark

The included benchmarks isolate archive/blob construction and upload-body
delivery with a 4 MiB pack and in-memory S3 sinks. Three runs of the corrected
build on an Apple M1 Max measured the following; this intentionally excludes
network latency, TLS, and provider processing:

| S3 stage implementation | Mean time | Throughput | Allocations |
| --- | ---: | ---: | ---: |
| Historical temporary file + sync + two rereads | 13.80 ms | 304 MB/s | 88 |
| Current normal one-pass monolithic stream | **2.44 ms** | **1,716 MB/s** | 82 |
| Opt-in content-addressed chunks, one authority | 3.74 ms | 1,122 MB/s | 133 |
| Content-addressed chunks, two concurrent authorities | 3.80 ms | 1,105 MB/s | 183 |

The normal monolithic path is 5.6x faster than the original temporary-archive
path in this CPU/memory-only test. Opt-in chunking adds about 53% relative to
the normal path. Concurrent fanout added about 1.6% with memory sinks; over a
real network, acknowledgement waits for the slower authority. This stage
microbenchmark excludes network latency and the index read/CAS. Run the
variants on the target host with:

```sh
go test ./internal/wal -run '^$' \
  -bench 'BenchmarkS3StageFourMiB' -benchmem -count 5
```

The [DigitalOcean Spaces API reference](https://docs.digitalocean.com/reference/api/spaces/)
does not expose destination `If-Match` as a supported `PutObject` header, so
Spaces cannot provide this manifest compare-and-swap. Run one writer per
repository, or serialize writers through a primary/coordinator, and enable the
explicit compatibility mode:

```sh
export AWS_REGION=nyc3
export WALGIT_S3_ENDPOINT=https://nyc3.digitaloceanspaces.com
./bin/walgit bench \
  -nodes 2 \
  -pushes 100 \
  -blob-bytes 65536 \
  -store s3://company-git/benchmarks \
  -s3-single-writer \
  -persistent-writer
```

`-s3-single-writer` disables conditional manifest replacement; it is unsafe if
two independent coordinators can write the same repository concurrently.
`-persistent-writer` allows concurrent clients to safely share one coordinator
and group commit. Immutable WAL objects, checksums, reconstruction, and
cross-node reads remain enabled.

For a long-running repository, start the coordinator on the primary and expose
its owner-only Unix socket to the Git gateway and hooks:

```sh
export AWS_REGION=nyc3
export WALGIT_S3_ENDPOINT=https://nyc3.digitaloceanspaces.com
export WALGIT_S3_DISABLE_CONDITIONAL_WRITES=true
export WALGIT_WRITER_SOCKET=/run/walgit/origin.sock

walgit writer \
  -socket "$WALGIT_WRITER_SOCKET" \
  -store s3://company-git/walgit \
  -id origin \
  -batch-window 5ms \
  -batch-maximum 64
```

The coordinator is repository-scoped. It retains one backend session, avoids
redundant transaction and manifest reads, treats manifest publication as the
durable commit point, and appends a compensating WAL entry if Git explicitly
aborts afterward. Run only one coordinator for a Spaces-backed repository.
The `gateway` or `serve` process and its Git hooks must inherit the same
`WALGIT_WRITER_SOCKET` value. Protect the socket as owner-only and supervise the
writer as a required dependency: if it is configured but unavailable, Git
requests fail closed instead of bypassing serialization.

### DigitalOcean Spaces end-to-end benchmark

The following two-node runs used 100 pushes with 64 KiB changed per push. The
same-region runs executed on a temporary 2-vCPU machine in `nyc3` beside the
Space:

| Configuration | p50 | p90 | p99 | Worst |
| --- | ---: | ---: | ---: | ---: |
| Push, Los Angeles to `nyc3`, direct hooks | 2.97 s | 3.41 s | 3.68 s | 5.57 s |
| Push, same region, direct hooks | 509 ms | 655 ms | 764 ms | 766 ms |
| Push, same region, persistent writer | **196 ms** | **245 ms** | **267 ms** | **279 ms** |
| Cross-node read, same region, direct hooks | 170 ms | 206 ms | 258 ms | 328 ms |
| Cross-node read, same region, persistent writer | **108 ms** | **146 ms** | **182 ms** | **188 ms** |

The persistent writer reduced same-region push p50 by 61% and p99 by 65%. An
eight-client burst grouped eight simultaneous commits into four manifest
writes, with a largest batch of four. Every run reconstructed all updates,
passed `git fsck --strict`, and removed its isolated object prefix. These are
development measurements, not a DigitalOcean service-wide benchmark.

## Smart HTTP

Set the authentication token through the environment so it does not appear in
the process list:

```sh
export WALGIT_HTTP_TOKEN='replace-with-a-secret'
walgit serve \
  -listen 127.0.0.1:8080 \
  -repo /srv/git/origin.git \
  -store s3://company-git/walgit \
  -id origin
```

The Git URL is `http://127.0.0.1:8080/origin.git`. Basic-auth passwords and
Bearer tokens are accepted. Clone and fetch can optionally be public with
`-anonymous-read`; pushes always require the token. Put TLS or a trusted reverse
proxy in front of the server outside a private development network.

For repository-scoped read/write access, store token digests in a policy file
instead of storing plaintext tokens. Generate a digest without putting the
token in a command-line argument:

```sh
printf %s "$WALGIT_HTTP_TOKEN" | shasum -a 256
```

```json
{
  "version": 1,
  "repositories": {
    "origin": [
      {
        "name": "developer-read",
        "sha256": "<64 lowercase or uppercase hex characters>",
        "read": true,
        "write": false
      },
      {
        "name": "ci-write",
        "sha256": "<64 lowercase or uppercase hex characters>",
        "read": true,
        "write": true
      }
    ]
  }
}
```

Start the server with `-auth-file /etc/walgit/auth.json`. Multiple digests can
coexist during rotation. `WALGIT_HTTP_TOKEN` remains supported and, when set,
grants both read and write access to the repository served by that process.

The unauthenticated liveness and readiness probes are
`/_walgit/health/live` and `/_walgit/health/ready`. Prometheus-format metrics
are exposed at `/metrics` and require a token with read access.

### Multi-repository host

One process can route independent repositories using a JSON configuration:

```json
{
  "version": 1,
  "authorization_file": "auth.json",
  "repositories": [
    {
      "id": "frontend",
      "repository": "caches/frontend.git",
      "store": "s3://company-git/walgit",
      "maintenance_interval": "5m",
      "scrub_interval": "5m",
      "compact_after_entries": 100,
      "compact_after_bytes": 1073741824,
      "gc_grace": "24h",
      "idle_cache_after": "6h"
    },
    {
      "id": "backend",
      "repository": "caches/backend.git",
      "store": "s3://company-git/walgit"
    }
  ]
}
```

Relative cache, filesystem-store, and authorization paths are resolved relative
to the configuration file. Start it with:

```sh
walgit serve -listen 127.0.0.1:8080 -config /etc/walgit/host.json
```

The URLs are `/frontend.git` and `/backend.git`. Each repository has its own
serving lock, authorization grants, metrics, maintenance schedule, and local
cache lifecycle. Aggregate readiness checks every configured cache and durable
manifest. `/metrics` only includes repositories for which the supplied token
has read permission.

## Compaction and garbage collection

Compaction clones a consistent snapshot, verifies its refs and objects, writes
a checkpoint, and atomically updates the manifest. WAL entries covered by the
checkpoint are removed from the manifest but retained on disk for the GC grace
period.

```sh
walgit compact -repo /srv/git/origin.git -store /srv/walgit -id origin
walgit gc -store /srv/walgit -id origin -grace 24h
```

`walgit serve` runs maintenance every five minutes by default. It checkpoints
after either 100 tail WAL entries or 1 GiB of tail WAL data and garbage-collects
objects older than the 24-hour grace period. These settings can be changed with
`-maintenance-interval`, `-compact-after-entries`, `-compact-after-bytes`, and
`-gc-grace`.

Idle local caches can be made disposable with, for example,
`-idle-cache-after 6h`. Eviction runs under the repository's exclusive serving
lock, first moves the old cache to a recovery path, initializes an empty cache,
and only then removes the old data. Startup detects an interrupted eviction and
either restores the old cache or completes the new one. The next Git request
reconstructs refs and objects from the latest checkpoint plus WAL tail.

## Consistency model

- Each manifest update has a monotonically increasing generation.
- The manifest contains authoritative refs, symbolic `HEAD`, and object format.
- Transactions are immutable and idempotent.
- Expected old ref values are checked against the manifest, not only local Git.
- Reference-transaction notifications for symbolic `HEAD`, emitted by newer
  Git versions such as 2.43, are ignored while every actual `refs/*` update
  remains strictly parsed and validated against the staged transaction.
- The WAL index is the single durable commit point. Its one conditional update
  both orders and publishes the push.
- Git's later `committed` notification performs no durable-store I/O. If the
  local cache aborts after index publication, reconciliation repairs or replaces
  that cache; it does not roll back authoritative history.
- A lost response after a successful conditional PUT is an unknown client
  outcome, not an unknown repository state. The index is authoritative and the
  transaction identity makes retry idempotent.
- Older version-2 manifests may contain `prepared` records from the retired
  two-publication protocol. New normal commits clear them; they are not used as
  locks by the current protocol.
- A local generation marker is written only after the cache matches the
  manifest.
- Reconciliation is idempotent across crashes between object extraction, ref
  updates, and generation-marker writes.
- Replayed loose objects and pack components are fsynced to temporary files and
  atomically renamed. Every affected object directory is then fsynced before
  the generation marker advances, so neither a partial object nor a lost
  rename can masquerade as a current cache.
- Serving repositories use `core.fsync=committed` with
  `core.fsyncMethod=fsync`; objects and references represented by a durable
  local generation marker therefore use real cache-flush semantics, including
  on macOS.
- Git's unsynchronized automatic GC and maintenance are disabled on serving
  caches. Scheduled compaction performs a controlled cache repack while holding
  that repository's exclusive serving lock.
- Transient pack locks, keep files, and replay temporary files are never copied
  into WAL entries or checkpoints.
- WAL and checkpoint contents are protected by SHA-256 checksums.
- Explicit blob-replication mode uses ordered 8 MiB content-addressed chunks
  plus a whole-file checksum. Both configured chunk authorities must
  acknowledge every chunk and descriptor before index publication.
- Filesystem manifest publication uses an advisory lock plus atomic rename.
  File contents are synced before rename and the containing directory is
  synced afterward. A checkpoint directory is synced before a manifest may
  durably reference the new checkpoint.
- Any filesystem WAL write, close, file-sync, directory-sync, or
  persistence-class rename error poisons the entire store. The current process refuses all later operations,
  even if a subsequent `fsync` would appear to succeed, and writes a
  best-effort `.walgit-poisoned` sentinel so independent hook processes also
  fail closed. Do not simply delete this marker: repair or replace the storage,
  reconstruct and verify the store from an independent authority, then remove
  it as an explicit operator recovery action. This fail-stop rule follows the
  recovery model documented after PostgreSQL's
  [fsyncgate investigation](https://wiki.postgresql.org/wiki/Fsync_Errors).
- S3 manifest publication normally uses `If-Match` ETag CAS; immutable object
  creation uses `If-None-Match: *`. The explicit compatibility mode replaces
  the manifest unconditionally and therefore requires the repository-scoped
  writer coordinator or equivalent external serialization.

Set `WALGIT_FAILPOINT` to a comma-separated selection of `stage.after_sync`,
`commit.after_entry_rename`, `commit.after_manifest`,
`s3.stage.before_wal_upload`, `s3.stage.after_wal_upload`,
`s3.commit.before_index_cas`, or `s3.commit.after_index_cas` to inject failures
at normal durability boundaries. The integration suite verifies orphan safety,
the authoritative index, lost-success retry, and reconstruction behavior.
Additional rollback/finalize failpoints remain for the historical certificate
experiment.

## Current boundary

This is still a prototype rather than a complete multi-tenant Git host.
Authorization policies contain static token digests rather than an
identity-provider integration. Nodes sharing a conditional-write-capable S3
store converge on every request and preserve disjoint concurrent updates.
DigitalOcean Spaces uses the included per-repository writer coordinator because
its object API cannot perform the required manifest CAS. The coordinator does
not yet have distributed leases or automatic primary failover, so operators
must ensure that only one instance is active for a Spaces-backed repository.
One S3 bucket is still one administrative failure domain. The normal prototype
does not yet mirror the complete WAL and committed index history into a second
independently restorable authority. Versioning, Object Lock, restricted
credentials, continuous verification, and tested backups are deployment
requirements, not reasons to claim literal impossibility of data loss.

The historical `WALGIT_REQUIRE_DUAL_AUTHORITY` experiment can independently
replay one surviving certificate authority, and its scrub/repair/drill tools
remain in the repository. It is deliberately excluded from the normal hot path
because it abandons any-node S3 CAS, requires a single writer, and regressed
latency. A future provider-loss design should mirror immutable WAL and committed
index snapshots without becoming a second ordering authority.

There is no built-in service discovery, rendezvous-hash router, gossip, or
cache warming. The system also has no Git LFS protocol integration,
admission-control
quotas, or TLS termination. Scheduled maintenance is intentionally serialized
with Git traffic per repository, so a large checkpoint can temporarily
increase that repository's request latency.
