# Reliability model

Walgit's normal protocol follows one rule:

> An index may reference a transaction only after its immutable WAL object is
> durably accepted and verified by the configured S3 authority.

The WAL index is the only ordering authority. Bare Git repositories are caches.
There is no PostgreSQL database, witness, certificate quorum, or cross-provider
consensus operation in the normal push path.

## Normal commit protocol

```text
Git prepares and locks refs
          │
          ▼
stream immutable WAL object to S3 and hash it
          │
          ▼
GET index and ETag; validate expected old refs
          │
          ▼
conditional PUT(index, If-Match: ETag)
          │
          ▼
index is committed ──► Git finishes its local cache transaction
```

The WAL object contains the reference transaction and quarantined Git objects.
Its key is immutable and derived from the transaction identity. A retry after a
timeout reads, parses, and hashes an existing object before accepting it.

The conditional index update is atomic. Concurrent writers may upload
independent WAL objects, but only one can replace a particular ETag. A loser
reloads the winning index, validates its expected old refs, and retries. A
persistent coordinator may put several consecutive entries into one index CAS.

The later Git `committed` hook does not publish anything remotely. If the local
Git transaction fails after the index CAS, the index remains authoritative and
the disposable cache is reconciled or rebuilt. Rolling the index back would be
unsafe because another writer may already have committed a successor.

## Crash boundaries

| Failure boundary | Authoritative result |
| --- | --- |
| Before WAL upload | No transaction exists and the index is unchanged |
| During WAL upload | No index reference; an incomplete provider request fails |
| After WAL upload, before index CAS | Verified orphan WAL; index unchanged; retry is safe |
| CAS loses to another writer | Reload, revalidate, and retry; no lost update |
| CAS succeeds but its response is lost | Index contains the commit; retry finds the transaction and performs no write |
| Local Git cache aborts after CAS | Index remains committed; reconcile repairs the cache |
| Process exits before client acknowledgement | Client outcome may be unknown, but repository state is determined by the index |

Deterministic tests cover the before/after WAL and before/after index boundaries,
lost-success retry, independent-writer CAS races, and zero-I/O finalization.

## Filesystem backend and fsync errors

The filesystem backend is a correctness and performance baseline, not the
distributed production design. It publishes a durable file by:

1. writing a temporary file and checking every write;
2. syncing and closing the file;
3. atomically renaming it;
4. syncing the containing directory; and
5. publishing the manifest only after the WAL is durable.

Any write, close, file-sync, directory-sync, or persistence-class rename error
poisons the store. The current process refuses later operations and writes a
best-effort `.walgit-poisoned` marker so new processes also fail closed. A later
successful sync is not evidence that an earlier failed writeback survived. This
is the class of failure described by PostgreSQL's
[fsyncgate investigation](https://wiki.postgresql.org/wiki/Fsync_Errors).

Recovery means replacing or repairing the faulty storage, reconstructing and
verifying it from an independent copy, and only then explicitly removing the
poison marker. Marker removal must not be automated as error recovery.

## S3 assumptions and limits

The normal S3 mode depends on:

- strong read-after-write consistency for objects;
- atomic single-key replacement;
- `If-Match`/`If-None-Match` conditional writes;
- durable acknowledgement and provider-side redundancy; and
- credentials and bucket policies that prevent unauthorized mutation.

AWS S3 provides the required conditional writes. An S3-compatible provider must
be verified rather than assumed. DigitalOcean Spaces does not currently expose
conditional `PutObject`, so its compatibility mode requires exactly one fenced,
externally supervised coordinator per repository. Starting two coordinators in
that mode can lose updates.

One S3 authority is highly durable against ordinary device, host, and
availability-zone failures, but no software can literally promise that data can
never be lost. Complete account compromise, destructive credentials, a provider
control-plane failure, or deletion after retention expires remain outside one
authority's boundary. Versioning, Object Lock, least-privilege credentials,
continuous verification, and tested offline or independent backups are still
required for a serious no-data-loss objective.

## Content-addressed replication experiment

`WALGIT_REQUIRE_BLOB_REPLICATION=true` plus
`WALGIT_BLOB_SECONDARY_STORE` externalizes each transaction into verified 8 MiB
chunks and writes them to two stores before the primary WAL stub is published.
This protects payload copies and supports verified fallback. It does not copy
the authoritative index, so it is not a complete provider-loss protocol.

Normal transactions stay monolithic because the measured in-memory 4 MiB stage
cost was 2.44 ms versus 3.74 ms for one-authority chunking. Chunking belongs
behind an explicit requirement until large-pack retry behavior justifies its
53% local processing cost.

## Historical certificate experiment

The codebase retains `WALGIT_REQUIRE_DUAL_AUTHORITY` and `bench-dual` so the
earlier two-provider certificate design remains reproducible. That mode is not
the normal architecture: it disables S3 CAS, requires one external writer, and
puts both providers plus certificate validation in the foreground commit path.
It should not be used when reporting normal walgit performance.

The experiment's scrub, repair, retention, and independent restore tools remain
useful research artifacts. Their protocol and limitations are documented in
[certificates.md](certificates.md), [scrub-repair.md](scrub-repair.md), and
[disaster-drills.md](disaster-drills.md).

## Performance acceptance rules

Correctness tests also guard the request shape of the hot path:

- one index GET and one conditional index PUT per unbatched commit;
- zero remote calls for finalization;
- one streamed WAL object by default, including payloads above 1 MiB;
- no content-addressed chunking unless explicitly configured; and
- independent writers must converge through CAS without losing disjoint refs.

End-to-end benchmarks must report raw Git and walgit from the same run, include
mean, p50, p90, p99, and worst case, and separate direct hooks from persistent
writer mode. A performance regression greater than 5% should not be accepted
without an explained reliability or functionality tradeoff.
