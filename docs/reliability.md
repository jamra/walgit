# Reliability model

This document distinguishes properties walgit enforces in code from
properties that require deployment policy. The governing rule is:

> Walgit must never acknowledge a Git update unless every object needed to
> reconstruct it has crossed the configured durable commit boundary.

## Implemented boundaries

### Filesystem backend

A durable file is published in this order:

1. Write a temporary file and check every write.
2. Sync the file and check the result.
3. Close the file and check the result.
4. Atomically rename it into place.
5. Sync the containing directory.
6. Only then publish or acknowledge state that references it.

Checkpoint files use the same protocol. In particular, the `checkpoints`
directory is synced before the repository manifest may reference a checkpoint.

An error at a write, close, file-sync, directory-sync, or persistence-class
rename boundary poisons the store. The running process refuses further reads and writes, and a
best-effort `.walgit-poisoned` marker makes later hook processes fail closed.
A later successful `fsync` is not treated as proof that data from the failed
writeback survived. This is the failure mode commonly called
[fsyncgate](https://wiki.postgresql.org/wiki/Fsync_Errors).

Recovery requires replacing or repairing the faulty storage, reconstructing
the store from an independent durable authority, validating all hashes and Git
objects, removing the poison marker, and restarting the process. Never automate
marker removal without those checks.

### S3-compatible backend

Small object sets remain on the original no-spool path:

```text
Git quarantine files
        │
        ▼
256 KiB reusable buffer ──► tar encoder ──► SHA-256 ──► S3 WAL object
```

At 1 MiB and above, the content-addressed path avoids turning the entire pack
into one retry and replication unit:

```text
Git quarantine file
        │
        ├── 8 MiB chunk ── SHA-256 ──┬──► authority A
        │                            └──► authority B (when configured)
        │
        ├── whole-file SHA-256
        ▼
ordered descriptor ── SHA-256 ──────► both authorities
        │
        ▼
tiny WAL stub ──────────────────────► primary authority
```

The uploader has at most four 8 MiB buffers in flight. Chunk keys are derived
only from a validated lowercase SHA-256 digest. Creation is immutable. A retry
that encounters an existing chunk reads and validates it before treating the
write as successful, covering both deduplication and a lost success response.
Replay validates each chunk's size and digest, then validates the reconstructed
file's size and whole-file digest before atomically publishing it to the cache.

When `WALGIT_BLOB_SECONDARY_STORE` is configured, foreground writes fan out to
both blob authorities concurrently and succeed only if both accept or already
contain the verified chunk. Reads verify the primary and fall back to the
secondary. Walgit does not let foreground writer credentials overwrite a
corrupt immutable object; privileged repair is intentionally a separate future
responsibility.

The WAL request also asks the provider to validate SHA-256. Walgit compares a
returned checksum when one is supplied and records its independently calculated
digest in the manifest. A timeout is ambiguous: the provider might have durably
stored the object before the response was lost. Because transaction objects are
immutable, walgit resolves this by downloading, parsing, and hashing the object.
It publishes the manifest only after that validation succeeds.

The same validation allows a restarted coordinator, which has lost its memory
cache, to safely recover a staged transaction. Replay always recalculates and
checks the manifest digest before applying reference updates. Inline archives
created before the blob format remain readable.

### Dual-authority certificates

With `WALGIT_REQUIRE_DUAL_AUTHORITY=true`, the durable commit path is:

```text
all chunks + transaction descriptor
        │
        ├──────────────► authority A (verified)
        └──────────────► authority B (verified)
                              │
previous certificate hash + entries + resulting refs
        │
        ├──────────────► authority A (immutable certificate)
        └──────────────► authority B (immutable certificate)
                              │
                              ▼
                     primary manifest cache
                              │
                              ▼
                       acknowledge Git
```

Every transaction is externalized in this mode, including small pushes and
metadata-only rollback records. A certificate is a SHA-256-addressed immutable
link containing the prior certificate hash, consecutive generation entries,
their descriptor hashes and ref updates, and the resulting complete ref map.
The immediate parent is re-read and verified on each authority before a child
is stored. Both child writes must verify before primary-manifest publication.

The primary manifest can be reconstructed from the chain. Replay uses certified
descriptors when primary WAL objects are unavailable. When both authorities are
readable, only their highest common valid chain is accepted; when one has been
lost, the survivor's fully validated chain is recoverable. S3 uses one
repository-scoped writer because two object stores cannot perform one atomic
cross-provider CAS. Full rules and the unavoidable unknown-outcome-tail case are
documented in [the certificate protocol](certificates.md).

### Serving caches

Git repositories on serving nodes are reconstructible caches, not authority.
Walgit nevertheless prevents a durable generation marker from claiming objects
whose directory entries could disappear in the same crash:

- extracted files are synced before rename;
- affected object directories are synced from leaf to root;
- Git uses `core.fsync=committed` and `core.fsyncMethod=fsync`;
- the generation marker is written, synced, renamed, and followed by a parent
  directory sync.

If cache verification fails, discard and reconstruct the cache from checkpoint
plus WAL rather than attempting an in-place repair.

## Failure behavior

| Failure | Result |
| --- | --- |
| Process exits before S3 object success | Push fails; no manifest reference |
| S3 stores object but response is lost | Immutable object is read and verified before continuing |
| Process exits after object but before manifest | Orphan remains safe; retry recovers it |
| One blob authority rejects or loses a write | Push fails before WAL publication |
| Primary blob copy is missing or corrupt | Verified secondary copy is used |
| Both blob copies are missing or corrupt | Replay fails closed; refs do not advance |
| One certificate write fails | Push fails; highest common chain does not advance |
| Both certificates succeed but manifest publication is lost | Retry recovers the certified commit; client had an unknown outcome |
| Primary manifest, WAL, and blobs are lost | Survivor certificate chain and descriptors reconstruct the repository |
| Certificate chains disagree while both are readable | Only their highest common valid chain is selected |
| A certificate chain is corrupt on both sides | Recovery fails closed |
| Checksum or archive validation fails | No manifest publication or acknowledgement |
| Manifest CAS loses a race | Reload winner and retry against its ETag |
| Filesystem sync reports an error | Entire store is poisoned; no retry in that process |
| Crash after checkpoint rename | Synced checkpoint is either unreferenced or safely referenced |
| Crash while reconstructing a cache | Generation does not advance; replay is idempotent |

## Strict no-loss deployment

The code now enforces the foreground half of the stronger policy for
repositories initialized with dual-authority mode:

1. Store every transaction's chunks and descriptor in two authorities.
2. Validate immutable existing objects and fall back to a verified secondary.
3. Store a hash-chained commit certificate in both authorities before success.
4. Reject one-sided writes and select only the common chain while both sides are
   readable.
5. Reconstruct refs, ordering, and objects from one surviving authority after
   complete primary manifest/WAL/blob loss.
6. Preserve abort semantics with a certified compensation entry.

This means loss of one complete authority does not lose an acknowledged commit,
provided the other still retains its acknowledged objects. Two copies alone do
not make that statement timeless. The remaining operational protocol is:

1. Enable versioning and retention/Object Lock independently on both sides,
   using credentials that cannot shorten retention or delete retained data.
2. Continuously run `walgit scrub` against both copies and alert on any nonzero
   result.
3. Stop the repository writer and use `walgit repair` with separately
   privileged credentials to copy and verify missing chunks, descriptors, and
   certificate links before another failure can overlap. The exact procedure
   is in the [scrub and repair runbook](scrub-repair.md).
4. Exercise restoration in a separate account and run `git fsck --strict`.
5. Add a certified checkpoint migration before claiming full-history protection
   for a repository anchored above generation zero.

If one authority disappears, a survivor-only tail could be an unacknowledged
write whose peer failed just before success. Recovering it avoids acknowledged
data loss but can expose an extra unknown-outcome commit. Avoiding both outcomes
requires a third witness or consensus quorum; see the protocol document.

Ordinary same-provider replication, RAID, snapshots controlled by the same
credentials, or a successful local `fsync` are useful layers, but they are not
substitutes for two independent durable authorities.

## Required destructive-operation policy

- Garbage collection must retain data until both authorities confirm that a
  newer checkpoint and its complete WAL tail are durable.
- The current prototype never garbage-collects content-addressed blobs. Leaked
  orphan chunks are preferable to deleting a live chunk before dual-authority
  reachability is implemented.
- Deletion credentials must be separate from foreground writer credentials.
- Retention must exceed the longest credible detection and recovery window.
- A scrub or repair mismatch must stop garbage collection and alert an
  operator; it must never choose a winner silently.
