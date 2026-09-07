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
| Checksum or archive validation fails | No manifest publication or acknowledgement |
| Manifest CAS loses a race | Reload winner and retry against its ETag |
| Filesystem sync reports an error | Entire store is poisoned; no retry in that process |
| Crash after checkpoint rename | Synced checkpoint is either unreferenced or safely referenced |
| Crash while reconstructing a cache | Generation does not advance; replay is idempotent |

## Strict no-loss deployment

The current code protects against process crashes, torn publication, detected
filesystem writeback failures, corrupt transfers, and loss of a serving cache.
It can also require the large-object data plane to reach two blob authorities.
A single primary S3 bucket still owns the manifest and WAL stub, however, so
the implementation cannot yet satisfy a requirement that acknowledged history
survive complete loss or hostile deletion of that provider account or region.

The completed part of the stronger policy is:

1. Split large files into content-addressed immutable chunks.
2. Store every chunk and its external transaction descriptor in two configured
   authorities concurrently.
3. Validate existing objects and fall back to a verified secondary on reads.
4. Refuse publication after any one-sided foreground write.

The remaining dual-authority commit protocol is:

1. Publish an immutable, hash-chained commit certificate containing the
   generation, previous certificate hash, transaction descriptor hash, and
   resulting refs.
2. Store and validate that certificate in both authorities.
3. Acknowledge Git only after both authorities contain the data and certificate.
4. Elect/recover the current head from the certificate chain without depending
   on the lost primary manifest.
5. Treat a one-sided write as unacknowledged but repairable. A privileged repair
   worker copies and verifies the missing side without changing content or
   generation.
6. Enable versioning and retention/Object Lock independently on both sides,
   using credentials that cannot shorten retention or delete retained data.
7. Continuously scrub both copies against the certificate chain and exercise
   restoration in a separate account.

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
