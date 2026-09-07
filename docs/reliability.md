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

The normal transaction path has no local durable spool:

```text
Git quarantine files
        │
        ▼
256 KiB reusable buffer ──► tar encoder ──► SHA-256
                                  │
                                  └────────► bounded upload pipe ──► S3
```

The request asks the provider to validate SHA-256. Walgit compares the returned
checksum when one is supplied and records its independently calculated digest
in the manifest. A timeout is ambiguous: the provider might have durably stored
the object before the response was lost. Because transaction objects are
immutable, walgit resolves this by downloading, parsing, and hashing the object.
It publishes the manifest only after that validation succeeds.

The same validation allows a restarted coordinator, which has lost its memory
cache, to safely recover a staged transaction. Replay always recalculates and
checks the manifest digest before applying reference updates.

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
| Checksum or archive validation fails | No manifest publication or acknowledgement |
| Manifest CAS loses a race | Reload winner and retry against its ETag |
| Filesystem sync reports an error | Entire store is poisoned; no retry in that process |
| Crash after checkpoint rename | Synced checkpoint is either unreferenced or safely referenced |
| Crash while reconstructing a cache | Generation does not advance; replay is idempotent |

## Strict no-loss deployment

The current code protects against process crashes, torn publication, detected
filesystem writeback failures, corrupt transfers, and loss of a serving cache.
A single S3 bucket is still one administrative failure domain. It cannot, by
itself, satisfy a requirement that acknowledged history survive complete loss
or hostile deletion of that provider account or region.

That stronger policy requires a dual-authority protocol:

1. Store each content-addressed transaction in two independent providers,
   accounts, credentials, and regions.
2. Validate the checksum returned by each provider.
3. Publish an immutable, hash-chained commit certificate containing the
   generation, previous certificate hash, transaction hash, and resulting refs.
4. Store and validate that certificate in both authorities.
5. Acknowledge Git only after both authorities contain the transaction and
   certificate.
6. Treat a one-sided write as unacknowledged but repairable. A repair worker
   copies and verifies the missing side without changing the transaction or
   generation.
7. Enable versioning and retention/Object Lock independently on both sides,
   using credentials that cannot shorten retention or delete retained data.
8. Continuously scrub both copies against the certificate chain and exercise
   restoration in a separate account.

Ordinary same-provider replication, RAID, snapshots controlled by the same
credentials, or a successful local `fsync` are useful layers, but they are not
substitutes for two independent durable authorities.

## Required destructive-operation policy

- Garbage collection must retain data until both authorities confirm that a
  newer checkpoint and its complete WAL tail are durable.
- Deletion credentials must be separate from foreground writer credentials.
- Retention must exceed the longest credible detection and recovery window.
- A scrub or repair mismatch must stop garbage collection and alert an
  operator; it must never choose a winner silently.
