# Content-addressed blob store

Walgit's blob path makes large Git object payloads independently retryable,
deduplicated, verifiable, and replicable. It is used automatically when the
non-transient files in a staged object directory total at least 1 MiB. Smaller
transactions keep the lower-overhead inline WAL representation.

## Format and publication

Each regular Git object file is read once and split into ordered 8 MiB chunks.
The descriptor records:

- format version;
- whole-file SHA-256 and byte length;
- relative object path and mode;
- ordered chunk SHA-256 values and byte lengths.

Chunks use this immutable key layout:

```text
<store-prefix>/.walgit-blobs/sha256/<first-two-hex>/<64-hex-digest>
```

The complete transaction metadata, including every file descriptor, is encoded
as JSON and stored as another content-addressed blob. The WAL archive contains
only the transaction identity, reference updates, and the descriptor's hash and
length. This keeps the authoritative WAL object small without making the
manifest grow with every chunk.

Publication order is:

1. Read, hash, and upload every data chunk.
2. Wait for every configured blob authority to verify every chunk.
3. Store and verify the external transaction descriptor in every authority.
4. Publish the immutable WAL stub to the primary store.
5. In dual-authority mode, publish the hash-chained commit certificate to both
   authorities.
6. Publish the primary manifest update using its single-writer rule.
7. Acknowledge the Git reference transaction only after those boundaries.

A failed or crashed attempt can leave unreferenced chunks, but cannot publish a
manifest that references a chunk which the configured blob boundary did not
acknowledge. Content keys make retry idempotent. Existing content is downloaded
and hashed before an immutable-write conflict is accepted as success.

## Resource bounds and performance

Four chunk uploads may run concurrently. A staging operation therefore retains
at most four 8 MiB chunk buffers, or 32 MiB, independent of pack size. The same
immutable buffer is shared by concurrent authority uploads and is not returned
to the pool until all of them finish.

This is not kernel zero-copy: SHA-256 requires the application to inspect every
byte, and the AWS request body consumes a userspace buffer. It does remove the
old temporary archive, local durability sync, and complete archive rereads. A
future Linux service can evaluate `io_uring`, registered buffers, larger
provider-native checksums, and HTTP/2/3 connection behavior, but each change
must preserve end-to-end content verification.

## Secondary authority

Set both variables in deployments where large writes must fail closed unless
two blob stores accept them:

```sh
export WALGIT_BLOB_SECONDARY_STORE=s3://second-provider/walgit
export WALGIT_REQUIRE_BLOB_REPLICATION=true
```

`WALGIT_BLOB_SECONDARY_STORE` may also be an absolute filesystem path or a
`file://` URI. For a second S3-compatible provider, these optional values
override the primary AWS SDK configuration:

| Variable | Purpose |
| --- | --- |
| `WALGIT_BLOB_SECONDARY_REGION` | Secondary region |
| `WALGIT_BLOB_SECONDARY_ENDPOINT` | S3-compatible endpoint |
| `WALGIT_BLOB_SECONDARY_PATH_STYLE` | Enable path-style bucket URLs |
| `WALGIT_BLOB_SECONDARY_ACCESS_KEY_ID` | Secondary access key |
| `WALGIT_BLOB_SECONDARY_SECRET_ACCESS_KEY` | Secondary secret key |
| `WALGIT_BLOB_SECONDARY_SESSION_TOKEN` | Optional session token |

The primary and secondary locations must differ. In production they should
also differ in provider/account administration, region, credentials, and
failure mode. Two buckets controlled by one deletion-capable credential are
two copies, not two independent durability authorities.

Reads try each authority in order and accept only a copy whose length and
SHA-256 match the descriptor. Automatic fallback is implemented. Automatic
repair is not: immutable foreground credentials should not be able to replace
a corrupt object, and choosing a repair source belongs in an audited,
privileged scrubber.

Set `WALGIT_REQUIRE_DUAL_AUTHORITY=true` instead of the blob-only requirement
when commit ordering must also survive primary loss. This forces small and
metadata-only transactions into the blob path and certifies their descriptors
on both stores. S3 deployments must also use the repository-scoped single
writer. The full format and recovery rules are in
[the certificate protocol](certificates.md).

## Recovery and compatibility

Replay resolves and verifies the external descriptor, downloads its chunks,
checks every chunk, checks the complete reconstructed file, syncs a temporary
file, atomically renames it, and syncs the affected Git object directories.
Only then may the local generation marker advance. Checkpoints use the same
blob representation. Older archives with inline object payloads remain
readable.

Manifest `bytes` records the small WAL/checkpoint archive size and
`payload_bytes` records the external object size. Maintenance thresholds count
both, so externalization does not accidentally disable compaction.

## Security and garbage collection

Deduplication is scoped to the configured store prefix. Give each tenant or
security domain a separate prefix; sharing content-addressed existence across
untrusted tenants can create a content-presence side channel.

The prototype does not delete content-addressed blobs. Safe reclamation needs a
dual-authority mark phase over retained checkpoints, the WAL tail, prepared
transactions, and retention windows, followed by an independently verified
sweep. Until then, leaking an orphan created by a failed upload is the safe
choice.

## Remaining operational boundary

The certificate protocol preserves independently replayable ordering and
payloads across primary loss for repositories initialized in dual-authority
mode. `walgit scrub` supports continuous verification of both copies, and
`walgit repair` provides a conservative, separately credentialed repair path;
see the [runbook](scrub-repair.md). Independently enforced retention and
recurring disaster-restore drills remain required. S3 retention enforcement is
described in the [retention runbook](retention.md). See the
[reliability model](reliability.md) for the exact boundary.
