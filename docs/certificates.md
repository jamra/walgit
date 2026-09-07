# Dual-authority commit certificates

Commit certificates make repository ordering reconstructible without the
primary manifest or primary WAL namespace. The protocol is opt-in because it
changes the write boundary and requires one serialized writer per repository.

## Enabling the protocol

For S3-compatible authorities:

```sh
export WALGIT_BLOB_SECONDARY_STORE=s3://independent-account/walgit
export WALGIT_REQUIRE_DUAL_AUTHORITY=true
export WALGIT_S3_DISABLE_CONDITIONAL_WRITES=true
export WALGIT_WRITER_SOCKET=/run/walgit/origin.sock
```

The secondary credential and endpoint overrides are documented in
[the blob-store design](blob-store.md). A filesystem primary and secondary use
the same protocol and serialize publication with the manifest lock.

`WALGIT_REQUIRE_DUAL_AUTHORITY` implies replicated blobs. It fails startup when
the secondary is absent or aliases the primary. S3 startup also fails unless
single-writer mode is enabled. The writer socket must be supervised and exposed
only to the repository's Git gateway and hooks.

## Certificate format

Every certificate is canonical JSON addressed by its SHA-256 digest and stored
under:

```text
<store-prefix>/.walgit-certificates/<repo-id>/<20-digit-generation>-<sha256>.cert
```

A root certificate records generation zero, symbolic `HEAD`, object format,
and the empty ref map. Each later certificate records:

- repository identity and format version;
- final generation for the atomic writer batch;
- SHA-256 of the previous certificate;
- every new manifest entry and its exact ref updates;
- each transaction's replicated descriptor hash;
- the complete resulting ref map.

Including both updates and resulting refs lets recovery verify every expected
old OID, apply each generation in order, and compare the calculated result with
the certificate. The descriptor hash lets replay bypass a lost primary WAL
stub. One certificate may cover several consecutive entries produced by one
group commit, making the batch one atomic chain link.

Certificate creation is deterministic for a given parent and batch. Retrying an
ambiguous write therefore addresses the same immutable object.

## Commit sequence

For each transaction or coordinated batch:

1. Store every object chunk in both blob authorities.
2. Store the complete transaction descriptor in both authorities.
3. Publish the small immutable WAL stub in the primary store.
4. Calculate the next refs and certificate from the current certified parent.
5. Re-read and verify the immediate parent certificate on each authority.
6. Write and checksum the identical new certificate on both authorities.
7. Publish the mutable primary manifest.
8. Return success to Git.

A failure at steps 1, 2, 5, or 6 prevents acknowledgement. A failure after one
certificate copy can leave a one-sided orphan. While both authorities are
readable, recovery chooses only their highest common chain, so that orphan is
not committed. Retrying completes the identical certificate.

If both certificate writes succeeded but the primary manifest write or response
failed, the client sees an unknown outcome. The certificate is authoritative;
the next load recovers it and a retry is idempotent. This preserves acknowledged
history and follows normal distributed-storage unknown-outcome semantics.

Git can still report `aborted` after the certificate commit point. Walgit then
writes a metadata-only inverse WAL entry and a new dual-authority compensation
certificate. This also works when the mutable manifest disappeared between the
original commit and the abort notification.

## Recovery

Each authority is read and validated independently:

1. Verify filename generation, content length, and SHA-256.
2. Parse the root and follow every `previous_sha256` link.
3. Reject cycles, missing links, discontinuous generations, or forks at the
   selected generation.
4. Reapply certified ref updates and compare the resulting refs.
5. Require every certified entry to contain a valid descriptor reference.

When both authorities are readable, walgit selects their highest common valid
certificate. If one authority is missing, empty, corrupt, or unavailable, it
can recover the highest valid chain from the survivor. The recovered manifest
contains all certified entries, so a fresh cache downloads verified descriptors
and chunks from the surviving blob authority without opening primary WAL files.

There is an unavoidable ambiguity after complete loss of one authority: a
certificate visible only on the survivor might have been written there just
before the other write failed, so recovery can include an unacknowledged tail.
It will not omit an acknowledged commit. Eliminating both lost updates and an
extra unknown-outcome tail requires a third voting witness or consensus system;
the two-copy protocol prioritizes the stated no-data-loss requirement.

Recovered state is read-only while the primary manifest is unavailable. An S3
coordinator does not cache it as writable state, and new stages still require
both blob authorities. Repair the primary certificate chain and manifest before
resuming writes.

## Existing repositories

Enabling certificates on an existing non-empty repository writes a legacy root
anchor at its current generation. Future commits are protected, but earlier WAL
payloads are not magically present in the secondary. The manifest records that
generation as the recovery floor; replay from an older or empty cache fails
instead of advancing a generation marker over missing objects.

For the strongest guarantee, initialize a new repository with dual-authority
mode already enabled. A production migration tool should create and certify a
complete checkpoint on both authorities before lowering the recovery floor;
that migration command is not implemented yet.

## Remaining requirements

The foreground protocol prevents acknowledgement before two verified copies,
but durability is also a continuing operational property:

- enable independently administered retention or Object Lock;
- deny overwrite and delete privileges to foreground writers;
- continuously scrub every chain, descriptor, and chunk;
- repair a damaged authority with separate audited credentials;
- stop writes and garbage collection when repair cannot prove a source;
- restore regularly into an isolated account and run `git fsck --strict`.

Certificate and content-addressed blob garbage collection remain disabled until
dual-authority reachability, retention, and repair are implemented.
