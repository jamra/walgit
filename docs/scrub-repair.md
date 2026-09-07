# Scrub and repair

> This runbook covers the historical certificate experiment, not the normal
> single-authority CAS commit path.

`walgit scrub` proves that both configured durable authorities contain the
same certificate head and that each authority can independently reconstruct
every committed payload from that head to its recovery floor. It reads and
hashes certificate JSON, transaction descriptors, every referenced 8 MiB
chunk, and each complete object. It does not trust object existence, S3 ETags,
or provider checksum metadata as a substitute for reading the bytes.

Scrubbing is read-only. Its memory use is bounded by a chunk plus certificate
metadata; payload contents are not accumulated in memory. The two authorities
are checked concurrently. Run it continuously from a scheduler and alert on a
nonzero exit status:

```sh
export WALGIT_BLOB_SECONDARY_STORE=s3://independent-b/repositories
walgit scrub -store s3://independent-a/repositories -id example
```

The command always writes a versioned JSON report to stdout. An unhealthy
report includes per-authority issues and exits nonzero. A healthy report names
the common generation and certificate SHA-256 and includes counts and verified
bytes for each authority.

The HTTP server can schedule the same scrub under the repository serving lock:

```sh
walgit serve \
  -repo /srv/git/example.git \
  -store s3://independent-a/repositories \
  -id example \
  -scrub-interval 5m
```

Each configured server runs an initial scrub before accepting traffic and then
repeats it at the requested interval. Until a scrub succeeds, and after any
later failure, writes return HTTP 503, readiness returns HTTP 503, and
checkpoint/garbage-collection maintenance stops. Reads remain available when
the backend can still reconcile them safely. A later successful scrub restores
readiness and write admission. The same scrub also rechecks configured S3
retention, making policy drift fail closed. Prometheus
metrics expose configured and healthy gauges, run and failure counters, total
duration, timestamps, and the last verified generation. Multi-repository host
configuration uses `"scrub_interval": "5m"` per repository.

## Repair protocol

Stop the repository writer before repair and keep it stopped until a final
scrub succeeds. Then select the known source explicitly:

```sh
export WALGIT_BLOB_SECONDARY_STORE=s3://independent-b/repositories
export WALGIT_REPAIR_PRIMARY_ACCESS_KEY_ID=...
export WALGIT_REPAIR_PRIMARY_SECRET_ACCESS_KEY=...
export WALGIT_REPAIR_SECONDARY_ACCESS_KEY_ID=...
export WALGIT_REPAIR_SECONDARY_SECRET_ACCESS_KEY=...
export WALGIT_MIN_RETENTION=720h
export WALGIT_RETENTION_MODE=compliance

walgit repair \
  -store s3://independent-a/repositories \
  -id example \
  -source primary
```

For S3, repair refuses to start without the dedicated primary and secondary
repair credentials. Optional session tokens use
`WALGIT_REPAIR_PRIMARY_SESSION_TOKEN` and
`WALGIT_REPAIR_SECONDARY_SESSION_TOKEN`. Region, endpoint, and path-style
settings remain the normal primary and secondary settings, so repair cannot be
redirected to a different authority through its credential variables. Repair
also requires `WALGIT_MIN_RETENTION` and protects repaired S3 versions and
audit records with that policy.

Repair follows these rules:

1. Fully verify the selected source before writing anything.
2. Refuse if both authorities are independently valid but have different
   certificate heads. A lagging or unknown-outcome tail requires an operator
   decision outside this command.
3. Copy and verify missing or corrupt chunks before publishing certificate
   links.
4. Re-scrub both authorities and require an identical head.
5. Write the same content-addressed audit record to both authorities under
   `.walgit-repair-audit/<repo>/`.

The repair output is also versioned JSON and includes the before and after
scrub reports, copied-object counts, and audit SHA-256. A repeated no-op repair
still writes a replicated audit record. This lets a retry complete the audit
stage if a prior run restored data but lost one authority while recording the
operation.

Filesystem repair performs an atomic replacement followed by file and
directory synchronization. S3 repair uses an unconditional, checksummed
`PutObject`, which can create a new current version over a corrupt version.
Normal serving processes never call this replacement path. Where the provider
can express it, restrict foreground credentials from replacing keys under the
immutable blob and certificate prefixes; independently enforced retention is
the stronger backstop. Expose repair credentials solely to the isolated repair
worker. Repair never deletes an object or an older S3 version.

## Failure boundary

The command intentionally cannot repair:

- a selected source that fails any checksum or chain validation;
- two valid but different heads;
- an extra corrupt or unlinked certificate that is not part of the selected
  source chain, because removing it would be destructive;
- a filesystem marked `.walgit-poisoned` until the operator replaces or
  recovers that storage and explicitly clears the fail-stop condition.

A repair interrupted midway is safe to retry: payloads are written before
certificates, every write is content-verified, and no data is deleted. Writers
must remain stopped because the prototype does not yet provide a distributed
maintenance lease. The in-process scheduled scrub is serialized with that
server's Git requests, but it cannot fence a separate writer process or another
host. Enforced [retention/Object Lock](retention.md) and recurring isolated
restore plus `git fsck --strict` remain separate required controls.
