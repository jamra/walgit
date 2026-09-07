# Disaster restore drills

> These commands exercise the historical dual-authority certificate experiment,
> not the normal single-index CAS architecture. They remain useful research and
> operational test tools, but their latency is not normal walgit latency.

`walgit drill` proves that one durable authority can reconstruct a repository
without reading its peer. This is stronger than checking that two manifests or
object listings match: the command verifies the selected authority's complete
immutable certificate chain, hashes every reachable descriptor and chunk,
replays every reference transaction into an empty bare repository, and runs
`git fsck --strict`.

```sh
export WALGIT_BLOB_SECONDARY_STORE=s3://independent-b/repositories

walgit drill \
  -store s3://independent-a/repositories \
  -id origin \
  -source primary

walgit drill \
  -store s3://independent-a/repositories \
  -id origin \
  -source secondary
```

The secondary drill uses the secondary endpoint, region, and credentials. It
does not open the primary. The primary drill likewise does not require the
secondary to be available. This makes either command usable during a real
provider outage, not only while both providers are healthy.

The JSON report includes the verified generation and certificate digest,
object counts and bytes, inspection time, replay time, fsck time, and total
time. By default the reconstructed local cache is deleted after verification.
Use `-keep` to retain it and receive its path in `restored_repository`; this
never keeps or modifies remote benchmark data.

Run the drill while the repository is quiescent, or retry if its certified head
advances between inspection and replay. The command rejects that race rather
than reporting a restore against a mixture of generations.

## Failure behavior

The drill fails closed when:

- any certificate, descriptor, or content chunk is absent or corrupt;
- the authority contains a fork, broken chain, or ambiguous head;
- a descriptor changes between verification and replay;
- reconstructed references differ from the certified reference set;
- Git's strict object check fails; or
- the certificate chain begins at a nonzero legacy floor.

A legacy floor means the repository enabled certificates after existing
history had already been committed. Its certificates alone cannot recreate an
empty repository; preserve and independently replicate its anchoring
checkpoint before claiming provider-loss recovery.

Run both source drills after initial migration, after storage or credential
changes, and on a recurring schedule. Retain the JSON results outside both
storage authorities as operational evidence. A successful drill proves that
the selected copy was complete at that instant; continuous scrub and immutable
retention are still required to bound failures between drills.

## Dual-authority latency benchmark

`bench-dual` measures the incremental cost of each durability layer using four
otherwise equivalent push targets:

1. raw Git on the benchmark host's local filesystem;
2. one S3 authority;
3. content blobs replicated to two authorities without commit certificates;
4. full dual-authority blobs and commit certificates.

It reports mean, p50, p90, p99, and maximum push latency for every target. The
replicated-blob minus single-authority mean isolates replication cost; the full
dual-authority minus replicated-blob mean isolates certificate-commit cost.
Retention is a storage policy rather than an extra request, so its status is
reported as `durability_mode` and in the final scrub report instead of being
misrepresented as a separately measurable latency.

Protected benchmark objects cannot be deleted until their retention expires,
so a protected run requires `-keep`:

```sh
export WALGIT_MIN_RETENTION=720h
export WALGIT_RETENTION_MODE=compliance

walgit bench-dual \
  -primary s3://independent-a/benchmarks \
  -secondary s3://independent-b/benchmarks \
  -pushes 100 \
  -blob-bytes 65536 \
  -keep
```

For disposable latency testing on buckets without Object Lock, opt out
explicitly:

```sh
walgit bench-dual \
  -primary s3://independent-a/benchmarks \
  -secondary s3://independent-b/benchmarks \
  -pushes 100 \
  -blob-bytes 65536 \
  -allow-unprotected
```

The unprotected result is prominently labeled
`UNPROTECTED_BENCHMARK_ONLY` and cannot support a durability claim. Every run
adds the same cryptographically random `walgit-benchmark-*` child to both base
URIs. Cleanup refuses any location without such a child and removes all current
repository, blob, descriptor, certificate, and audit objects only beneath that
exact generated prefix. Existing bucket data and adjacent prefixes are not in
scope. On versioned buckets, cleanup also lists and deletes versions and delete
markers, so the benchmark identity needs those narrowly scoped permissions.

Different URIs do not by themselves prove independent failure domains. For a
meaningful reliability result, use different provider accounts (preferably
different providers), credentials, endpoints, regions, and administrative
control planes.
