# Fast S3 on AWS

Walgit's normal protocol can use either a general-purpose S3 bucket or an S3
Express One Zone directory bucket without changing its ordering model. Both use
one immutable WAL object followed by one conditional WAL-index update.

## What to benchmark

Run the same workload from EC2, not a developer laptop:

| Target | Placement | Purpose |
| --- | --- | --- |
| S3 Standard | EC2 in the same AWS Region | Multi-AZ durability baseline |
| S3 Express One Zone | EC2 in the directory bucket's AZ | Lowest-latency AWS path |

AWS documents S3 Express as providing consistent single-digit-millisecond
access, up to 10x faster than S3 Standard. The supported Go v2 SDK recognizes a
directory bucket's `--zone-id--x-s3` name, routes requests to its zonal endpoint,
and automatically creates and refreshes low-latency session credentials.

Directory buckets support the `PutObject` `If-Match` and `If-None-Match`
preconditions used by walgit. They require virtual-hosted-style requests and do
not support versioning or Object Lock. Walgit detects their names, rejects
path-style configuration, and uses directory-bucket-compatible benchmark
cleanup.

## Safe benchmark setup

Use AWS CLI v2 and authenticate first. The machine currently used to develop
walgit has no configured AWS credentials, so no AWS resources or existing
buckets have been touched.

For the multi-AZ baseline, create or select a general-purpose bucket in the EC2
runner's Region. Pass only a dedicated benchmark base prefix:

```sh
export AWS_REGION=us-west-2
./bin/walgit bench \
  -nodes 2 \
  -pushes 100 \
  -blob-bytes 65536 \
  -store s3://YOUR-STANDARD-BUCKET/walgit-benchmarks
```

For S3 Express, choose a supported AZ ID, create a uniquely named directory
bucket, and launch the benchmark runner in that physical AZ:

```sh
export AWS_REGION=us-west-2
export WALGIT_EXPRESS_ZONE_ID=usw2-az1
export WALGIT_EXPRESS_BUCKET=YOUR-UNIQUE-NAME--usw2-az1--x-s3

aws s3api create-bucket \
  --region "$AWS_REGION" \
  --bucket "$WALGIT_EXPRESS_BUCKET" \
  --create-bucket-configuration \
  "Location={Type=AvailabilityZone,Name=$WALGIT_EXPRESS_ZONE_ID},Bucket={DataRedundancy=SingleAvailabilityZone,Type=Directory}"

./bin/walgit bench \
  -nodes 2 \
  -pushes 100 \
  -blob-bytes 65536 \
  -store "s3://$WALGIT_EXPRESS_BUCKET/walgit-benchmarks"
```

The benchmark creates a cryptographically random `walgit-benchmark-*` child
prefix. Cleanup refuses paths without that marker. The JSON result identifies
the target as `express-one-zone` or `standard-or-general-purpose`.

The runner needs object access to the benchmark prefix. For Express, its IAM
identity also needs `s3express:CreateSession` on the directory bucket; the SDK
handles the resulting session credentials. Bucket creation and deletion should
use a separate setup identity rather than the benchmark process.

## Reliability decision

S3 Standard is the recommended authority today. It stores data across at least
three Availability Zones and supports versioning and Object Lock.

S3 Express is designed for 11-nines object durability across multiple devices,
but all copies are inside one AZ. AWS explicitly warns that physical loss of the
AZ may lose One Zone data. That violates walgit's stronger provider/AZ-loss
objective even though ordinary device-loss durability is excellent.

Therefore:

```text
current production recommendation

Git servers ──► S3 Standard WAL + CAS index (authoritative, multi-AZ)
       └──────► local NVMe Git caches (disposable)

current performance experiment

EC2 in AZ ───► S3 Express WAL + CAS index (fast, single-AZ)
       └──────► local NVMe Git caches (disposable)
```

Do not put S3 Standard synchronously after every Express index CAS merely to
claim a backup; that adds Standard's latency back to every push. The next
reliability experiment should instead use a second low-latency AZ authority as
a non-ordering mirror:

1. write the immutable WAL to both AZs concurrently;
2. order the batch with one primary Express index CAS;
3. write the winning immutable commit/index snapshot to the second AZ;
4. acknowledge only after the snapshot is verified; and
5. stop writes on primary loss until an operator performs a fenced promotion.

This preserves one ordering authority and avoids consensus in the hot path.
Group commit amortizes the secondary snapshot write across many pushes. It also
has the normal distributed-system unknown-outcome rule: a crash after the
secondary snapshot but before client acknowledgement may recover an extra
commit, but an acknowledged commit must not be absent from either AZ.

That mirror is not implemented yet. Until it is, S3 Express results are
performance data, not proof of AZ-loss durability.

## Sources

- [Optimizing S3 Express One Zone performance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-performance.html)
- [S3 Express session authentication](https://docs.aws.amazon.com/sdkref/latest/guide/feature-s3-express.html)
- [PutObject API and conditional headers](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)
- [S3 storage-class durability comparison](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html)
- [Creating a directory bucket](https://docs.aws.amazon.com/cli/latest/reference/s3api/create-bucket.html)
