# ECS RAM role credentials for the OSS fixture

These adapters connect the official Alibaba Cloud credential provider to
Iceberg's AWS SDK v2 provider extension and Hadoop 3.3 S3A's AWS SDK v1
extension. Each adapter obtains one complete session value per signing request.
The official provider owns caching and synchronous renewal. IMDSv1 fallback and
background refresh are disabled. Temporary keys and tokens are never written
into the fixture publication or passed on a process command line.

Select the providers explicitly:

```properties
client.credentials-provider=org.novarocks.fixture.auth.AliyunEcsCredentialsProvider
client.credentials-provider.role-name=YOUR_ECS_ROLE
```

For Hadoop S3A, select
`org.novarocks.fixture.auth.AliyunEcsV1CredentialsProvider` with
`fs.s3a.aws.credentials.provider` and set the non-secret deployment variable
`NOVA_OSS_ROLE_NAME=YOUR_ECS_ROLE`. Neither adapter tries the AWS or Alibaba
default credential chain. Remove static access-key/session-token settings:
older Iceberg versions can choose those before a custom provider.

The v2 and v1 classes are separate so the REST fixture can load its provider
without an AWS SDK v1 dependency. Both expose their SDK's close interface and
close the owned source. The wrapper launcher reads the upstream REST adapter's
manifest main class and starts it with a real classpath containing the bridge.
It preserves the upstream application and working directory; `java -jar` would
ignore an independently supplied `CLASSPATH`.

## Offline build and functional checks

The fixture input lock supplies the 18 runtime jars selected by
`runtime-jars.txt`, plus the already locked Iceberg AWS and AWS SDK v1 bundles
used for compilation. The input lock owns URLs and content hashes. Building or
running this directory does not invoke Maven or download dependencies.

```bash
bash docker/iceberg-rest/aliyun-credentials/build.sh \
  "$VERIFIED_ARTIFACT_DIRECTORY" "$NEW_OUTPUT_DIRECTORY" --test
python3 -m unittest discover \
  -s docker/iceberg-rest/aliyun-credentials/tests -p 'test_*.py' -v
```

The Java checks exercise atomic session rotation through the real official
provider cache, source close, and v2-only class loading. The Python checks
exercise session adaptation, exact scope, ordinary object operations,
pagination, response/resource close and safe errors using an isolated fake
transport. They neither contact metadata/OSS nor create persistent resources.
Run them when the adapters, package inputs or operation contract change. The
fixture owner maintains these correctness checks; they do not establish live
OSS, REST/Spark or native 1FE+3BE acceptance.

The Docker wrapper accepts `BUILDER`, `SERVICE_BASE`, `SERVICE_KIND` (`rest` or
`spark`) and `SERVICE_USER`. Its base is an exact already provisioned service
image. Spark receives the jars under `/opt/spark/jars`; REST uses the new
launcher classpath. The existing local MinIO service images are unchanged.

## Scoped Python object operations

`../oss_ops.py` runs with the provisioned, locked Python environment. It uses
the native OSS SDK with a synchronous official ECS role provider. Its config
contains only `kind`, `region`, `bucket`, `role_name`, and `root_prefix`:

```json
{
  "kind": "aliyun-oss",
  "region": "cn-beijing",
  "bucket": "YOUR_BUCKET",
  "role_name": "YOUR_ECS_ROLE",
  "root_prefix": "novarocks-development"
}
```

The CLI syntax is:

```bash
"$OSS_PYTHON" docker/iceberg-rest/oss_ops.py --config "$CONFIG" \
  put --uri s3://YOUR_BUCKET/novarocks-development/run/file --input "$FILE"
"$OSS_PYTHON" docker/iceberg-rest/oss_ops.py --config "$CONFIG" \
  get --uri s3://YOUR_BUCKET/novarocks-development/run/file --output "$DESTINATION"
```

The remaining commands are `stat`, `list`, `delete`, `list-multipart`, and
`abort-multipart` (which requires `--upload-id`). Global `--timeout-seconds`
can carry the caller's remaining request budget. Every completed command emits
one JSON object. `get` writes bytes to the named local output file, replacing
it only after the read completes. Errors expose only a code and HTTP status,
not exception bodies, credentials or server responses.

`list` returns `objects` with `uri`, `size`, `etag`, and `last_modified`;
`list-multipart` returns `uploads` with `uri`, `upload_id`, and `initiated`.
Listings append a directory boundary to the requested prefix and read all
pages. `delete` removes exactly one named object; the CLI has no recursive
prefix delete, IAM operation, conditional publication or rebuild command.

Bucket and prefix checks constrain these operations but do not grant cleanup
authority. The calling fixture owner must first establish the exact run scope,
actual writer exit and any required catalog barrier. The role's cloud IAM
permissions remain an external deployment input. No token returned by this
provider is represented as catalog-vended or remotely downscoped.
