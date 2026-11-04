# Iceberg read policy inputs

The `iceberg` cases `novarocks_full_metadata_depth64` and
`novarocks_history_future_partition_source`, and the `iceberg-ddl` case
`full_metadata_depth64_hadoop`, protect file/table identity, deep metadata and
the separation between historical reads and current write authority. They
are maintained by the Iceberg suite owner. The two depth64 cases are
temporarily outside the finite support scope by the user decision on
2026-11-03; the historical partition case remains ordinary suite coverage.
The exact depth cases are registered in `tools/ci/baselines/dispositions.json`
with an owner, review date and exit condition. Quarantine excludes admission
judgment only; it does not make raw full-suite results pass. Local B validation
uses the existing `--skip` option below, retaining the original SQL, goldens,
SDK inputs and every failed attempt. No maximum-depth expansion is required.

SQL tests are the lowest layer that can cover the complete
catalog, FE planning, native 1FE+3BE, Parquet reading and commit paths together;
the FS and Connector unit tests separately protect policy/cache behavior.

`fixture.scala` is composed after the existing
`iceberg-delete-applicability/generate.scala` to use its original run-scoped
storage checks and official Iceberg 1.11.0 SDK imports. It creates only the
case's `${case_db}` namespace and exact named tables. The Spark wrapper owns
the original fixture budget and actual producer exit; the SQL runner owns
table/catalog cleanup, role shutdown and whole-run fixture cleanup. SDK file
and row readers close in `finally`; copied local Parquet files are removed in
`finally`. Spark failures keep their original logs and fail the case. No
independent timeout, shared-fixture start, upgrade or new test directive is
introduced.

## Independent expected values

The expected data is fixed before NovaRocks reads or writes it:

- The depth schema has 63 optional ARRAY containers and an `INT` leaf,
  exactly logical depth 64. Its schema JSON is 66 containers deep, within the
  SDK manifest reader's 127-container boundary. The SDK writes `(1, -128)`,
  `(1, -128)`, `(2, NULL-parent)`. Native appends add `(3, NULL-parent)` and
  `(5, 7)` in separate commits; CTAS preserves the same five-row bag. Each
  non-NULL scalar is wrapped in 63 singleton arrays. The Java SDK checks both
  complete bags, depth, table identities and actual Parquet footer IDs. After
  dropping the deep column and appending `4`, current IDs are `1,1,2,3,4,5`;
  the initial tag returns the complete original three-row bag.
- The future-partition case has one SDK-written `id BIGINT` row (`1`) in S0.
  Adding optional `future INT` and `identity(future)` are metadata-only
  commits. The SDK checks that the sole S0 snapshot still binds the old schema,
  that historical projection is `{"id":1}`, and that current projection is
  `{"id":1,"future":null}`. Current UPDATE moves `future` to `7`, then MERGE
  moves it to `9`; the SDK checks the complete final bag and an output file
  under the current `identity(future)` spec with partition `9`. The table UUID
  and historical S0 stay fixed while the current metadata pointer and snapshot
  change; two independent SDK receipts are compared by the case shell. Current
  NovaRocks already supports partition-moving UPDATE/MERGE, so the old WIP's
  rejection expectations were replaced with this existing behavior rather
  than introducing a new write prohibition. The expected snapshot count is
  three (initial append, UPDATE and MERGE).
- The Hadoop case uses the same ARRAY shape and ordinary `INT` leaf. Native
  id-only append writes `1,1,2` with NULL parents; another native append writes
  `(4, 7)` with 63 singleton arrays. After DROP and append `3`, current IDs are
  `1,1,2,3,4`; the saved tag retains the complete four-row deep bag. The SDK
  checks all three snapshots, both complete bags, retained schema depth/IDs
  and actual Parquet footer IDs. This remains a MinIO-only, single-writer
  test catalog under ADR-0196. S3A serves only file discovery; the original
  run's S3FileIO reads the table's original S3 locations.
- Both depth cases also submit a logical64 pure STRUCT definition. Its schema
  JSON has 192 containers, exceeding the SDK manifest boundary, so CREATE must
  return the typed Unsupported reason before metadata publication. A subsequent
  same-name shallow CREATE without `IF NOT EXISTS`, then DROP, verifies no
  catalog object survived. Connector unit tests protect refusal before I/O.

Typed goldens are derived from these SDK declarations and fixed rows: `id`
and counts are `bigint`, `future` is `integer`, `typeof` is `varchar`; `deep`
is 63 nested Trino `array` types around `integer`. Values are the corresponding
63 nested JSON arrays or SQL `NULL`; leaf `typeof` is `int`. Native historical
queries read `deep` as well as `id`, and the SDK independently validates their
complete historical bags. These checked-in expectations do not claim that the
current product or fixture has already passed.

Run through the prepared, hash-bound SQL runner with a verified immutable
fixture publication and explicit `NOVAROCKS_BIN`:

```bash
"$sql_runner" --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg,iceberg-ddl --mode verify \
  --skip novarocks_full_metadata_depth64,full_metadata_depth64_hadoop \
  --cluster-mode cross-process --cluster-size 3 -j 1 \
  --results-file "$attempt/iceberg-read-policy.results.jsonl"
```

`--only` with the ordinary history case is useful for diagnosis; complete suite
runs with the two declared depth exclusions remain the B combination scope.
Restoring the depth cases requires a separate capability review after the
upstream manifest API/version is suitable, followed by complete native
1FE+3BE verification and cleanup. The unresolved Hadoop DML Invariant is
retained as unconfirmed; do not attribute it to the SDK or Writer32 without
evidence. Shell logs and SDK receipts
are retained below `tests/sql/.runtime/iceberg-read-policy/<case UUID>/`; each
stage uses a distinct filename. Failed attempts are retained, and reruns have
fresh case UUIDs. The old WIP inputs were audited at commit `28e444127`; only
these cases and the required official SDK operations were carried forward.
The obsolete MV fixture, old namespace construction, private 120-second
deadline and old TSV goldens were not retained.
