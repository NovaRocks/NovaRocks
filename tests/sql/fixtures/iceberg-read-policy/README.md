# Iceberg read policy inputs

The `iceberg` cases `novarocks_full_metadata_depth64` and
`novarocks_history_future_partition_source`, and the `iceberg-ddl` case
`full_metadata_depth64_hadoop`, protect file/table identity, deep metadata and
the separation between historical reads and current write authority. They are
ordinary correctness cases, maintained by the Iceberg suite owner and run with
their suites. SQL tests are the lowest layer that can cover the complete
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

- The depth schema has 63 optional struct containers and an integer leaf,
  with the leaf declared as `tinyint` by its actual Iceberg field ID. The SDK
  writes `(1, -128)`, `(1, -128)`, `(2, NULL-parent)`. Native append adds
  `(3, NULL-parent)` and CTAS must preserve the same four-row bag. The Java SDK
  checks both complete bags, depth, leaf declaration, table identities and
  actual Parquet footer IDs. After dropping the deep column and appending
  `4`, the current IDs are `1,1,2,3,4`, while the initial tag returns `1,1,2`.
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
- The Hadoop case uses the same 63-container shape with an ordinary `INT`
  leaf and only NULL parent values. Its written IDs are `1,1,2`, then `3` after
  dropping the deep field. Current IDs are `1,1,2,3`; the saved tag returns
  `1,1,2`. An official SDK Hadoop catalog reads that exact native-created
  table, checks both complete bags, retained schema depth/IDs and actual
  Parquet footer IDs. It is explicitly restricted to the MinIO fixture and
  remains a single-writer test catalog under ADR-0196. S3A is used only for
  Hadoop's file discovery; the original run's configured S3FileIO reads the
  table's original S3 locations.

Typed goldens are derived from these SDK declarations and fixed rows: `id`
and counts are `bigint`, `future` is `integer`, `typeof` is `varchar`; a struct
is a Trino `row` and its value is a positional JSON array. Nested row type
names come directly from `n1` through `n63`; the REST leaf is `tinyint` and the
Hadoop leaf is `integer`. Their `typeof` text remains the engine's declared
SQL names `tinyint` and `int`. These checked-in expectations are not a claim
that the current product or fixture has already passed.

Run through the prepared, hash-bound SQL runner with a verified immutable
fixture publication and explicit `NOVAROCKS_BIN`:

```bash
"$sql_runner" --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg,iceberg-ddl --mode verify \
  --cluster-mode cross-process --cluster-size 3 -j 1 \
  --results-file "$attempt/iceberg-read-policy.results.jsonl"
```

`--only` with the three case names is useful for diagnosis; complete suite
runs remain the required combination evidence. Shell logs and SDK receipts
are retained below `tests/sql/.runtime/iceberg-read-policy/<case UUID>/`; each
stage uses a distinct filename. Failed attempts are retained, and reruns have
fresh case UUIDs. The old WIP inputs were audited at commit `28e444127`; only
these cases and the required official SDK operations were carried forward.
The obsolete MV fixture, old namespace construction, private 120-second
deadline and old TSV goldens were not retained.
