# Iceberg Variant interoperability fixture

此正确性 fixture 的配方推荐 Iceberg Java 1.11.0 与 Spark 3.5，版本集中在 `docker/fixture-inputs/versions.json`。已有 Spark fixture 直接使用，不进行自动环境版本或 BOM 准入；实际 Java 写入与内容 oracle 仍须验证。Java API 创建 Variant/UUID 表；可选通用 reader 探针的既有观测限制见下文。
Spark SQL does not need to represent Variant values.

The protected product properties are reading externally authored unshredded and
shredded Variant, preserving ordinary struct identity, refusing unsupported data
writes before publication or physical data creation, projecting unrelated fields
of nested-Variant tables, and retaining supported DV/equality-delete behavior.
The existing NovaRocks-only tests cannot supply independent Variant files or an
independent reader, so a Java-backed SQL correctness fixture is the minimum
sufficient level. Expected values come from fixed Java construction inputs and
the Variant physical-type specification, not from NovaRocks output.

The Iceberg connector owns this fixture and its maintenance. Consumers are the
`java_variant_*`, `java_uuid_write_rejected`, and
`java_timestamptz_write_adaptation` cases in `iceberg-compatibility`, the three
`iceberg-dml/variant_*` cases, and `optimizer/variant_path_pushdown`. Run the
affected native cases when changing the corresponding fixture mode; the full
independent type matrix is a separate validation boundary described below.

```bash
# Resolve the generated immutable fixture publication once before invoking consumers.
fixture_publication="$(python3 -c 'from pathlib import Path; print(Path("docker/iceberg-rest/runtime/current/published").resolve(strict=True))')"
source "$fixture_publication/env.sh"
receipt_parent="${XDG_STATE_HOME:-$HOME/.local/state}/novarocks/experiments/iceberg-variant"
mkdir -p "$receipt_parent"
out="$(mktemp -d "$receipt_parent/$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX")"
tests/sql/fixtures/iceberg-variant/run.sh \
  --mode unshredded --namespace iru9_example --table variants --output "$out"
```

Modes `unshredded`, `shredded`, `bounds`, `nested`, `uuid`, and `write-target`
create one new table. `observe` prints sorted IDs through `IcebergGenerics`.
`unchanged` compares both the current snapshot and the physical object-store
`data/` listing with facts saved before the rejected operations. The shredded
mode supplies an explicit `VariantShreddingFunction`; its manifest omits Variant
bounds. The bounds mode retains and asserts real Java writer Variant bounds,
so the NovaRocks hint test cannot pass accidentally on a bounds-free file.

The three empty modes replace the former NovaRocks Variant CREATE setup, which
is now unsupported even for format-v3 tables:

| Mode | Exact input schema and properties | Consumers |
|---|---|---|
| `empty-int` | optional `id: int`, optional `v: variant`; unpartitioned | `variant_get`, `variant_insert`, `variant_path_pushdown` |
| `empty-bigint` | optional `id: long`, optional `v: variant`; unpartitioned, `write.row-lineage=true` | `variant_mutations` |
| `empty-region` | optional `id: long`, optional `region: string`, optional `v: variant`; `identity(region)` partition | `variant_mutations` |

Java creates these tables without any append, including an empty-file append.
The fixture checks the published schema's field IDs/types/nullability, partition
fields, explicit properties, format version, empty physical `data/` listing, and
absence of both a current snapshot and snapshot history. Saving the no-change
receipt changes only properties and is checked again to preserve no snapshot.
The fixed shell result markers report these asserted facts; they are not derived
from NovaRocks output. This preserves the original missing-statistics EXPLAIN
oracle and the existing empty-snapshot DML boundary rather than seeding data to
make them pass. `unchanged` also understands the explicit `absent` snapshot fact.

Each migrated case creates its own explicit REST catalog alias with the same
generated endpoint, warehouse and credential binding as this Java fixture. The
suite's shared init and ordinary Hadoop catalog remain unchanged; in particular,
`variant_mutations` keeps its ordinary MERGE source in the original catalog and
uses full REST names only for its Java-authored Variant targets.

The optimizer case retains its deterministic `${case_db}` namespace and original
table name so every cell of the full EXPLAIN golden remains meaningful. Its REST
alias is unique, and the case owns that namespace/table at the original case
isolation granularity under the global SQL-runner mutex. Java creation fails if
a previous failed table still exists: there is no reuse, drop-existing fallback,
or golden normalization. Resolve and retain that failed attempt before explicitly
cleaning its owned table and rerunning. Other migrated cases use fresh UUID names.

Each run requires a fresh output directory. SQL consumers allocate it under the
existing XDG state experiment root; `IRU9_ARTIFACT_DIR` in the native shell output
links retained Scala and Spark output to that case. The connector owner retains
failed and formally referenced inputs until their investigation and handoff are
complete; successful probes are not automatically deleted. It retains the exact generated Scala
script and complete Spark log, and succeeds only with `IRU9_FIXTURE_COMPLETE`.
Scala exceptions exit nonzero; Scala compilation errors are also caught by the
missing completion marker. Runtime versions are checked explicitly. SQL cases
use unique table names and drop their tables on success. Failed run artifacts
and tables remain for investigation under the runner's normal evidence policy;
the fixture does not purge shared storage or rewrite expected results.

The full independent footer/type matrix and Spark row/vectorized interoperability
matrix were explicitly waived for IRU-9 on 2026-10-15. They are not implemented or
claimed by this fixture. Focused data/equality writer tests independently inspect
Parquet footer annotations with parquet-rs; this fixture retains the independent
Java Variant read/write and no-side-effect checks.

此前 fixture 镜像的通用 Java Variant reader 已观测到以下失败；该历史观测不证明任意现有机器的当前状态：
`NoSuchMethodError: commons.lang3.stream.Streams.of(Iterable)`. `observe` remains
an explicit independent probe and is not called by the default SQL cases while
the approved independent-read exception applies. No implicit host Maven jar or
classpath override is used. Generation receipts depend on successful Java writes,
actual writer metrics/bounds, and catalog publication; NovaRocks reads the fixed
fixture values in the SQL cases.

### 共享机器运行预算

共享消费者通过本次不可变 publication 读取机器事实，继承
`NOVA_FIXTURE_RUN_CONTEXT` 的 run 身份与原截止时间。嵌套 Spark 调用不会重置
预算；容器内 owner 负责准确进程组的终止与实际 wait。正常清理失败会返回失败，
不能以未来 TTL 清扫代替本次清理。私有 publication 保留其原有资源 owner。
