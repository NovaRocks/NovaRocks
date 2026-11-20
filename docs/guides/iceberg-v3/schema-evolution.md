<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Schema 演进 DDL

> Iceberg 通过 field-id 维护 schema 历史，支持 ADD / DROP / RENAME / type widening / reorder / required↔optional 等"安全"变更。Phase 2（PR #86 / #88 / #89）落地后，**§5 schema 演进 10 项全部支持**，包含嵌套 STRUCT / ARRAY / MAP 元素、commit 冲突重试、`SET / UNSET TBLPROPERTIES` 全套。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| ADD COLUMN（顶层 + 嵌套 STRUCT 路径） | ✅ | PR #86 |
| DROP COLUMN（含嵌套） | ✅ | PR #86 |
| RENAME COLUMN（含嵌套） | ✅ | PR #86 |
| 类型 widening（int→long / float→double / decimal precision↑ / date→timestamp） | ✅ | PR #86 |
| ARRAY / MAP 元素类型 widening（`<list>.element` / `<map>.key` / `<map>.value`） | ✅ | PR #86 |
| Column reorder（`EXECUTE move_column`，顶层 + 嵌套） | ✅ | PR #86 |
| `SET / DROP NOT NULL`（含 identifier-field 保护 + `novarocks.nullability.attested.*` 留痕） | ✅ | PR #86 |
| DDL 失败原子回滚（commit 冲突 3 次指数退避 10/100/500ms 重试，每次重试前 invalidate cache + 重 load） | ✅ | PR #88 |
| `ALTER TABLE ... SET / UNSET TBLPROPERTIES`（denylist：`novarocks.*` 全前缀 + `format-version` + Iceberg 内部 schema/spec/last-* 键） | ✅ | PR #89 |

实现入口：`src/connector/iceberg/catalog/schema_update.rs`

参考 spec：[2026-05-06-iceberg-schema-evolution-phase2-design.md](../../design/specs/2026-05-06-iceberg-schema-evolution-phase2-design.md)

---

## ✅ ADD COLUMN

```sql
-- 顶层加列
ALTER TABLE t ADD COLUMN c STRING;
ALTER TABLE t ADD COLUMN c STRING DEFAULT 'pending';     -- v3 default value，详见 default-values.md

-- 嵌套加列（在 STRUCT 内插字段，按 field-id 维护）
ALTER TABLE t ADD COLUMN address.zip INT;

-- 配合位置子句
ALTER TABLE t ADD COLUMN c INT FIRST;
ALTER TABLE t ADD COLUMN c INT AFTER existing_col;
-- 退役位置子句，解析阶段拒绝
ALTER TABLE t ADD COLUMN c INT BEFORE existing_col;
```

新加列默认 nullable（`optional` in Iceberg 术语）；带 DEFAULT 时写入 `initial-default` + `write-default`。嵌套字段路径用 `.` 分段；目标父级必须是 STRUCT（往 LIST / MAP / 原始类型加字段会被 reject）。

## ✅ DROP COLUMN

```sql
ALTER TABLE t DROP COLUMN c;
ALTER TABLE t DROP COLUMN address.street;
```

DROP 是逻辑删除：新 schema 中移除字段，老 data file 中的对应列被读端忽略。Equality-delete 文件引用该列时会 reject（避免读路径无法解释 delete 行）；StarRocks MV 引用该列时也 reject。

## ✅ RENAME COLUMN

```sql
ALTER TABLE t RENAME COLUMN old_name TO new_name;
ALTER TABLE t RENAME COLUMN address.zip TO postal_code;
```

按 field-id 维系，老 data file 不需要重写。如果新名字与同父级下任何兄弟字段冲突会 reject。

## ✅ ALTER COLUMN … SET DATA TYPE（类型 widening）

按 spec 允许的方向：

| 起点 | 终点 |
| --- | --- |
| `int` | `long` |
| `float` | `double` |
| `decimal(p1, s)` | `decimal(p2, s)` 其中 `p2 > p1`、scale 不变 |
| `date` | `timestamp` |

```sql
ALTER TABLE t ALTER COLUMN c SET DATA TYPE BIGINT;             -- int → long
ALTER TABLE t ALTER COLUMN price SET DATA TYPE DECIMAL(20, 4); -- precision 增加
ALTER TABLE t ALTER COLUMN created_on SET DATA TYPE TIMESTAMP; -- date → timestamp

-- 嵌套 STRUCT 内 widen
ALTER TABLE t ALTER COLUMN address.zip SET DATA TYPE BIGINT;

-- ARRAY 元素 widen
ALTER TABLE t ALTER COLUMN tags.element SET DATA TYPE BIGINT;

-- MAP 值 widen
ALTER TABLE t ALTER COLUMN attrs.value SET DATA TYPE BIGINT;

-- MAP 键 widen
ALTER TABLE t ALTER COLUMN attrs.key SET DATA TYPE VARCHAR;
```

下列变更会 reject（与 spec 不安全方向一致）：

- 任何 narrow（`long → int` / `double → float` / `decimal` precision 减小 / `timestamp → date`）
- `decimal` scale 改变（含同 precision 异 scale）
- 跨族（`string → binary` / `int → string` / 等等）
- `timestamp` ↔ `timestamptz` 互转（暂未实现，归 V3 timestamp_ns 工作）

> 改类型使用 `ALTER COLUMN … SET DATA TYPE`。需要调整位置时另行调用 `move_column`。

## ✅ Column reorder

NovaRocks 用 `move_column` 过程调整已有列的位置。

```sql
ALTER TABLE t EXECUTE move_column(column => 'c', position => 'FIRST');
ALTER TABLE t EXECUTE move_column(column => 'c', position => 'AFTER', reference => 'other_col');
ALTER TABLE t EXECUTE move_column(column => 'address.street', position => 'BEFORE', reference => 'address.city');  -- 嵌套同样支持
```

`AFTER` / `BEFORE` 的目标必须与被移动列在同一父级 STRUCT；跨父级引用直接 reject。

## ✅ 设置和解除 NOT NULL（required ↔ optional）

```sql
ALTER TABLE t EXECUTE set_column_not_null(column => 'c');
ALTER TABLE t ALTER COLUMN c DROP NOT NULL;
ALTER TABLE t EXECUTE set_column_not_null(column => 'address.street');
```

- `EXECUTE set_column_not_null`（optional → required）：不扫老数据；commit 时附带 property `novarocks.nullability.attested.<dot.path> = <iso8601_ts>` 留痕（行为对齐 Spark "user attested"）
- `DROP NOT NULL`（required → optional）：spec 安全方向，直接 commit
- identifier field（主键）禁止 `DROP NOT NULL`（spec 要求 identifier 必须 required）
- `novarocks.nullability.attested.*` 这一族 property 由 schema 演进路径维护，PR #89 SET TBLPROPERTIES 路径会 reject 用户直接修改

## DDL 发布与失败结果

schema、分区、属性与 ref 等直接 DDL 路径冻结原表 UUID 和必要 requirements，经 prepared catalog owner 与唯一 journal 单次派发。确定冲突返回冲突，调用方可以重新发起新语句；Provider 不在这些路径内隐式重试。准备和锁等待遵循语句 deadline/stop；请求可能已发出后使用独立有限证据期限，Unknown 不重新派发，也不授权删除产物。

只有快照过期、manifest 重写等明确支持的维护入口，才在确定冲突后按操作意图、固定评价时间和重试总预算重准备。Applied 与物化/缓存收尾分开报告；Unknown 不能推出 metadata 未改变。详见 [ADR-0179](../../adr/ADR-0179-iceberg-publication-lifecycle-and-evidence.md)。

## ✅ SET / UNSET TBLPROPERTIES

PR #89 引入：

``` sql
ALTER TABLE t SET PROPERTIES extra_properties = MAP(ARRAY['write.parquet.compression-codec'], ARRAY['zstd']);
ALTER TABLE t SET PROPERTIES gc_enabled = true, extra_properties = MAP(ARRAY['comment'], ARRAY['hello']);

ALTER TABLE t EXECUTE unset_extra_properties(keys => ARRAY['comment'], if_exists => false);
ALTER TABLE t EXECUTE unset_extra_properties(keys => ARRAY['a', 'b'], if_exists => true);   -- 缺失键静默跳过
```

**Denylist**（写 / 删都拒绝）：

| 类别 | 键 / 前缀 | 原因 |
| --- | --- | --- |
| NovaRocks 私有命名空间 | `novarocks.*`（含未知键） | 留扩展空间，避免用户先占用未来 NovaRocks 内部键 |
| Iceberg 标识 | `format-version` | 升级走未来 `UPGRADE TABLE` 语法（暂未实现） |
| Iceberg 标识 | `identifier-field-ids` / `current-schema-id` / `default-spec-id` / `default-sort-order-id` / `last-column-id` / `last-partition-id` / `last-sequence-number` | 引擎内部维护 |

**严格性**：

- SET：同一语句重复键 reject；空 parens reject；非 string 字面量 key/value reject
- UNSET：默认 strict（缺键 fail）；`IF EXISTS` 切换为静默跳过；同一语句重复键 reject
- SET 与 UNSET 不能在同一语句混用（grammar 层禁止；和 Spark / Hive 行为一致）
- 任何键违反 denylist → 整条 ALTER reject，`metadata.json` 不动；错误信息包含被拒的键 + 原因

`UNSET IF EXISTS` 在所有目标键都已不存在时短路，避免无差别 metadata.json 版本递增。

放行键（默认允许 SET / UNSET，仅写入 `metadata.json` 的 `properties` map，不在引擎层产生副作用）：

- `write.format.default` / `write.parquet.compression-codec` / `write.parquet.row-group-size-bytes` / `write.parquet.page-size-bytes` / `write.target-file-size-bytes`
- `write.metadata.compression-codec` / `write.metadata.previous-versions-max`
- `history.expire.max-snapshot-age-ms` / `history.expire.min-snapshots-to-keep` / `history.expire.max-ref-age-ms`
- `commit.retry.num-retries` / `commit.retry.min-wait-ms` / `commit.retry.max-wait-ms` / `commit.retry.total-timeout-ms`
- `gc.enabled`
- 其他用户自定义键

> **重要**："放行" ≠ "引擎遵守"。比如 `write.parquet.compression-codec=zstd` 设置后，NovaRocks parquet writer 是否真读这个属性是另一件事 —— 本次只保证用户能写、cross-engine 读得到、不被引擎吞掉。哪些 key 实际被消费会随后续 audit 文档化。
