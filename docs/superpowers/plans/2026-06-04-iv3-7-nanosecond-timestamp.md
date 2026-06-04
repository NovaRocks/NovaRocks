# IV3-7 纳秒精度时间类型 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 standalone Iceberg 模式支持 Iceberg v3 纳秒时间类型 `timestamp_ns` / `timestamptz_ns`，读不截断、谓词正确、可写、可原生 CREATE `timestamp_ns`。

**Architecture:** 方案 A——让内部 thrift 描述符 `TScalarType` 携带时间单位（新增 optional `time_unit` 字段），codegen 编码、lowering 解码，纳秒精度端到端贯通 DDL → SqlType → Arrow → thrift desc → lower → scan → exec → write。新增 `SqlType::DateTimeNs` 逻辑类型供原生 DDL，新增 `MinMaxPredicateValue::DateTimeNanos` 供谓词下推。Arrow `RecordBatch` 是执行层权威类型，纳秒在 Arrow 层天然可表达；本计划只补"丢单位的往返"与各 unitless 映射点。

**Tech Stack:** Rust，Arrow（`arrow::datatypes::{DataType, TimeUnit}`），vendored iceberg-rust 0.9，thrift（`idl/thrift/Types.thrift` → 构建期生成 `crate::types`），sql-test runner（`tests/sql-test-runner`），本地 iceberg-rest + MinIO + Spark fixture（`docker/iceberg-rest/`）。

**设计 spec:** `docs/superpowers/specs/2026-06-04-iv3-7-nanosecond-timestamp-design.md`

**关键事实（盘点结论，实现时无需重新调研）:**
- catalog `ColumnDef.data_type` 对纳秒 Iceberg 列已是 `Timestamp(Nanosecond)`（`src/connector/iceberg/catalog/registry.rs:797`），Arrow 是权威类型。
- 读截断根因：`src/sql/codegen/type_infer.rs:151` 把 `Timestamp(_,_)` 都塌缩成 unitless `DATETIME`，再经 `src/lower/type_lowering.rs:130` 还原成微秒，`align_batch_to_iceberg_schema` 据此把纳秒数组 cast 成微秒。
- Iceberg sink `src/connector/iceberg/sink.rs:833-834` 已映射 `Timestamp(Nanosecond)` → `TimestampNs/TimestamptzNs`，写侧类型映射就绪。
- parquet 行组裁剪走 `predicate.value().as_i64()`（`src/formats/parquet/mod.rs:1238-1366`，共 8 处）。
- exchange 用 Arrow IPC，且 `src/runtime/exchange.rs:114,158` 对 `(Timestamp,Timestamp)` 采纳 `actual.clone()`，纳秒数组在 exchange 透传。

---

## File Structure

**修改（类型单位往返，Phase 1）:**
- `idl/thrift/Types.thrift` — `TScalarType` 新增 `5: optional i32 time_unit`。
- `src/lower/type_lowering.rs` — time_unit 常量/编码 helper；`arrow_type_from_nodes` DATETIME arm 按单位解码；构造点补参。
- `src/sql/codegen/type_infer.rs` — `append_arrow_type_nodes` 新增 Timestamp arm 编码单位；构造点补参。
- 其余 11 个 `TScalarType` 构造点补 `None` / `time_unit: None`（见 Task 1）。

**修改（SqlType + 原生 DDL + 写，Phase 2）:**
- `src/sql/parser/ast/mod.rs` — `enum SqlType` 新增 `DateTimeNs`。
- `src/sql/parser/dialect/mod.rs` — `timestamp_ns`/`timestamptz_ns` → `DateTimeNs`。
- `src/sql/parser/dialect/create_table.rs` — 默认值解析补 `DateTimeNs`。
- `src/engine/sql_expr.rs` — `sql_type_to_arrow_type` 补 `DateTimeNs → Timestamp(Nanosecond,None)`。
- `src/connector/iceberg/catalog/registry.rs` — `iceberg_type_for_sql_type` 补 `DateTimeNs → TimestampNs`。
- `src/connector/iceberg/default_value.rs` — 默认值双向映射补 `DateTimeNs`。
- `src/connector/iceberg/catalog/schema_update.rs` — schema 演进映射补 `DateTimeNs`。
- `src/connector/starrocks/table/{ddl,mv_ddl}.rs` — SqlType 显示/解析补 `DateTimeNs`（standalone DDL 文本）。
- `src/engine/iceberg_ctas.rs` — `arrow_data_type_to_sql_type` 按单位区分。
- `src/engine/insert.rs` — INSERT 字面量按目标列单位建纳秒数组；arrow→SqlType 补纳秒。
- `src/engine/parquet.rs` — 新增 `parse_datetime_string_to_nanos`。
- `src/formats/parquet/mod.rs` — `arrow_type_to_iceberg_type`（:1738）补纳秒 arm。

**修改（谓词，Phase 5）:**
- `src/common/min_max_predicate.rs` — `MinMaxPredicateValue` 新增 `DateTimeNanos(i64)` + `as_i64` arm。
- `src/lower/expr/min_max.rs` — `time_unit_from_node` + `parse_datetime_literal_nanos` + extract_*_literal 纳秒分支。

**修改（cast，Phase 6）:**
- `src/exec/expr/cast.rs` — 纳秒 cast：收窄截断、拓宽溢出报错。

**修改（分区 fail-fast，Phase 7）:**
- Iceberg 分区 transform 路径（见 Task 13）。

**新增（测试 fixture）:**
- `sql-tests/iceberg-rest/sql/timestamp_ns_roundtrip.sql` + `result/...`（NovaRocks-only 端到端）。
- `sql-tests/iceberg-compatibility/sql/spark_rest_minio_v3_timestamp_ns.sql` + `result/...`（跨引擎）。

---

## Phase 1 — 类型单位往返基础（lib 单测，无外部依赖）

### Task 1: TScalarType 新增 time_unit 字段并修全部构造点编译通过

**Files:**
- Modify: `idl/thrift/Types.thrift:117-126`
- Modify: `src/lower/type_lowering.rs`（顶部加常量 + 构造点 :48、:281）
- Modify: `src/sql/codegen/type_infer.rs:89,104`
- Modify: `src/server/encoding.rs:554`、`src/common/util.rs:1600`、`src/exec/expr/cast.rs:3830`、`src/lower/node/project.rs:333`
- Modify: `src/connector/starrocks/lake/schema.rs:1670,1802`、`src/connector/starrocks/table/ddl.rs:1304`、`src/service/internal_rpc.rs:372,388`

- [ ] **Step 1: 给 thrift struct 加字段**

`idl/thrift/Types.thrift` 把
```thrift
struct TScalarType {
    1: required TPrimitiveType type
    2: optional i32 len
    3: optional i32 precision
    4: optional i32 scale
}
```
改为
```thrift
struct TScalarType {
    1: required TPrimitiveType type
    2: optional i32 len
    3: optional i32 precision
    4: optional i32 scale

    // NovaRocks-only: time unit code for DATETIME (THRIFT_TIME_UNIT_* in
    // src/lower/type_lowering.rs). Absent = microsecond (FE-compat default);
    // only nanosecond is additionally produced by the standalone codegen.
    5: optional i32 time_unit
}
```

- [ ] **Step 2: 加时间单位常量**

`src/lower/type_lowering.rs` 在 `use` 之后加：
```rust
/// Thrift `TScalarType.time_unit` codes for DATETIME descriptors. Absent
/// (`None`) means microsecond so FE-compat descriptors stay byte-identical;
/// only nanosecond is additionally produced by the standalone codegen.
pub(crate) const THRIFT_TIME_UNIT_MICROS: i32 = 2;
pub(crate) const THRIFT_TIME_UNIT_NANOS: i32 = 3;

/// Map an Arrow `TimeUnit` to the thrift descriptor code. Microsecond maps to
/// `None` to preserve FE-compat byte-identical descriptors. Only Microsecond
/// and Nanosecond are supported; other units are an explicit error.
pub(crate) fn thrift_time_unit_for_arrow(
    unit: arrow::datatypes::TimeUnit,
) -> Result<Option<i32>, String> {
    use arrow::datatypes::TimeUnit;
    match unit {
        TimeUnit::Microsecond => Ok(None),
        TimeUnit::Nanosecond => Ok(Some(THRIFT_TIME_UNIT_NANOS)),
        other => Err(format!(
            "unsupported timestamp unit {other:?} for thrift descriptor; only Microsecond/Nanosecond supported"
        )),
    }
}
```

- [ ] **Step 3: 修 7 个 `TScalarType::new(...)` 构造点，补第 5 个参数 `None`**

每处把 `TScalarType::new(<type>, <len>, <precision>, <scale>)` 改为 `TScalarType::new(<type>, <len>, <precision>, <scale>, None)`：
- `src/server/encoding.rs:554`（`TPrimitiveType::JSON, None, None, None` → 加 `, None`）
- `src/common/util.rs:1600`（同上）
- `src/exec/expr/cast.rs:3830`（同上）
- `src/lower/type_lowering.rs:48`（`scalar_type_desc` 内 `primitive, None, None, None` → 加 `, None`）
- `src/lower/node/project.rs:333`（本地 `scalar_type_desc`，`ty, None, None, None` → 加 `, None`）
- `src/sql/codegen/type_infer.rs:89`（DECIMAL128：`..., Some(i32::from(*s))` → 加 `, None`）
- `src/sql/codegen/type_infer.rs:104`（DECIMAL256：同上加 `, None`）

- [ ] **Step 4: 修 6 个 `TScalarType { ... }` 结构体字面量，补 `time_unit`**

- `src/connector/starrocks/lake/schema.rs:1670`（DECIMALV2）→ 加 `time_unit: None,`
- `src/connector/starrocks/lake/schema.rs:1802`（BIGINT）→ 加 `time_unit: None,`
- `src/connector/starrocks/table/ddl.rs:1304`（拷贝另一个 scalar）→ 加 `time_unit: scalar.time_unit,`（保留传递）
- `src/service/internal_rpc.rs:372`（INT）→ 加 `time_unit: None,`
- `src/service/internal_rpc.rs:388`（BIGINT）→ 加 `time_unit: None,`
- `src/lower/type_lowering.rs:281`（#[cfg(test)] DECIMALV2）→ 加 `time_unit: None,`

- [ ] **Step 5: 构建通过**

Run: `cargo build`
Expected: 编译通过，无 `TScalarType` 缺字段/参数错误。若报某处仍缺，按报错补 `None` / `time_unit: None`（说明有未列出的构造点，按相同模式处理）。

- [ ] **Step 6: Commit**

```bash
git add idl/thrift/Types.thrift src/
git commit -m "feat(iv3-7): add optional time_unit field to TScalarType descriptor"
```

---

### Task 2: codegen 编码 + lowering 解码时间单位（往返保单位）

**Files:**
- Modify: `src/sql/codegen/type_infer.rs`（`append_arrow_type_nodes`，:118 前加 Timestamp arm）
- Modify: `src/lower/type_lowering.rs`（`arrow_type_from_nodes` DATETIME arm，:130）
- Test: `src/sql/codegen/type_infer.rs`（#[cfg(test)] 加往返测试）

- [ ] **Step 1: 写失败测试（往返保单位）**

在 `src/sql/codegen/type_infer.rs` 的 `#[cfg(test)] mod tests` 加（无则新建）：
```rust
#[test]
fn timestamp_unit_roundtrips_through_thrift_desc() {
    use arrow::datatypes::{DataType, TimeUnit};
    use crate::lower::type_lowering::arrow_type_from_desc;

    // microsecond stays microsecond (FE-compat default)
    let micro = DataType::Timestamp(TimeUnit::Microsecond, None);
    let desc = arrow_type_to_type_desc(&micro).unwrap();
    assert_eq!(arrow_type_from_desc(&desc), Some(micro));

    // nanosecond must survive the round-trip (the bug this task fixes)
    let nano = DataType::Timestamp(TimeUnit::Nanosecond, None);
    let desc = arrow_type_to_type_desc(&nano).unwrap();
    assert_eq!(
        arrow_type_from_desc(&desc),
        Some(DataType::Timestamp(TimeUnit::Nanosecond, None))
    );
}

#[test]
fn unsupported_timestamp_unit_is_rejected() {
    use arrow::datatypes::{DataType, TimeUnit};
    let sec = DataType::Timestamp(TimeUnit::Second, None);
    assert!(arrow_type_to_type_desc(&sec).is_err());
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test timestamp_unit_roundtrips_through_thrift_desc unsupported_timestamp_unit_is_rejected`
Expected: FAIL —— nanosecond 往返返回 `Timestamp(Microsecond, None)`；second 未报错。

- [ ] **Step 3: 编码——在 `append_arrow_type_nodes` 加 Timestamp arm**

`src/sql/codegen/type_infer.rs` 在 `DataType::Decimal256(p, s) => { ... }` 之后、`_ => { ... }` 之前插入：
```rust
        DataType::Timestamp(unit, _tz) => {
            // Carry the time unit so the unitless thrift DATETIME descriptor does
            // not collapse nanosecond to microsecond. tz is intentionally not
            // carried (DATETIME descriptors are tz-less); the nanosecond *value*
            // is preserved regardless. Microsecond keeps time_unit absent so
            // FE-compat descriptors stay byte-identical.
            let time_unit = crate::lower::type_lowering::thrift_time_unit_for_arrow(*unit)?;
            let scalar = types::TScalarType::new(
                types::TPrimitiveType::DATETIME,
                None::<i32>,
                None::<i32>,
                None::<i32>,
                time_unit,
            );
            nodes.push(types::TTypeNode::new(
                types::TTypeNodeType::SCALAR,
                scalar,
                None,
                None,
            ));
            Ok(())
        }
```

- [ ] **Step 4: 解码——`arrow_type_from_nodes` DATETIME arm 按单位还原**

`src/lower/type_lowering.rs` 把 `arrow_type_from_nodes` 内
```rust
                t if t == types::TPrimitiveType::DATETIME => {
                    DataType::Timestamp(TimeUnit::Microsecond, None)
                }
```
改为
```rust
                t if t == types::TPrimitiveType::DATETIME => {
                    let unit = match scalar.time_unit {
                        None => TimeUnit::Microsecond,
                        Some(c) if c == THRIFT_TIME_UNIT_MICROS => TimeUnit::Microsecond,
                        Some(c) if c == THRIFT_TIME_UNIT_NANOS => TimeUnit::Nanosecond,
                        Some(_) => return None,
                    };
                    DataType::Timestamp(unit, None)
                }
```

- [ ] **Step 5: 跑测试确认通过**

Run: `cargo test timestamp_unit_roundtrips_through_thrift_desc unsupported_timestamp_unit_is_rejected`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/sql/codegen/type_infer.rs src/lower/type_lowering.rs
git commit -m "feat(iv3-7): encode/decode timestamp time unit through thrift descriptor"
```

---

### Task 3: FE-compat 默认微秒回归断言

**Files:**
- Test: `src/lower/type_lowering.rs`（#[cfg(test)]）

- [ ] **Step 1: 写测试——无 time_unit 的 DATETIME 描述符仍解码为微秒**

`src/lower/type_lowering.rs` tests 加：
```rust
#[test]
fn datetime_desc_without_time_unit_defaults_to_microsecond() {
    use arrow::datatypes::{DataType, TimeUnit};
    // An FE-style descriptor never sets time_unit; it must stay microsecond.
    let desc = TTypeDesc {
        types: Some(vec![TTypeNode {
            type_: TTypeNodeType::SCALAR,
            scalar_type: Some(TScalarType {
                type_: TPrimitiveType::DATETIME,
                len: None,
                precision: None,
                scale: None,
                time_unit: None,
            }),
            is_named: None,
            struct_fields: None,
        }]),
    };
    assert_eq!(
        arrow_type_from_desc(&desc),
        Some(DataType::Timestamp(TimeUnit::Microsecond, None))
    );
}
```

- [ ] **Step 2: 跑测试确认通过**

Run: `cargo test datetime_desc_without_time_unit_defaults_to_microsecond`
Expected: PASS（守护 "不改默认 DATETIME 精度" 与 FE-compat 字节一致）

- [ ] **Step 3: Commit**

```bash
git add src/lower/type_lowering.rs
git commit -m "test(iv3-7): assert FE-compat DATETIME descriptors stay microsecond"
```

---

## Phase 2 — SqlType + 原生 DDL + 写类型映射

### Task 4: 新增 `SqlType::DateTimeNs` 与解析 / Arrow 映射

**Files:**
- Modify: `src/sql/parser/ast/mod.rs:359-386`
- Modify: `src/sql/parser/dialect/mod.rs`（`convert_sql_type` Custom arm ~:166-199；`parse_modifier_to_sql_type` :223）
- Modify: `src/engine/sql_expr.rs:1225`
- Modify: `src/sql/parser/dialect/create_table.rs:1094`
- Test: `src/sql/parser/dialect/mod.rs`（#[cfg(test)]）

- [ ] **Step 1: 写失败测试——解析 + Arrow 映射**

`src/sql/parser/dialect/mod.rs` tests 加：
```rust
#[test]
fn timestamp_ns_type_name_parses_to_datetimens() {
    assert_eq!(parse_modifier_to_sql_type("timestamp_ns"), Ok(SqlType::DateTimeNs));
    assert_eq!(parse_modifier_to_sql_type("timestamptz_ns"), Ok(SqlType::DateTimeNs));
}

#[test]
fn datetimens_maps_to_nanosecond_arrow() {
    use arrow::datatypes::{DataType, TimeUnit};
    assert_eq!(
        crate::engine::sql_expr::sql_type_to_arrow_type(&SqlType::DateTimeNs),
        Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
    );
}
```

- [ ] **Step 2: 跑测试确认失败（编译错误：无 DateTimeNs 变体）**

Run: `cargo test timestamp_ns_type_name_parses_to_datetimens datetimens_maps_to_nanosecond_arrow`
Expected: FAIL —— `no variant DateTimeNs`。

- [ ] **Step 3: 加枚举变体**

`src/sql/parser/ast/mod.rs` 在 `DateTime,` 之后加：
```rust
    /// Iceberg v3 nanosecond timestamp (`timestamp_ns`). Default DATETIME stays
    /// microsecond; this is a distinct variant so existing DATETIME behavior is
    /// untouched. Time-zone (`timestamptz_ns`) is carried at the Arrow level on
    /// read/insert; native CREATE of the tz variant is out of scope.
    DateTimeNs,
```

- [ ] **Step 4: 解析——两处类型名映射**

`src/sql/parser/dialect/mod.rs` 的 `convert_sql_type` 的 `Custom(name, modifiers)` 匹配里，在 `"variant" => Ok(SqlType::Variant),` 之后加：
```rust
                "timestamp_ns" | "timestamptz_ns" | "datetime_ns" => Ok(SqlType::DateTimeNs),
```
`parse_modifier_to_sql_type` 在 `"datetime" | "timestamp" => Ok(SqlType::DateTime),` 之后加：
```rust
        "timestamp_ns" | "timestamptz_ns" | "datetime_ns" => Ok(SqlType::DateTimeNs),
```

- [ ] **Step 5: SqlType → Arrow**

`src/engine/sql_expr.rs` 在 `SqlType::DateTime => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),` 之后加：
```rust
        SqlType::DateTimeNs => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
```

- [ ] **Step 6: create_table 默认值解析**

`src/sql/parser/dialect/create_table.rs` 的 `parse_string_default` 在 `SqlType::DateTime => { ... }` arm 之后加：
```rust
        SqlType::DateTimeNs => {
            let nanos = crate::engine::parquet::parse_datetime_string_to_nanos(s)?;
            Ok(DefaultLiteral::DateTime(nanos))
        }
```
（`parse_datetime_string_to_nanos` 在 Task 7 定义；本步若先编译会失败，可与 Task 7 合并提交，或先用 `parse_datetime_string_to_micros` 占位再在 Task 7 改。推荐：先在 Task 7 定义该函数，再回填本步。为保持顺序，这里只加 arm，构建留到 Step 7。）

- [ ] **Step 7: 构建并跑测试**

先确保 `parse_datetime_string_to_nanos` 存在（见 Task 7 Step 1，可提前落该函数），再：
Run: `cargo test timestamp_ns_type_name_parses_to_datetimens datetimens_maps_to_nanosecond_arrow`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add src/sql/parser/ src/engine/sql_expr.rs
git commit -m "feat(iv3-7): add SqlType::DateTimeNs with parser and arrow mapping"
```

---

### Task 5: SqlType::DateTimeNs 全量 match arm 补齐（编译完整性）

**Files:**
- Modify: `src/connector/iceberg/catalog/registry.rs:1891`
- Modify: `src/connector/iceberg/default_value.rs:104,188`
- Modify: `src/connector/iceberg/catalog/schema_update.rs:1983,3459`
- Modify: `src/connector/starrocks/table/ddl.rs:469,1210,1325,1355,1422`
- Modify: `src/connector/starrocks/table/mv_ddl.rs:1897`

- [ ] **Step 1: 构建找出所有缺 arm**

Run: `cargo build`
Expected: 多处 `non-exhaustive patterns: SqlType::DateTimeNs not covered`，逐一定位。

- [ ] **Step 2: DDL→Iceberg 类型映射（registry）**

`src/connector/iceberg/catalog/registry.rs` 在 `SqlType::DateTime => Type::Primitive(PrimitiveType::Timestamp),` 之后加：
```rust
        SqlType::DateTimeNs => Type::Primitive(PrimitiveType::TimestampNs),
```

- [ ] **Step 3: 默认值双向映射（default_value）**

`src/connector/iceberg/default_value.rs` 的 `default_literal_to_iceberg` 在 `(DefaultLiteral::DateTime(t), SqlType::DateTime) => PrimitiveLiteral::Long(*t),` 之后加：
```rust
        (DefaultLiteral::DateTime(t), SqlType::DateTimeNs) => PrimitiveLiteral::Long(*t),
```
`iceberg_literal_to_ast` 在 `SqlType::DateTime` arm 之后加（渲染 9 位小数）：
```rust
        (IcebergLiteral::Primitive(PrimitiveLiteral::Long(nanos)), SqlType::DateTimeNs) => {
            use chrono::DateTime as ChronoDateTime;
            let dt = ChronoDateTime::from_timestamp_nanos(*nanos);
            Ok(AstLiteral::String(
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.9f").to_string(),
            ))
        }
```

- [ ] **Step 4: schema 演进映射（schema_update）**

`src/connector/iceberg/catalog/schema_update.rs:1983` 处（`let new = SqlType::DateTime`）与 `:3459`（`(Type::Primitive(PrimitiveType::Date), SqlType::DateTime)`）按其上下文：凡 Iceberg `Timestamp`→SqlType 处保持 `DateTime`；新增 Iceberg `TimestampNs`→`SqlType::DateTimeNs` 的对应 arm（与 `Timestamp` arm 并列），并为类型 promotion 矩阵把 `DateTimeNs` 视作不可隐式转其它类型（沿用 `DateTime` 的限制）。具体：在每个对 `Type::Primitive(PrimitiveType::Timestamp)` / `SqlType::DateTime` 的 arm 旁补对应 `TimestampNs` / `DateTimeNs` arm。

- [ ] **Step 5: standalone DDL 文本（starrocks/table/ddl.rs, mv_ddl.rs）**

- `ddl.rs:469`、`:1325`（`SqlType::BigInt | SqlType::DateTime | SqlType::Time` 这类分组）→ 加上 `| SqlType::DateTimeNs`（与 DateTime 同组语义：8 字节、可排序）。
- `ddl.rs:1210`（`SqlType::DateTime => (TPrimitiveType::DATETIME, ...)`）→ 加 `SqlType::DateTimeNs => (TPrimitiveType::DATETIME, ...)`（保持 thrift primitive 为 DATETIME；单位由 Arrow/desc 携带）。
- `ddl.rs:1355`（`SqlType::DateTime => "DATETIME".to_string()`）→ 加 `SqlType::DateTimeNs => "TIMESTAMP_NS".to_string()`。
- `ddl.rs:1422`（`"DATETIME" => Ok(SqlType::DateTime)`）→ 加 `"TIMESTAMP_NS" | "DATETIME_NS" => Ok(SqlType::DateTimeNs)`。
- `mv_ddl.rs:1897`（`DataType::Timestamp(_, _) => Ok(SqlType::DateTime)`）→ 改为按单位区分：
  ```rust
  DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => Ok(SqlType::DateTimeNs),
  DataType::Timestamp(_, _) => Ok(SqlType::DateTime),
  ```

- [ ] **Step 6: 构建通过**

Run: `cargo build`
Expected: 编译通过，无 non-exhaustive 报错。

- [ ] **Step 7: Commit**

```bash
git add src/connector/
git commit -m "feat(iv3-7): handle SqlType::DateTimeNs across iceberg/ddl mappings"
```

---

### Task 6: parquet arrow→iceberg 写映射补纳秒

**Files:**
- Modify: `src/formats/parquet/mod.rs:1738`
- Test: `src/formats/parquet/mod.rs`（#[cfg(test)]）

- [ ] **Step 1: 写失败测试**

`src/formats/parquet/mod.rs` tests 加（函数名以实际为准，下设其为 `arrow_type_to_iceberg_type`）：
```rust
#[test]
fn nanosecond_timestamp_maps_to_iceberg_timestamp_ns() {
    use arrow::datatypes::{DataType, TimeUnit};
    let t = arrow_type_to_iceberg_type(&DataType::Timestamp(TimeUnit::Nanosecond, None)).unwrap();
    assert!(matches!(t, iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::TimestampNs)));
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test nanosecond_timestamp_maps_to_iceberg_timestamp_ns`
Expected: FAIL（无纳秒 arm）。

- [ ] **Step 3: 加纳秒 arm**

`src/formats/parquet/mod.rs` 在
```rust
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Type::Primitive(PrimitiveType::Timestamp)
        }
```
之后加：
```rust
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            Type::Primitive(PrimitiveType::Timestamptz)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            Type::Primitive(PrimitiveType::TimestampNs)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => {
            Type::Primitive(PrimitiveType::TimestamptzNs)
        }
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test nanosecond_timestamp_maps_to_iceberg_timestamp_ns`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/formats/parquet/mod.rs
git commit -m "feat(iv3-7): map nanosecond arrow timestamp to iceberg timestamp_ns in parquet writer"
```

---

### Task 7: INSERT 字面量保纳秒精度

**Files:**
- Modify: `src/engine/parquet.rs`（`parse_datetime_string_to_micros` :118 旁加 nanos 版）
- Modify: `src/engine/insert.rs:121,416-423`
- Test: `src/engine/parquet.rs`（#[cfg(test)]）

- [ ] **Step 1: 写失败测试（9 位小数解析为纳秒）**

`src/engine/parquet.rs` tests 加：
```rust
#[test]
fn parse_datetime_string_to_nanos_keeps_nanoseconds() {
    let nanos = parse_datetime_string_to_nanos("2024-01-02 03:04:05.123456789").unwrap();
    // 验证最后三位纳秒不被截断
    assert_eq!(nanos % 1_000, 789);
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test parse_datetime_string_to_nanos_keeps_nanoseconds`
Expected: FAIL（函数不存在）。

- [ ] **Step 3: 实现 `parse_datetime_string_to_nanos`**

`src/engine/parquet.rs` 在 `parse_datetime_string_to_micros` 旁加（镜像其格式解析，改用 `timestamp_nanos_opt`）：
```rust
/// Parse a `YYYY-MM-DD HH:MM:SS[.fffffffff]` literal into nanoseconds since the
/// Unix epoch. Mirrors `parse_datetime_string_to_micros` but keeps nanosecond
/// precision for Iceberg v3 `timestamp_ns` columns. Errors if the value is
/// outside the nanosecond-representable range (~1677-09-21 .. 2262-04-11).
pub(crate) fn parse_datetime_string_to_nanos(s: &str) -> Result<i64, String> {
    use chrono::{NaiveDate, NaiveDateTime};
    let text = s.trim();
    let dt = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S"))
        .or_else(|_| {
            NaiveDate::parse_from_str(text, "%Y-%m-%d")
                .and_then(|d| d.and_hms_opt(0, 0, 0).ok_or_else(|| {
                    // reuse chrono's ParseError shape via a re-parse failure
                    NaiveDateTime::parse_from_str("", "%Y-%m-%d %H:%M:%S").unwrap_err()
                }))
        })
        .map_err(|_| format!("invalid DATETIME literal '{s}'"))?;
    dt.and_utc()
        .timestamp_nanos_opt()
        .ok_or_else(|| format!("DATETIME literal '{s}' out of nanosecond representable range"))
}
```

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test parse_datetime_string_to_nanos_keeps_nanoseconds`
Expected: PASS

- [ ] **Step 5: INSERT 数组构建——加纳秒 arm**

`src/engine/insert.rs` 顶部 `use` 补 `parse_datetime_string_to_nanos` 与 `TimestampNanosecondArray`。
在 `:416` 的 `DataType::Timestamp(TimeUnit::Microsecond, _) => { Ok(Arc::new(TimestampMicrosecondArray::from(... parse_datetime_string_to_micros ...))) }` arm 之后加：
```rust
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Ok(Arc::new(TimestampNanosecondArray::from(
                values
                    .iter()
                    .map(|v| match v {
                        None => Ok(None),
                        Some(v) => parse_datetime_string_to_nanos(v).map(Some),
                    })
                    .collect::<Result<Vec<_>, String>>()?,
            )) as ArrayRef)
        }
```
（`values`/迭代形态以现有 micro arm 为准，纳秒 arm 镜像之，仅换数组类型与解析函数。）

- [ ] **Step 6: arrow→SqlType（insert.rs:121）补纳秒**

把 `DataType::Timestamp(TimeUnit::Microsecond, _) => SqlType::DateTime,` 改为：
```rust
        DataType::Timestamp(TimeUnit::Nanosecond, _) => SqlType::DateTimeNs,
        DataType::Timestamp(TimeUnit::Microsecond, _) => SqlType::DateTime,
```

- [ ] **Step 7: 构建**

Run: `cargo build`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add src/engine/parquet.rs src/engine/insert.rs
git commit -m "feat(iv3-7): preserve nanosecond precision in INSERT literal coercion"
```

---

### Task 8: CTAS arrow→SqlType 按单位区分

**Files:**
- Modify: `src/engine/iceberg_ctas.rs:331`

- [ ] **Step 1: 改 Timestamp arm 区分单位**

把 `DataType::Timestamp(_, _) => SqlType::DateTime,` 改为：
```rust
        DataType::Timestamp(TimeUnit::Nanosecond, _) => SqlType::DateTimeNs,
        DataType::Timestamp(_, _) => SqlType::DateTime,
```
（`iceberg_ctas.rs:510` 的测试断言 `cols[12].data_type == SqlType::DateTime` 不变——其源列为微秒。）

- [ ] **Step 2: 构建**

Run: `cargo build`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add src/engine/iceberg_ctas.rs
git commit -m "feat(iv3-7): discriminate nanosecond timestamp in CTAS type inference"
```

---

## Phase 3 — NovaRocks-only 端到端 ns 往返（核心正确性 gate，iceberg-rest）

### Task 9: 纳秒 CREATE / INSERT / SELECT 往返 sql-test

**Files:**
- Create: `sql-tests/iceberg-rest/sql/timestamp_ns_roundtrip.sql`
- Create: `sql-tests/iceberg-rest/result/timestamp_ns_roundtrip.result`

环境准备（每次本任务执行前）：
```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
# 用 dev-opt 构建获得快编译 + 接近 release 的查询速度
cargo build --profile dev-opt
```

- [ ] **Step 1: 写 fixture（CREATE ns 表 → INSERT 纳秒字面量 → CAST AS STRING 读回 9 位）**

`sql-tests/iceberg-rest/sql/timestamp_ns_roundtrip.sql`，参照 `sql-tests/iceberg-rest/` 既有用例的 catalog/命名约定（`${suite_uuid0}` / `${uuid0}` 隔离）。核心查询：
```sql
-- @sequential=true
-- @order_sensitive=true
-- IV3-7: NovaRocks creates a v3 timestamp_ns table, inserts nanosecond
-- literals, and reads them back without truncation.

-- query 1
CREATE TABLE ts_ns_${uuid0} (
  id BIGINT,
  c_ts_ns TIMESTAMP_NS
);

-- query 2
INSERT INTO ts_ns_${uuid0} VALUES
  (1, '2024-01-02 03:04:05.123456789'),
  (2, '2024-01-02 03:04:05.000000001'),
  (3, '1970-01-01 00:00:00.000000000');

-- query 3
-- 默认渲染走 MySQL wire（截到微秒），用 CAST AS STRING 验证纳秒
SELECT id, CAST(c_ts_ns AS STRING) AS s
FROM ts_ns_${uuid0}
ORDER BY id;

-- query 4
DROP TABLE ts_ns_${uuid0} FORCE;
```

- [ ] **Step 2: 录制前先跑，确认纳秒未被截断（人工判读）**

启动 standalone-server（按 CLAUDE.md readiness 标记 gating），再：
```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --only timestamp_ns_roundtrip --mode diff
```
Expected: query 3 的 `s` 列应显示到纳秒：`2024-01-02 03:04:05.123456789` / `...000000001` / `...000000000`。
若显示只到微秒（`...123456`），说明读路径或 INSERT 仍截断 → 用 `EXPLAIN` 看 scan 输出 schema、回查 Phase 1/Task 7；不要录制错误 golden。

- [ ] **Step 3: 录制 golden**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --only timestamp_ns_roundtrip \
  --mode record --record-from target
```
确认 `result/timestamp_ns_roundtrip.result` 内 `s` 列为 9 位小数。

- [ ] **Step 4: verify 通过**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --only timestamp_ns_roundtrip --mode verify
```
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add sql-tests/iceberg-rest/sql/timestamp_ns_roundtrip.sql sql-tests/iceberg-rest/result/timestamp_ns_roundtrip.result
git commit -m "test(iv3-7): NovaRocks-only nanosecond timestamp create/insert/read roundtrip"
```

---

## Phase 4 — 跨引擎 compat（iceberg-compatibility，Spark）

### Task 10: 跨引擎纳秒写读对称

**Files:**
- Create: `sql-tests/iceberg-compatibility/sql/spark_rest_minio_v3_timestamp_ns.sql`
- Create: `sql-tests/iceberg-compatibility/result/spark_rest_minio_v3_timestamp_ns.result`

环境同 Task 9，并确保 Spark 镜像可用（`novarocks/spark-iceberg:3.5.5_1.11.0`）。

- [ ] **Step 1: 先探测 Spark 能否用 SQL 直接建 timestamp_ns 表**

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
docker/iceberg-rest/spark-sql.sh "CREATE TABLE ice_rest.probe.ts_ns_probe (c TIMESTAMP_NS) USING iceberg TBLPROPERTIES ('format-version'='3'); DROP TABLE ice_rest.probe.ts_ns_probe;"
```
记录结果：Spark 3.5 + Iceberg 1.11 是否接受 `TIMESTAMP_NS` DDL（或需 Iceberg API / 配置）。

- [ ] **Step 2: 写 fixture——方向 A（必做，可靠）：NovaRocks 写、Spark 读**

镜像 `sql-tests/iceberg-compatibility/sql/spark_rest_minio_v3_primitive_types.sql` 的 shell+Spark 结构。流程：
1. NovaRocks `CREATE TABLE ... (c_ts_ns TIMESTAMP_NS)` 并 `INSERT` 纳秒值；
2. `shell:` 调 `docker/iceberg-rest/spark-sql.sh` 让 Spark `SELECT date_format(c_ts_ns, ...)` 或读出，断言 9 位纳秒；
3. `-- @result_contains=` 校验 Spark 输出含完整纳秒。

- [ ] **Step 3: 写 fixture——方向 B（条件性）：Spark 写、NovaRocks 读**

仅当 Step 1 探测 Spark 可建 ns 表时加入：Spark 建表+插值，NovaRocks `SELECT CAST(c_ts_ns AS STRING)` 断言 9 位。若 Spark 无法用 SQL 建 ns 表，在 fixture 顶部注释说明并跳过方向 B（方向 A + Task 9 的 NovaRocks 往返已覆盖读正确性与写读对称）。

- [ ] **Step 4: 录制 + verify**

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility --only spark_rest_minio_v3_timestamp_ns \
  --mode record --record-from target
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility --only spark_rest_minio_v3_timestamp_ns --mode verify
```
Expected: PASS（方向 A 必过；方向 B 视探测结果）。

- [ ] **Step 5: Commit**

```bash
git add sql-tests/iceberg-compatibility/sql/spark_rest_minio_v3_timestamp_ns.sql sql-tests/iceberg-compatibility/result/spark_rest_minio_v3_timestamp_ns.result
git commit -m "test(iv3-7): cross-engine nanosecond timestamp write/read compatibility"
```

---

## Phase 5 — 谓词下推纳秒正确性

### Task 11: MinMaxPredicateValue::DateTimeNanos + 纳秒边界

**Files:**
- Modify: `src/common/min_max_predicate.rs:25-42,109-190`
- Modify: `src/lower/expr/min_max.rs`（`extract_int_literal` :381、`extract_large_int_literal` :436、`extract_string_literal` :517、`extract_date_literal` :588、新增 `time_unit_from_node` 与 `parse_datetime_literal_nanos`）
- Test: `src/lower/expr/min_max.rs`（#[cfg(test)]）+ 谓词集成（sql-test，复用 Task 9 表）

- [ ] **Step 1: 加 `DateTimeNanos` 变体与 `as_i64` arm**

`src/common/min_max_predicate.rs` 在 `DateTimeMicros(i64),` 之后加：
```rust
    /// Nanosecond-precision DATETIME bound for Iceberg v3 `timestamp_ns`
    /// columns. Distinct from `DateTimeMicros` because parquet row-group
    /// statistics for nanosecond columns are i64 nanoseconds.
    DateTimeNanos(i64),
```
在 `as_i64` 的 `MinMaxPredicateValue::DateTimeMicros(v) => Some(*v),` 之后加：
```rust
            MinMaxPredicateValue::DateTimeNanos(v) => Some(*v),
```

- [ ] **Step 2: 写失败测试——纳秒列字面量产出 DateTimeNanos**

`src/lower/expr/min_max.rs` tests 加（构造一个 DATETIME + time_unit=nano 的 TExprNode，调用 `extract_string_literal`）：
```rust
#[test]
fn string_literal_on_nanosecond_column_produces_datetime_nanos() {
    let node = datetime_node_with_time_unit(Some(
        crate::lower::type_lowering::THRIFT_TIME_UNIT_NANOS,
    ));
    let v = extract_string_literal(&node, "2024-01-02 03:04:05.123456789").unwrap();
    assert_eq!(
        v,
        MinMaxPredicateValue::DateTimeNanos(
            chrono::NaiveDateTime::parse_from_str(
                "2024-01-02 03:04:05.123456789", "%Y-%m-%d %H:%M:%S%.f"
            ).unwrap().and_utc().timestamp_nanos_opt().unwrap()
        )
    );
}
```
（`datetime_node_with_time_unit` 为测试辅助，构造 `TExprNode { type_: TTypeDesc{ DATETIME, time_unit } , ... }`，按本文件既有测试构造 TExprNode 的方式写。）

- [ ] **Step 3: 跑测试确认失败**

Run: `cargo test string_literal_on_nanosecond_column_produces_datetime_nanos`
Expected: FAIL（当前产出 `DateTimeMicros`）。

- [ ] **Step 4: 加 `time_unit_from_node` 与 `parse_datetime_literal_nanos`**

`src/lower/expr/min_max.rs` 加：
```rust
/// Read the DATETIME time-unit code from an expression node's scalar type, if
/// present. `None` means microsecond (default).
fn time_unit_from_node(node: &exprs::TExprNode) -> Option<i32> {
    node.type_
        .types
        .as_ref()?
        .first()?
        .scalar_type
        .as_ref()?
        .time_unit
}

fn parse_datetime_literal_nanos(value: &str) -> Result<i64, String> {
    let text = value.trim();
    if text.is_empty() {
        return Err("empty DATETIME literal".to_string());
    }
    let dt = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S"))
        .or_else(|_| {
            NaiveDate::parse_from_str(text, "%Y-%m-%d")
                .map_err(|_| ())
                .and_then(|d| d.and_hms_opt(0, 0, 0).ok_or(()))
                .map_err(|_| ())
        })
        .map_err(|_| format!("invalid DATETIME literal '{}'", value))?;
    dt.and_utc()
        .timestamp_nanos_opt()
        .ok_or_else(|| format!("DATETIME literal '{}' out of nanosecond range", value))
}
```
（若上面的 `or_else` 链类型不齐，按本文件 `parse_datetime_literal_micros` 同样的逐个 `if let Ok(...)` 风格改写——目标是支持 `%.f`/无小数/纯日期三种，最终用 `timestamp_nanos_opt()`。）

- [ ] **Step 5: extract_*_literal 纳秒分支**

在 `extract_string_literal`、`extract_date_literal` 的 `DATETIME || TIME` arm 内，先判 `time_unit_from_node(node) == Some(THRIFT_TIME_UNIT_NANOS)`：是则产 `DateTimeNanos(parse_datetime_literal_nanos(value)?)`，否则维持 `DateTimeMicros(parse_datetime_literal_micros(value)?)`。
在 `extract_int_literal`、`extract_large_int_literal` 的 `DATETIME || TIME` arm 内同理：纳秒列直接产 `DateTimeNanos(value)`（int 值按列单位即纳秒 ticks），否则 `DateTimeMicros(value)`。
需要 `use crate::lower::type_lowering::THRIFT_TIME_UNIT_NANOS;`。

- [ ] **Step 6: 跑单测确认通过**

Run: `cargo test string_literal_on_nanosecond_column_produces_datetime_nanos`
Expected: PASS

- [ ] **Step 7: 谓词裁剪集成测试（行数正确）**

在 `sql-tests/iceberg-rest/sql/timestamp_ns_roundtrip.sql` 追加范围谓词查询（构造"按微秒舍入会裁错"的边界），并录制 golden：
```sql
-- query 5
-- 两行同微秒不同纳秒：> 边界须按纳秒判定
SELECT COUNT(*) FROM ts_ns_${uuid0}
WHERE c_ts_ns > CAST('2024-01-02 03:04:05.000000001' AS TIMESTAMP_NS);
```
重录该 fixture golden 并 verify（命令同 Task 9 Step 3/4）。
Expected: 计数符合纳秒边界语义（不因微秒舍入多/漏行）。

- [ ] **Step 8: Commit**

```bash
git add src/common/min_max_predicate.rs src/lower/expr/min_max.rs sql-tests/iceberg-rest/
git commit -m "feat(iv3-7): nanosecond-aware min/max predicate pushdown"
```

---

## Phase 6 — cast 微秒↔纳秒语义（截断 + 溢出报错）

### Task 12: 纳秒 cast 单测与实现

**Files:**
- Modify: `src/exec/expr/cast.rs`（`cast_numeric_to_timestamp_array` :488、`cast_utf8_to_timestamp_array` :801；timestamp→timestamp 路径）
- Test: `src/exec/expr/cast.rs`（#[cfg(test)]）+ sql-test

- [ ] **Step 1: 写失败测试——三条规则**

`src/exec/expr/cast.rs` tests 加：
```rust
#[test]
fn cast_nanos_to_micros_truncates() {
    use arrow::array::TimestampNanosecondArray;
    use arrow::datatypes::{DataType, TimeUnit};
    let src = Arc::new(TimestampNanosecondArray::from(vec![Some(1_000_000_789i64)])) as ArrayRef;
    let out = cast_value(&src, &DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap();
    let arr = out.as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>().unwrap();
    assert_eq!(arr.value(0), 1_000); // 1_000_000_789 ns -> 1_000 us (截断)
}

#[test]
fn cast_micros_to_nanos_widens_in_range() {
    use arrow::array::TimestampMicrosecondArray;
    use arrow::datatypes::{DataType, TimeUnit};
    let src = Arc::new(TimestampMicrosecondArray::from(vec![Some(1_000i64)])) as ArrayRef;
    let out = cast_value(&src, &DataType::Timestamp(TimeUnit::Nanosecond, None)).unwrap();
    let arr = out.as_any().downcast_ref::<arrow::array::TimestampNanosecondArray>().unwrap();
    assert_eq!(arr.value(0), 1_000_000);
}

#[test]
fn cast_micros_to_nanos_overflow_errors() {
    use arrow::array::TimestampMicrosecondArray;
    use arrow::datatypes::{DataType, TimeUnit};
    // 远超纳秒可表示范围（> ~2262 年）
    let src = Arc::new(TimestampMicrosecondArray::from(vec![Some(i64::MAX / 2)])) as ArrayRef;
    let err = cast_value(&src, &DataType::Timestamp(TimeUnit::Nanosecond, None));
    assert!(err.is_err());
}
```
（`cast_value` 用本文件实际的 cast 入口函数名替换。）

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test cast_nanos_to_micros_truncates cast_micros_to_nanos_widens_in_range cast_micros_to_nanos_overflow_errors`
Expected: 至少 overflow 用例 FAIL（Arrow 默认 cast 对溢出可能产 null 而非 err）；truncate/widen 视现状。

- [ ] **Step 3: 实现 timestamp→timestamp 单位 cast 规则**

在 cast dispatch 增加显式 `(DataType::Timestamp(su, _), DataType::Timestamp(tu, ttz))` arm：
- `su == tu`：必要时仅调整 tz，值不变。
- 纳秒→微秒（收窄）：用 Arrow `cast`（向下截断）即可。
- 微秒→纳秒（拓宽）：先检查每个非空值 `v` 满足 `v.checked_mul(1000).is_some()` 且落在 i64 纳秒范围，否则返回 `Err("CAST timestamp microsecond->nanosecond overflow: ...")`；通过后 `cast` 到纳秒。
同时让 `cast_numeric_to_timestamp_array`、`cast_utf8_to_timestamp_array` 在 `target_type` 为纳秒时不强制走中间 `TimestampMicrosecondArray`：直接构建纳秒数组（用 `timestamp_nanos_opt()`），避免拓宽前丢精度。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test cast_nanos_to_micros_truncates cast_micros_to_nanos_widens_in_range cast_micros_to_nanos_overflow_errors`
Expected: PASS

- [ ] **Step 5: sql-test 覆盖（CAST AS DATETIME 截断）**

在 `sql-tests/iceberg-rest/sql/timestamp_ns_roundtrip.sql` 追加：
```sql
-- query 6
SELECT id, CAST(CAST(c_ts_ns AS DATETIME) AS STRING) AS micros
FROM ts_ns_${uuid0} ORDER BY id;
```
重录 golden 并 verify：`micros` 列应为 6 位（纳秒被显式 CAST 截断到微秒）。

- [ ] **Step 6: Commit**

```bash
git add src/exec/expr/cast.rs sql-tests/iceberg-rest/
git commit -m "feat(iv3-7): nanosecond<->microsecond cast (truncate narrowing, error on widen overflow)"
```

---

## Phase 7 — 分区 transform 暂 fail-fast

### Task 13: 纳秒列分区 transform 显式报错

**Files:**
- Modify: Iceberg 分区构建路径（`grep -rn "PartitionSpec\|partition.*transform\|build_partition" src/connector/iceberg/ src/engine/` 定位 CREATE/写时把列+transform 组装成 Iceberg partition field 的函数）
- Test: sql-test（`@expect_error`）

- [ ] **Step 1: 定位分区 transform 组装点**

Run: `grep -rn "Transform\|partition_spec\|PartitionField" src/connector/iceberg/ | grep -iv test`
找到把 source 列 + transform 生成 partition field 的位置（CREATE TABLE ... PARTITIONED BY 路径）。

- [ ] **Step 2: 写失败测试（纳秒列 + transform 应报错）**

`sql-tests/iceberg-rest/sql/timestamp_ns_roundtrip.sql` 追加：
```sql
-- query 7
-- @expect_error=nanosecond
CREATE TABLE ts_ns_part_${uuid0} (id BIGINT, c_ts_ns TIMESTAMP_NS)
PARTITIONED BY (day(c_ts_ns));
```

- [ ] **Step 3: 实现 fail-fast**

在分区组装点：当 source 列 Arrow 类型为 `Timestamp(TimeUnit::Nanosecond, _)` 且 transform 为时间类（year/month/day/hour）时，返回
`Err("partition transform on nanosecond timestamp column is not supported yet (IV3-7.1)")`。

- [ ] **Step 4: verify**

重录/verify 该 fixture（`@expect_error` 用例）。
Expected: PASS（CREATE 被显式拒绝，错误信息含 "nanosecond"）。

- [ ] **Step 5: Commit**

```bash
git add src/connector/iceberg/ sql-tests/iceberg-rest/
git commit -m "feat(iv3-7): fail-fast on partition transform over nanosecond timestamp columns"
```

---

## Phase 8 — 回归与文档

### Task 14: 套件回归 + roadmap 状态更新

- [ ] **Step 1: lib 单测全绿**

Run: `cargo test`
Expected: PASS（含本计划新增单测；无既有回归）。

- [ ] **Step 2: Iceberg 套件回归**

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
for s in iceberg iceberg-rest iceberg-compatibility; do
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite "$s" --mode verify || break
done
```
Expected: 三套件 PASS（默认 DATETIME 行为不变、FE-compat 不受影响）。

- [ ] **Step 3: 更新 roadmap 任务状态**

把 `NovaRocks TODO/NovaRocks Roadmap.md` 中 IV3-7 行与 `IV3-7-nanosecond-timestamp-types.md` 状态从"未实现"更新为已落地（注明范围：standalone Iceberg 读/写/原生 CREATE timestamp_ns；timestamptz_ns 原生 CREATE 与分区 transform 留 IV3-7.1）。

- [ ] **Step 4: Commit**

```bash
git add docs/
git commit -m "docs(iv3-7): mark nanosecond timestamp support landed; note IV3-7.1 follow-ups"
```
（roadmap 在仓库外，按用户习惯单独更新，不纳入本仓库 commit。）

---

## Self-Review（计划对 spec 覆盖核对）

- **读纳秒不截断** → Phase 1（往返保单位）+ Task 9（端到端验证）。✓
- **谓词下推正确裁剪** → Task 11（DateTimeNanos + 纳秒边界 + 行数测试）。✓
- **cast 截断 + 溢出报错** → Task 12。✓
- **写入 ns 表** → Task 6/7（parquet 映射 + INSERT 字面量）+ Task 9（往返）。✓
- **原生 CREATE timestamp_ns** → Task 4/5（SqlType + 解析 + iceberg 映射）。✓
- **timestamptz_ns 原生 CREATE out-of-scope** → Task 4 注释 + 仅 `timestamp_ns` 走原生映射；tz 走 Arrow（读/写入已存在表）。✓
- **分区 transform fail-fast** → Task 13。✓
- **MySQL wire 截微秒、纳秒验证走 CAST AS STRING** → Task 9/10 用 `CAST(... AS STRING)`。✓
- **FE-compat / 默认 DATETIME 不变** → Task 1（time_unit 缺省=微秒）+ Task 3（断言）+ Task 14 Step 2。✓
- **跨引擎写读对称** → Task 10。✓

**类型一致性核对：** `THRIFT_TIME_UNIT_NANOS`（type_lowering）在 type_infer / min_max 统一引用；`SqlType::DateTimeNs`、`MinMaxPredicateValue::DateTimeNanos`、`parse_datetime_string_to_nanos`、`parse_datetime_literal_nanos`、`thrift_time_unit_for_arrow` 命名跨任务一致。

**已知占位风险（实现期按 TDD 收口）：** Task 5 Step 4（schema_update 演进矩阵的精确 arm 位置）、Task 11 Step 4（`parse_datetime_literal_nanos` 的 or_else 链类型对齐）、Task 13 Step 1（分区组装点定位）需在实现时按 grep/编译报错精确定位；均给出了定位命令与目标形态。
