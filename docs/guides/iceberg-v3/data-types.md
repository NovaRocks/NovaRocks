# 数据类型

> Iceberg spec 把类型分为原始 / 嵌套 / V3 新增三组。本页给出每个类型 NovaRocks 是否可用，以及 SQL 语法对应。
>
> 字面量解析、cast、比较、运算等行为对齐 Iceberg type promotion 规则；具体 widening 见 [Schema 演进](schema-evolution.md)。

---

## 原始类型

| Iceberg type | SQL 语法 | 状态 | 备注 |
| --- | --- | --- | --- |
| `boolean` | `BOOLEAN` | ✅ | |
| `int` | `INT` / `INTEGER` | ✅ | |
| `long` | `BIGINT` | ✅ | |
| `float` | `FLOAT` | ✅ | |
| `double` | `DOUBLE` | ✅ | |
| `date` | `DATE` | ✅ | |
| `time` | `TIME` | ✅ | |
| `timestamp` | `TIMESTAMP`（毫秒/微秒） | ✅ | |
| `timestamptz` | `TIMESTAMP WITH TIME ZONE` | ✅ | |
| `string` | `STRING` / `VARCHAR` | ✅ | |
| `binary` | `BINARY` / `VARBINARY` | ✅ | |
| `fixed[N]` | `FIXED(N)` | ✅ | |
| `decimal(P,S)` | `DECIMAL(P,S)` | ✅ | 含写入 + 比较 + cast |
| `uuid` | `UUID` | ✅ | 基础支持 |
| `timestamp_ns` (V3) | — | ❌ | TODO |
| `timestamptz_ns` (V3) | — | ❌ | TODO |
| `unknown` (V3) | — | ❌ | TODO |

### ❌ V3 纳秒时间戳（`timestamp_ns` / `timestamptz_ns`）

Spec：V3 引入纳秒精度的 timestamp / timestamptz，物理表示从 INT64-micros 升级为 INT64-nanos。

**TODO**：未实现。

- 读端：parquet `INT64` + `LogicalType: TimestampType(isAdjustedToUTC, NANOS)` 不会被识别为 timestamp_ns，会按字面 INT64 处理。
- 写端：DDL 不接受纳秒精度的 timestamp 关键字。

### ❌ Unknown 类型（V3 placeholder）

Spec：V3 引入 `unknown` 类型作为字段值未确定时的占位（schema 演化中"已声明但暂无数据"场景）。

**TODO**：未实现。

---

## 嵌套类型

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| `struct<...>`（含嵌套） | ✅ | |
| `list<T>` | ✅ | |
| `map<K, V>` | ✅ | |
| 复合类型 schema evolution（按 field-id 对齐） | ✅ | |

```sql
CREATE TABLE t (
  id BIGINT,
  profile STRUCT<
    name: STRING,
    tags: ARRAY<STRING>,
    attrs: MAP<STRING, STRING>
  >
);
```

---

## V3 新增类型

### ✅ Variant（读）

详见 [variant.md](variant.md)。读路径完整支持，**写路径 IcebergSink 仍会拒绝 variant 列**。

### ❌ Variant（写）

见 [variant.md](variant.md)。spec 要求把 metadata + value 两个 binary stream 写到 parquet；NovaRocks 写端尚未实现。

### ❌ Geometry / Geography（V3 空间类型）

Spec：V3 引入 geometry / geography 类型，物理用 parquet `geometry` 物理类型 + 元数据描述 CRS / edge interpolation。SQL 侧期待 `ST_*` 函数族。

**TODO**：未实现。

- 读端：parquet `geometry` 物理类型解码缺失
- 写端：sink 侧 WKB / EWKB 序列化缺失
- SQL：未提供 `ST_GeomFromText` / `ST_Within` / `ST_Distance` 等函数族

### ✅ Default Value（`initial-default` / `write-default`）

详见 [default-values.md](default-values.md)。CREATE TABLE / ALTER ADD COLUMN 都接受 `DEFAULT <literal>`，读端按 `initial-default` backfill，INSERT 显式列表形式按 `write-default` 自动补列。
