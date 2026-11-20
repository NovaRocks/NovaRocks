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
| `uuid` | — | 读支持 / 写不支持 | 外部表列可读；SQL 不提供 `UUID` 类型关键字 |
| `timestamp_ns` (V3) | — | ❌ | TODO |
| `timestamptz_ns` (V3) | — | ❌ | TODO |
| `unknown` (V3) | — | ❌ | TODO |

### uuid 与写出目标类型

uuid 列可从外部 Iceberg 表读取。NovaRocks 不提供 SQL `UUID` 类型关键字；不能把它当成普通 FIXED/BINARY 后写回。目标表任意层级含 uuid 时，INSERT、OVERWRITE、UPDATE、MERGE 与 OPTIMIZE 数据重写拒绝。uuid 等值键的 ADD EQUALITY DELETE 拒绝；普通键的 equality delete、DV/position DELETE、TRUNCATE 与 uuid 表 ADD FILES 不因 uuid 被全表禁止。

恢复 uuid 写入需要正式保留 UUID 逻辑类型和 Parquet UUID 标注的 writer，并完成独立引擎读取验证，不能只输出 16 字节 FIXED 载体。

### timestamptz 写出

Provider 冻结的 Iceberg 目标 schema 保留 timestamp 与 timestamptz 的区别。写出 SDK Arrow schema 对 timestamptz / timestamptz_ns 使用 `+00:00`；输入适配仅附加 UTC 标签，不改变整数时间值，因此 Parquet 标为 `isAdjustedToUTC=true`。执行载体上没有 timezone 标签不代表目标是不带时区的 timestamp。timestamp / timestamp_ns 保持 `isAdjustedToUTC=false`。

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
  profile ROW(
    name VARCHAR,
    tags ARRAY(VARCHAR),
    attrs MAP(VARCHAR, VARCHAR)
  )
);
```

---

## V3 新增类型

### ✅ Variant（读）

详见 [variant.md](variant.md)。支持外部表的顶层 Variant（含 shredded group）；嵌套 Variant 与其他引擎写入的 Variant manifest bounds 暂不支持。

### ❌ Variant（写）

见 [variant.md](variant.md)。CREATE / ALTER ADD 在所有 format-version 拒绝任意层级的新 Variant 列，CREATE LIKE / CTAS / MV 目标同样拒绝。含 Variant 列的数据写出在 Provider 准入时拒绝。Variant 表使用 v3，[ADD FILES](dml.md#登记已有-parquet-文件) 的规划准入目标为 v1/v2；已有表的普通 metadata 修改、DV DELETE、TRUNCATE 与非 Variant 等值键的 equality delete 保留原有路径。

### ❌ Geometry / Geography（V3 空间类型）

Spec：V3 引入 geometry / geography 类型，物理用 parquet `geometry` 物理类型 + 元数据描述 CRS / edge interpolation。SQL 侧期待 `ST_*` 函数族。

**TODO**：未实现。

- 读端：parquet `geometry` 物理类型解码缺失
- 写端：sink 侧 WKB / EWKB 序列化缺失
- SQL：未提供 `ST_GeomFromText` / `ST_Within` / `ST_Distance` 等函数族

### ✅ Default Value（`initial-default` / `write-default`）

详见 [default-values.md](default-values.md)。CREATE TABLE / ALTER ADD COLUMN 都接受 `DEFAULT <literal>`，读端按 `initial-default` backfill，INSERT 显式列表形式按 `write-default` 自动补列。
