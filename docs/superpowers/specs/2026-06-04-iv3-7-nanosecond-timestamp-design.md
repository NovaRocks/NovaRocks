# IV3-7 · 纳秒精度时间类型（timestamp_ns / timestamptz_ns）设计

- 日期：2026-06-04
- Roadmap：Iceberg v3 Spec Support Roadmap，任务 IV3-7
- 任务文档：`NovaRocks TODO/IV3-7-nanosecond-timestamp-types.md`
- 状态：设计已评审通过，待出实现计划

## 1. 背景与问题

Iceberg v3 引入纳秒精度时间类型 `timestamp_ns` / `timestamptz_ns`。NovaRocks 当前
执行层把时间统一当作 Arrow `Timestamp(Microsecond, *)`，读 v3 纳秒列会被截断到微秒。

vendor `iceberg-0.9.0/src/arrow/schema.rs` 已能把 Iceberg `timestamp_ns` /
`timestamptz_ns` 正确转成 Arrow `Timestamp(Nanosecond, *)`，读侧来源是对的；截断发生
在 NovaRocks 内部类型管线。

### 1.1 架构事实（盘点结论）

- 执行层**直接使用 Arrow `RecordBatch` / `DataType`**，没有 NovaRocks wrapper 类型枚举。
  Arrow 本身就能表达 `Timestamp(Nanosecond, *)`。
- standalone catalog 的列类型用 Arrow `DataType` 作权威类型：`ColumnDef.data_type:
  DataType`（`src/sql/catalog.rs`）。`logical_type: Option<SqlType>` 只是 JSON/BITMAP/HLL
  这类 Arrow 无法区分时的旁路；注释明确"None 表示 Arrow type 即权威类型"。
- Iceberg 表的 `ColumnDef.data_type` 来自 vendor `schema_to_arrow_schema` 的输出
  （`src/connector/iceberg/catalog/registry.rs:767-813`），因此**纳秒列在 catalog 里本来
  就是 `Timestamp(Nanosecond)`**。
- 两套逻辑类型枚举都是 unitless：standalone 的 `SqlType::DateTime`
  （`src/sql/parser/ast/mod.rs`）与 thrift 的 `TPrimitiveType::DATETIME`
  （`idl/thrift/Types.thrift`）——均不携带精度。

### 1.2 读路径截断根因

即使在 standalone 模式，列类型也会经 codegen↔lowering 的 **unitless thrift `TTypeDesc`
往返**，纳秒在该往返中丢失：

```
catalog ColumnDef.data_type = Timestamp(Nanosecond)            ✓ vendor 正确
  ↓ codegen arrow_type_to_primitive → TPrimitiveType::DATETIME  ✗ 单位丢失 (type_infer.rs:151)
  ↓ thrift TScalarType{DATETIME}                                （无单位字段）
  ↓ lower type_lowering.rs:130 → Timestamp(Microsecond)         ✗ 单位被臆造成微秒
  ↓ hdfs_scan.rs:611 arrow_type = 微秒 → build_projected_output_schema 目标=微秒
  ↓ align_batch_to_iceberg_schema 把读到的纳秒数组 cast 成微秒    ✗ 数据截断（执行点）
```

关键源点：

- `src/sql/codegen/type_infer.rs:151`：`DataType::Timestamp(_, _) => TPrimitiveType::DATETIME`
  （micro 与 nano 都塌缩成 unitless DATETIME）。
- `src/lower/type_lowering.rs:130-133`：`DATETIME → Timestamp(Microsecond, None)`、
  `TIME → Time64(Microsecond)`（臆造微秒）。
- `src/lower/node/hdfs_scan.rs:611,1166-1170`：scan 输出 schema 从 lowered slot 描述符
  构造（微秒），`align_batch_to_iceberg_schema` 据此把纳秒数组 cast 成微秒。

### 1.3 已经能工作、无需改的部分

- vendor `arrow/schema.rs`：Iceberg ns 类型 → Arrow `Timestamp(Nanosecond, *)`。
- catalog `ColumnDef.data_type`：纳秒列已是 `Timestamp(Nanosecond)`。
- Iceberg 写 sink `arrow_type_to_iceberg_primitive_type`（`src/connector/iceberg/sink.rs:830-835`）：
  `Timestamp(Nanosecond, None) → TimestampNs`、`Timestamp(Nanosecond, Some(_)) → TimestamptzNs`。
- exchange 层用 Arrow IPC（schema 随数据走，纳秒存活线协议），且
  `src/runtime/exchange.rs:106-160` 的 desc-vs-actual 调和器对
  `(Timestamp, Timestamp)` 采纳 `actual.clone()`（信数组不信 desc）。
- Arrow compute kernel（比较 / cast）unit-agnostic；`format_timestamp`（`src/common/util.rs`）、
  `timestamp_to_naive`、`from_unixtime` 已正确处理纳秒。

## 2. 范围

### 2.1 In-scope（standalone Iceberg 模式）

- 读 `timestamp_ns` / `timestamptz_ns` 列保持纳秒精度，不截断。
- 纳秒列上的比较、范围谓词下推正确裁剪。
- 纳秒 ↔ 微秒 cast：收窄截断、溢出（落在纳秒可表示范围外）报错。
- 写入已声明为 ns 的表（INSERT / INSERT OVERWRITE）产出正确纳秒。
- 原生 DDL `CREATE TABLE (c timestamp_ns)`（ntz 纳秒）。

### 2.2 Out-of-scope（明确不做）

- FE-compat thrift 路径纳秒。FE 无纳秒类型，下发 `DATETIME`（微秒）即契约正确；
  非negotiable 规则要求"严格遵循 FE 类型元数据"，FE 路径维持微秒。
- StarRocks 原生格式纳秒（恒微秒）。
- 原生 CREATE `timestamptz_ns`（带时区）。NovaRocks 原生 DDL 当前连 micro 的
  `timestamptz` 都不支持（parser 把 `timestamp`/`datetime` 都映射成无 tz 的 `DateTime`，
  tz 仅在 Arrow 层表达），tz 建模是先于本任务的正交缺口。`timestamptz_ns` 的**读**与
  **写入已存在表**靠 Arrow 携带 `Some(UTC)` 支持，**不**新增原生 CREATE tz 类型。
  （与 IV3-7 非目标"不做时区相关无关改动"一致。）
- MySQL wire 显示 >6 位小数（协议小数秒上限为微秒）。纳秒值内部与写回 Iceberg
  完整保留；纳秒精度验证走 `CAST(ts AS STRING)`（9 位渲染）或跨引擎 Spark 回读。
- 改默认 DATETIME 精度（仍微秒）、时区库升级。
- 分区 transform（hour/day/month/year 等）对纳秒列：**暂 fail-fast**，不在本任务做完整
  纳秒 tick base 支持（见 §6.4）。

## 3. 选定方案（方案 A：描述符端到端携带 TimeUnit）

让类型表示端到端携带时间单位：thrift `TScalarType` 对 DATETIME 携带单位，codegen 编码、
lowering 解码，并扩展 `SqlType` 表达纳秒供 DDL。一处机制贯通
DDL → SqlType → Arrow → thrift desc → lower → scan → exec → write，读/写/DDL 全部一致正确。

### 3.1 为什么是 A（评审记录）

- 用户选定最大范围（读 + 写 + 原生 ns DDL）。A 的端到端统一让三条路径都不需要各自的
  "从权威类型取 schema"特殊管线。
- 代码库"无历史用户、直接改格式"的原则允许直接把内部描述符改成携带单位（属于"把格式
  改对"，非兼容 shim）。
- FE-compat 字节一致：FE 永不设新单位字段 → 默认微秒，符合"不改默认 DATETIME"。

### 3.2 否决的备选

- **方案 B（保 Arrow 权威类型、scan/sink schema 逐点取 catalog，desc 保持 unitless）**：
  更外科，且与现状一致（exchange 已"信数组不信 desc"）。但写 / MV / scan 每个"造 schema"
  的点都要逐一改成从权威类型取，存在 whack-a-mole；最大范围下不如 A 统一。
- **方案 C（用 logical_type 旁路表 / Field metadata 携带纳秒）**：Arrow 本就能原生表达
  `Timestamp(Nanosecond)`，问题只在 thrift 往返丢失，旁路表是绕路。

## 4. 类型系统改动

| 层 | 文件 / 位置 | 改动 |
|---|---|---|
| thrift IDL | `idl/thrift/Types.thrift` `TScalarType` | 新增 `5: optional i32 time_unit`，仅 DATETIME 使用；编码 micro / nano。FE 不设 → `None` → micro 默认。optional 字段 wire-safe。 |
| Arrow→primitive/desc | `src/sql/codegen/type_infer.rs`（`arrow_type_to_primitive` :151、`append_arrow_type_nodes`、`scalar_type_desc`） | `Timestamp(unit, _)` 把 `unit` 编码进 `TScalarType.time_unit`，不再无条件塌缩；微秒可不设（默认）。 |
| desc→Arrow | `src/lower/type_lowering.rs`（`arrow_type_from_primitive` :70、`arrow_type_from_nodes` :130-133） | 按 `time_unit` 还原 `Timestamp(Nanosecond/Microsecond, tz)`；`time_unit` 缺省 → 微秒。 |
| SQL 逻辑类型 | `src/sql/parser/ast/mod.rs` `enum SqlType` | 新增变体 `DateTimeNs`（不参数化已有 `DateTime`，churn 最小且天然满足"不改默认 DATETIME"）。 |
| 解析 | `src/sql/parser/dialect/mod.rs:223`、`create_table.rs` | `timestamp_ns` → `DateTimeNs`（ntz）。`timestamptz_ns` 读侧由 Arrow 处理；原生 CREATE tz out-of-scope。 |
| SqlType→Arrow | `src/engine/sql_expr.rs`（`sql_type_to_arrow_type` :1225） | `DateTimeNs → Timestamp(Nanosecond, None)`。 |
| DDL→Iceberg 类型 | `src/connector/iceberg/catalog/registry.rs` / `schema_update.rs` / `default_value.rs` 中 `SqlType` 映射处 | `DateTimeNs` → Iceberg `timestamp_ns`。 |

`time_unit` 编码约定：用 Arrow `TimeUnit` 的稳定整数（建议 `Second=0/Milli=1/Micro=2/Nano=3`），
但本任务只产出/消费 micro 与 nano；其余值遇到时 fail-fast。

## 5. 数据流（改动后）

### 5.1 读路径

```
Iceberg timestamp_ns → vendor Arrow Timestamp(Nanosecond) → catalog ColumnDef(纳秒)   ✓
  → codegen type_infer 编码 time_unit=nano → thrift desc 携带                          ✓（本任务）
  → lower type_lowering 还原 Timestamp(Nanosecond)                                      ✓（本任务）
  → hdfs_scan.rs arrow_type = 纳秒 → build_projected_output_schema 目标 = 纳秒
  → align_batch_to_iceberg_schema 纳秒→纳秒 no-op，无截断                                ✓
  → exchange（Arrow IPC + actual.clone() 调和）/ result buffer 透传纳秒                  ✓
  → MySQL 编码按协议截到微秒（值内部仍纳秒；纳秒验证走 CAST AS STRING / 跨引擎）
```

### 5.2 写 / DDL 路径

```
CREATE TABLE (c timestamp_ns) → SqlType::DateTimeNs → Arrow Timestamp(Nanosecond,None)
  → Iceberg schema timestamp_ns（DDL→Iceberg 映射补 ns）
INSERT / OVERWRITE → 投影 schema 经携带单位的 desc 还原为纳秒（不在写前下转）
  → sink arrow_type_to_iceberg_primitive_type 已映射 Timestamp(Nanosecond)→TimestampNs   ✓
```

### 5.3 谓词下推

```
WHERE ts_ns > TIMESTAMP '…' → 按目标列单位（纳秒）构造 min/max 边界 → parquet/Iceberg 裁剪
```

## 6. 组件详细设计

### 6.1 谓词下推（纳秒正确性）

- `src/common/min_max_predicate.rs`：`MinMaxPredicateValue` 新增 `DateTimeNanos(i64)`
  变体（评审选定"新增变体"而非访问器换算，更显式）。访问器 / 比较按列单位选择
  micro 或 nano 边界。
- `src/lower/expr/min_max.rs`（`extract_*_literal`、`parse_datetime_literal_micros` :625）：
  按目标列单位产出边界。纳秒列产出 `DateTimeNanos`（纳秒 ticks），避免微秒边界对纳秒
  数据裁错。

### 6.2 cast 语义（截断 + 溢出报错）

- `src/exec/expr/cast.rs`：
  - 纳秒目标不再强制走中间 `TimestampMicrosecondArray`（避免拓宽前先丢精度）。
  - nano → micro 收窄：按 Arrow 默认向下截断（显式 CAST，属预期 SQL 行为，非 silent
    downgrade）。
  - micro → nano 拓宽：值落在纳秒 i64 可表示范围（约 1677-09-21 ~ 2262-04-11）内则无损
    `×1000`；超出范围 fail-fast 报错（不饱和、不静默）。
- 比较：Arrow kernel unit-agnostic；确保字面量按列单位解析后再比较。

### 6.3 datetime 标量函数

- 函数返回类型推断（`src/sql/analyzer/functions.rs`）对 `now/current_timestamp` 等保持
  微秒返回，不变；只有来自 ns 列的数据流携带纳秒。
- 已 unit-aware 的函数（`format_timestamp`、`from_unixtime`、`timestamp_to_naive`）无需改。
- 对纳秒列做 tick 算术且仍硬编码微秒除/乘因子的函数：本任务以"读 / 写 / 谓词 / cast"
  正确性为主；逐个 datetime 函数的纳秒语义在实现期按测试暴露补齐，未覆盖到的形态
  fail-fast，不静默降级。

### 6.4 分区 transform（暂 fail-fast）

- 纳秒列参与 Iceberg 分区 transform（hour/day/month/year/bucket 等）时，本任务**不**实现
  完整纳秒 tick base 推导；遇到纳秒列分区 transform 显式报错，留作后续子任务
  （如 `IV3-7.1`）。fail-fast 错误信息需指明"纳秒列分区 transform 暂不支持"。

## 7. 错误处理与契约

- 所有"不支持 / 语义不明"形态显式报错，不隐式降级、不 best-effort（IV3 设计原则）。
- `time_unit` 出现非 micro/nano 值：fail-fast。
- cast 拓宽溢出：fail-fast。
- 纳秒列分区 transform：fail-fast（§6.4）。
- FE-compat 路径：FE 不设 `time_unit`，行为与改动前字节一致（需断言）。

## 8. 测试策略

写读对称、跨引擎可互操作（IV3 设计原则）。

- **读（跨引擎）**：`iceberg-compatibility` 加 Spark 写的 `timestamp_ns` /
  `timestamptz_ns` fixture；`SELECT CAST(ts_ns AS STRING)` 断言 9 位小数不截断。
- **谓词**：纳秒列范围谓词行数断言；构造"按微秒舍入会裁错"的纳秒边界用例。
- **cast**：nano↔micro 截断、micro→nano 溢出报错单测（`cast.rs`）+ sql-test 用例。
- **写读对称（跨引擎）**：NovaRocks INSERT 纳秒 → Spark 回读断言纳秒保留。
- **原生 DDL**：`CREATE … timestamp_ns` 后经 Spark / Iceberg metadata 验证 schema 为
  `timestamp_ns`。
- **lib 单测**：`type_infer` ↔ `type_lowering` 往返保单位；`min_max` 纳秒边界。
- **回归**：FE-compat DATETIME 仍微秒、字节一致；默认 DATETIME 行为不变。
- 套件：`iceberg`、`iceberg-compatibility`、`iceberg-rest`。

## 9. 风险

- 纳秒 i64 范围约 1677-09-21 ~ 2262-04-11；拓宽溢出 → 报错（已定）。
- 需审计 `arrow_type_from_*` / `arrow_type_to_primitive` 的全部消费方；slot 布局不变
  （micro / nano 同为 i64，8 字节，尺寸一致）。
- thrift 字段新增需确认 FE-compat 通路字节一致（FE 永不设 `time_unit`）。
- exchange 调和器已对 `(Timestamp, Timestamp)` 采纳 actual，需确认改动后 desc 与数组单位
  一致、不再依赖该调和兜底纠正。

## 10. 关键代码入口（实现起点）

- 类型往返：`src/sql/codegen/type_infer.rs`、`src/lower/type_lowering.rs`、
  `idl/thrift/Types.thrift`
- 逻辑类型 / DDL：`src/sql/parser/ast/mod.rs`、`src/sql/parser/dialect/{mod,create_table}.rs`、
  `src/engine/sql_expr.rs`、`src/connector/iceberg/catalog/{registry,schema_update}.rs`、
  `src/connector/iceberg/default_value.rs`
- 读 scan：`src/lower/node/hdfs_scan.rs`、`src/connector/iceberg/schema.rs`
  （`build_projected_output_schema`）、`src/formats/parquet/mod.rs`
  （`align_iceberg_array_to_field`）
- 写 sink：`src/connector/iceberg/sink.rs`（已映射纳秒，确认不被前置下转）
- 谓词：`src/common/min_max_predicate.rs`、`src/lower/expr/min_max.rs`
- cast：`src/exec/expr/cast.rs`
- MySQL 输出：`src/server/encoding.rs`（协议截微秒，确认行为符合预期）
- vendor 读侧来源：`vendor/iceberg-0.9.0/src/arrow/schema.rs`
