# IW-3 / SP1 设计:共享异步 Iceberg 文件写内核

- 日期：2026-06-03
- 分支：`claude/iw3-iceberg-async-writer`（基于 origin/main `df1a7c3b`，含已合入的 IW-1+IW-2 #239）
- 关联 Roadmap：`Iceberg Distributed Write Pipeline`（IW-3）
- 关联文档：`IW-3-iceberg-async-file-writer-sink.md`；IW-1+IW-2 设计 `docs/design/specs/2026-06-03-iw1-iw2-async-sink-foundation-design.md`

> 语言策略：本设计文档用中文；代码标识符 / 类型名 / 日志 / 错误信息用英文。

---

## 1. 战略背景与定位

IW 这条 roadmap 的终局是让 NovaRocks（含 standalone 模式）收敛到 StarRocks FE 的**“写入与提交分离”**模型：

- **writer（BE/driver）只写 staged data/delete 文件 + 上报 file metadata，自己不 commit；**
- **单一 coordinator（FE 角色）收齐后做唯一一次 table-metadata commit。**

现状里，FE-compat 路已是目标形态（BE 写 staged + 吐 thrift `TSinkCommitInfo`，FE 提交）；standalone 路是待演进的旧形态（`engine/iceberg_writer.rs` 用 iceberg-rs 自己写 + 自己 inline commit，也是 D2 `INSERT OVERWRITE` 多 BE hang 的那条路）。

IW-3 整体 = 做出两种模式共用的**模式无关异步 staged-file writer**（写入层）。它被拆为三个独立 spec/plan：

- **SP1（本设计）— 共享异步 writer 内核 + 统一描述符 + 两个适配器。** 直接单测，不切任何活路径。
- SP2 — FE-compat pipeline sink（`sink.rs`）改道到内核（`AsyncSinkBackend` + 工厂切换 + driver-harness 测试）。
- SP3 — standalone writer（`iceberg_writer.rs`）改道到内核（写 staged 跑在 `sink_io`，缓解 D2 hang；过渡期仍 inline commit，commit 集中化留给 IW-4）。

本设计只覆盖 **SP1**。

### 现状两条写路径（事实）

| 维度 | FE-compat（pipeline sink） | Standalone |
|---|---|---|
| 写入算子/入口 | `lower/fragment.rs:534` → `IcebergTableSinkOperator`（sink.rs） | `engine/iceberg_writer.rs` → `write_chunks_as_iceberg_data_files` |
| 用谁写 | 手写 Arrow→Parquet（`write_parquet_file`）+ 手动 thrift 统计 + theta sketch | iceberg-rs `DataFileWriter`（`connector/iceberg/data_writer.rs`，含分区扇出 + v3 variant） |
| 输出 | thrift `TIcebergDataFile`→`TSinkCommitInfo`（`sink_commit` 侧信道） | iceberg-rs 原生 `iceberg::spec::DataFile` |
| 谁 commit | FE | NovaRocks 自己（iceberg-rs commit action） |
| 是否被 standalone SQL 触达 | 否（仅 FE-compat） | 是（可用 standalone+iceberg-rest 端到端测） |

**差异分两类**：输出类型 + commit 归属是“本质必要”（协议不同）；而“Arrow chunk → 分区 → 写 parquet → 收列统计”这层物理写是**重复**，正是 SP1 内核要统一的部分。

---

## 2. 方案选择

**采用方案 A：内核统一到 iceberg-rs writer。** 把现有 `src/connector/iceberg/data_writer.rs`（已含 `write_record_batches_as_data_files` + 流式门面 `IcebergStreamingDataFileWriter`）硬化为共享内核，产出统一描述符；standalone 适配近恒等，FE-compat 适配转 thrift + 补 theta sketch。

理由：iceberg-rs 是更完整、原生、且与“FE/coordinator 集中提交消费原生文件元数据”方向一致的 writer（免费获得分区扇出 + v3 variant），并可消灭手写的 `sink.rs` writer（较弱、重复）。代价：FE-compat 的 thrift 等价映射需仔细做，等价性风险集中在 SP2（SP1 仅提供带单测的纯映射函数）。

被否方案：B（统一到手写 sink.rs writer）逆收敛方向、standalone 要重建分区/v3；C（抽 trait 两套并存）不消除重复。

---

## 3. 范围 / 非目标

### 3.1 范围（SP1）

- 把 `data_writer.rs` 提升为共享内核：把 writer 输入从 `iceberg::table::Table` 解耦为 `StagedWriteContext`。
- 定义统一描述符 `StagedDataFile`（iceberg-rs `DataFile` + 可选 theta sketch）。
- 内核 API：批量 `write_record_batches(ctx, batches, opts)` + 流式 `StagedDataFileWriter`（泛化现有 `IcebergStreamingDataFileWriter`，保留 per-batch backpressure 形态）。
- theta sketch 作为可选 pass（从 `sink.rs::collect_theta_sketches` 移植）。
- 两个适配器（纯函数 + 单测）：`to_iceberg_data_file`（恒等）、`to_sink_commit_info`（→ thrift `TSinkCommitInfo` + `FileSketchSet`）。
- `cleanup_staged_files(ctx, paths)` best-effort 清理工具。
- 加性重构：保留现有 `&Table` 接口（薄封装委托新核心），现有 caller 零改动。
- 直接单测矩阵（本地 `FileIO`）。

### 3.2 非目标

- 不切换任何活写路径（SP2/SP3）。
- 不做 Iceberg metadata commit（IW-4/IW-5）。
- 不引入 sink_io 管道（内核是纯 async fn；跑在 sink_io 由消费者负责）。
- 不实现 abort 兜底清理 / commit-unknown 恢复（IW-6）。
- 不改 FE thrift 协议、不改 standalone commit 行为。

---

## 4. 架构与接口

落点：硬化 `src/connector/iceberg/data_writer.rs`（不新开模块）。

### 4.1 `StagedWriteContext`（把 writer 输入从 `Table` 解耦 —— SP1 核心）

当前 `build_data_file_writer_with_schema` 依赖 `table.metadata()`（给 `DefaultLocationGenerator`）、`table.file_io()`、schema、partition spec。把这些零件收进一个上下文：

```rust
pub(crate) struct StagedWriteContext {
    schema: iceberg::spec::SchemaRef,
    partition_spec: iceberg::spec::PartitionSpecRef,
    file_io: iceberg::io::FileIO,
    location_generator: DefaultLocationGenerator, // 唯一仍需 TableMetadata 的零件
    format: DataFileFormat,
    writer_props: WriterProperties,
    partition_spec_id: i32,
}

impl StagedWriteContext {
    // standalone：平凡构造，保留现有 caller
    pub(crate) fn from_table(table: &iceberg::table::Table) -> Result<Self, String>;
    // FE-compat：SP2 从 thrift plan 造（schema/partition/FileIO/location 自备）
    pub(crate) fn from_parts(
        schema, partition_spec, file_io, location_generator,
        format, writer_props, partition_spec_id,
    ) -> Self;
}
```

> 接缝说明：`DefaultLocationGenerator::new` 仍要 `TableMetadata`。FE-compat 无 `Table`，需在 **SP2** 合成最小 metadata 或自定义 location generator。SP1 的职责仅是把 location generator 做成可注入参数（不再硬绑 `Table`），并提供 `from_table` 默认路径。

### 4.2 统一描述符 `StagedDataFile`

```rust
pub(crate) struct StagedDataFile {
    pub data_file: iceberg::spec::DataFile,                       // path/record_count/file_size/partition/列统计/split_offsets/content
    pub theta_sketches: Option<HashMap<i32, ThetaSketchHandle>>,  // 仅 collect_theta_sketches=true 时
}
```

以 iceberg-rs `DataFile` 为唯一真相源。theta sketch 是 StarRocks NDV 统计，iceberg-rs 不产，单独可选计算。

### 4.3 内核 API（纯 async，不碰 sink_io、不 commit）

```rust
pub(crate) struct StagedWriteOptions {
    pub collect_theta_sketches: bool,
    pub content: DataFileContent, // Data | PositionDeletes
}

// 批量
pub(crate) async fn write_record_batches(
    ctx: &StagedWriteContext,
    batches: impl IntoIterator<Item = RecordBatch>,
    opts: &StagedWriteOptions,
) -> Result<Vec<StagedDataFile>, String>;

// 流式（泛化 IcebergStreamingDataFileWriter）
pub(crate) struct StagedDataFileWriter { /* ctx, opts, buffered/rolling */ }
impl StagedDataFileWriter {
    pub(crate) fn new(ctx: StagedWriteContext, opts: StagedWriteOptions) -> Result<Self, String>;
    pub(crate) async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), String>;
    pub(crate) async fn finish(self) -> Result<Vec<StagedDataFile>, String>;
}
```

- 复用现有 `write_record_batches_as_data_files_with_writer` 的分区/未分区/v3-variant 逻辑。
- theta sketch：写每个文件时按 `opts` 计算（移植 `sink.rs::collect_theta_sketches`），挂到对应 `StagedDataFile`。
- 谁跑在 `sink_io`：消费者负责（SP2 经 IW-2 算子；SP3 standalone 在 sink_io 上 await）。SP1 不引入 sink_io。

### 4.4 适配器（纯函数 + 单测）

```rust
pub(crate) fn to_iceberg_data_file(staged: StagedDataFile) -> iceberg::spec::DataFile; // 恒等

pub(crate) fn to_sink_commit_info(
    staged: &StagedDataFile,
    ctx: &StagedWriteContext, // 取 staging_dir / content 等
) -> (crate::types::TSinkCommitInfo, Option<FileSketchSet>);
```

`to_sink_commit_info`：`DataFile` 的 path/record_count/file_size/partition/null_fingerprint/split_offsets/列统计 → thrift `TIcebergDataFile`；content → `TIcebergFileContent`；theta sketch → `FileSketchSet`。**等价性关键映射**——SP1 做成带单测纯函数，SP2 接活路径做端到端等价。

---

## 5. 加性重构边界（不破坏现有调用方）

底层换 `StagedWriteContext`/`StagedDataFile`，但现有 `&Table` 接口原样保留，改为薄封装：

- `write_record_batches_as_data_files(&Table, batches) -> Vec<DataFile>` 内部 = `from_table` → 新核心（`opts` 默认不收 sketch、content=Data）→ `to_iceberg_data_file` 映射回 `Vec<DataFile>`。
- `IcebergStreamingDataFileWriter::new(table)` / `write_batch` / `finish` 委托新流式 writer，返回 `Vec<DataFile>`。

→ MV refresh、`iceberg_writer.rs`、IVM-A1 MV sink 等 caller **零改动、行为不变**；回归风险低。

---

## 6. 错误处理 / 取消 / 清理

- **错误**：内核纯 `Result<_, String>`，如实上抛；无 fallback、无吞错（CLAUDE.md 快速失败）。
- **取消**：SP1 不拥有取消。“停止后台写入”是消费者能力（SP2 靠 IW-2 `AsyncSinkOperator::cancel`；SP3 自身）。
- **清理**：SP1 提供 `cleanup_staged_files(ctx, paths) -> Result<()>`（经 `FileIO` best-effort 删除）+ 单测。
- **正确性边界**：取消时 orphan staged 文件**永不会被 commit**（commit 独立、本任务非目标），无数据正确性问题；abort 时 future 被 drop 的兜底物理清理归 **IW-6**。SP1 的 cleanup helper 覆盖“显式错误/取消且仍持有 future”的路径。

---

## 7. 测试矩阵（直接单测，本地 `FileIO` `file:///tmp/...` 为主，沿用 `data_writer.rs` 既有测试设施）

1. 未分区写：N batch → `Vec<StagedDataFile>` 文件数/`record_count`/`file_size` 正确、文件存在。
2. 分区写：每分区 ≥1 文件、`DataFile.partition` 正确。
3. v3 variant 列：variant transform 生效。
4. theta sketch：`collect_theta_sketches=true` → 描述符含对应 field-id NDV；`false` → None。
5. `to_iceberg_data_file`：恒等回环。
6. `to_sink_commit_info`：`DataFile`+sketch → `TSinkCommitInfo`，断言 `TIcebergDataFile` 字段**语义等价**（path/record_count/partition_path/null_fingerprint/split_offsets/列统计 maps/content + `FileSketchSet` 的 field-id 集合）；**不要求** file_size/bound 字节级一致（换了 parquet 编码器）。
7. 流式 writer：`write_batch×N + finish` == 批量形式。
8. `cleanup_staged_files`：写文件 → cleanup → 文件消失。
9. 空输入：无 batch / 空 batch → 空 `Vec`。
10. 加性回归：现有 `write_chunks_as_iceberg_data_files` / `IcebergStreamingDataFileWriter` 的既有测试保持绿（证明门面委托不改变行为）。

---

## 8. 验收标准（SP1）

- 内核可异步写 staged 文件并产出统一 `StagedDataFile`；经 `to_sink_commit_info` 得到与旧 FE-compat 路**语义等价**的 `TSinkCommitInfo`/`TIcebergDataFile`。
- 多 writer 并发：file path 唯一（iceberg-rs `DefaultFileNameGenerator` + 唯一后缀）；stats 完整。
- 现有 `&Table` caller 零改动、行为不变（既有测试绿）。
- theta sketch、分区、v3 variant、cleanup 均有单测覆盖。
- “慢 I/O 不阻塞 driver”“cancel 端到端”属消费者侧（SP2/SP3 验收），不在 SP1。

---

## 9. 与后续 SP / IW 的关系

- **SP2（FE-compat 改道）**：`IcebergDataFileSinkBackend: AsyncSinkBackend` 用内核 + IW-2 算子；`StagedWriteContext::from_parts` 从 thrift 造（含 location generator 合成）；`to_sink_commit_info` 接活路径 + 端到端等价；切换 `IcebergTableSinkFactory`。
- **SP3（standalone 改道）**：`iceberg_writer.rs` 的写步改用内核 + 跑在 `sink_io`（缓解 D2 hang）；过渡期仍 inline commit（从描述符），为 IW-4 集中 commit 留接口。
- **IW-4/IW-5**：coordinator 收 `StagedDataFile` 描述符做唯一 commit。
- **IW-6**：abort 兜底清理 / commit-unknown 恢复。

---

## 10. 风险与决策记录

- **决策：方案 A（统一到 iceberg-rs writer）。** 贴合收敛方向、消灭手写 writer；FE-compat 等价风险集中到 SP2。
- **决策：描述符以 iceberg-rs `DataFile` 为唯一真相源 + 可选 theta sketch。** 避免发明新类型；standalone 近恒等。
- **决策：加性重构（门面委托）。** 现有 caller 零改动，降低回归。
- **风险：`DefaultLocationGenerator` 仍需 `TableMetadata`。** FE-compat 的合成留给 SP2；SP1 仅暴露可注入接缝。
- **风险：theta sketch 移植。** 从 `sink.rs::collect_theta_sketches` 移过来，需保持 field-id 选取与 NaN 归一化一致（单测覆盖）。
- **风险：FE-compat 列统计 thrift 映射等价性。** SP1 用语义等价单测约束；字节级差异（不同 parquet 编码器）不强求。

---

## 11. 预计文件改动清单（SP1）

- `src/connector/iceberg/data_writer.rs`：新增 `StagedWriteContext` / `StagedDataFile` / `StagedWriteOptions` / 新核心 API / 流式 writer 泛化 / `cleanup_staged_files`；现有 `&Table` 接口改薄封装委托。
- `src/connector/iceberg/sink.rs`：把 `collect_theta_sketches`（及其依赖）抽成内核可调用的纯函数（移动或暴露），供内核可选 pass 使用。**不改 `IcebergTableSinkOperator` 行为**（SP2 才动）。
- 适配器 `to_iceberg_data_file` / `to_sink_commit_info`：落在 `data_writer.rs` 或同模块的 `adapter` 子文件。
- 测试：`data_writer.rs` 既有 `#[cfg(test)]` 扩展（本地 FileIO）。

> 具体文件落点与拆分以 writing-plans 阶段为准。
