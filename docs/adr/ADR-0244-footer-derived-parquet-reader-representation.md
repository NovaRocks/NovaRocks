---
id: ADR-0244
title: "Derive the Parquet decoder representation from the verified footer"
domain: [provider-spi, low-cardinality]
status: active
supersedes: []
superseded-by: null
partially-supersedes: [ADR-0213]
date: 2026-11-20
provenance:
  - "discussion: 2026-11-20 footer-derived Parquet representations and exact column-chunk dictionary evidence"
related:
  - "ADR-0005-low-cardinality-runtime-carrier-first.md"
  - "ADR-0195-public-value-access-and-construction.md"
  - "ADR-0245-parquet-reader-sizing-and-scan-output-policy.md"
code-anchors:
  - "novarocks/fs/src/read.rs (ParquetInterpretationPolicy::cache_identity)"
  - "novarocks/fs/src/physical_reader/parquet/representation.rs (decoder_layout_and_rows, whole_chunk_dictionary)"
  - "novarocks/fs/src/cache/parquet_cache.rs (raw and interpreted metadata caches)"
  - "novarocks/connector/iceberg/src/typed_read/page_source.rs (ParquetFooterCache)"
  - "novarocks/type-contract/src/value_layout_match.rs (dictionary_value_layout)"
  - "novarocks/execution/src/exec/chunk/schema.rs (ChunkSlotSchema::reconcile_to_carrier)"
---

## 问题

湖表读取怎样在保持 Parquet 文件语义和表字段身份的同时，为单个 reader 选择字节叶子的解码表示和可续接的字典？

## 背景与执行事实

| 事实 | 作者与使用范围 |
|---|---|
| 原始 footer | FS 按授权访问域和准确文件身份保留 schema 树、字段 ID、物理路径、统计、编码信息和原顺序 KV |
| 基础 Arrow 解释 | FS 的解释缓存额外按完整政策与版本隔离。Iceberg 沿标准 Parquet 转换解释 footer，跳过 `ARROW:schema` |
| Reader 解码表示 | `decoder_layout_and_rows` 从标准 schema 和该 reader 的投影、行组及政策导出局部 supplied schema，通过 Arrow 的 `with_schema` 校验 |
| 表投射 | Iceberg 依据准确表 schema、真实字段 ID、name mapping、defaults、领域和冻结选集形成对外列，保持字段 requiredness |
| 页面载体 | Execution 依据冻结槽位布局 `L` 与 `dictionary_value_layout` 所定义的 `D(L)` 校验实际载体。消费能力由原算子声明 |

同一文件上的预备 footer、冷读取和 reader 打开均使用同一冻结选择派生的解释政策。Iceberg 的 footer 缓存按访问域、文件身份和政策隔离，FS 解释缓存键包含政策版本。与单次打开相关的投影、行组和 supplied schema 随 reader 持有。

## 考虑过的选项

1. **将 Arrow 写端提示或任意 supplied schema 作为文件语义。设计否决。** 写端可表达额外 Arrow 类型。将其用作权威会破坏从真实字段 ID、逻辑注解和表 schema 核对的契约。
2. **按标准 footer 派生局部字节载体。采用。** FS 复用公开 Parquet 转换和解码接口。view 可直接表达较大的重复变长列。确认为整列块字典的顶层字符串可共享值对象。每次打开需要核对投影与行组的编码信息。
3. **按批把平铺字符串重新编码为字典。设计否决。** 它无法续接原列块的字典值身份，会额外复制值，并使同一输入列的表示随单批选择变化。
4. **同时保证嵌套字典身份。成本否决。** Arrow 的 List/Map 构造路径会从 `ArrayData` 重建子数组。兑现嵌套身份需要跨 reader、绑定和 Chunk 的证据及额外实现。

## 裁决

1. **从真实 footer 派生。** 仅 FS 能在跳过提示的标准 schema 上替换受支持的普通 STRING/BINARY 变长叶子的物理载体。保留逻辑注解、真实字段身份、可空性、容器布局及领域的原判定。supplied schema 核对失败时终止并保留错误来源。此规则部分替代 [ADR-0213](ADR-0213-iceberg-read-interpretation-and-current-write-authority.md) 对使用 supplied schema 的否决。
2. **整列块证据。** 顶层字符串保持模式要求有字典页，且所读范围中每个非空列块均能证明其 DATA_PAGE/DATA_PAGE_V2 使用字典编码。缺少页编码统计时，仅接受 v1 编码集合中含 `PLAIN_DICTIONARY` 且其余编码限于 `RLE` 或 `BIT_PACKED` 的证据。出现 PLAIN 数据页或证据不足时，使用该普通字符串的 view 解码表示。嵌套普通字符串按普通布局交付。
3. **值对象身份。** 对合格列使用 `Dictionary(Int32, D(L))`。`Utf8` 和 `Utf8View` 槽的字典值布局均为 `Utf8`。代表同一列块的已兼容值数组沿切片、筛选和交接共享同一个对象。受支持的值布局确需适配时，识别原值对象并在其存续期间复用实际构造结果。新的列块独立核对值对象。既有消费方可按准确能力在入口展开。
4. **分别冻结布局和字典选择。** 开发控制以原语句授权分别选择普通字节列的 offsets/view 布局与顶层字典保持。同一选择进入执行声明和 Iceberg 私有载荷。默认最终输出采用普通 offsets 布局。真实列具有准确类型和值，实际 reader 批与转换后的 Chunk 分别提供验证事实。

## 接受的妥协（诚实记录）

- 旧 footer 可能无法证明字典资格，代价是按普通表示推进及对应转换成本。此选择避免为识别列块编码另写页解析器或增加一次完整扫描。
- 每个新 reader 需要按自己的读取范围派生和校验表示。缓存只保管可在完整政策键内复用的解释事实。
- 目前保留顶层字符串值对象。嵌套普通列及不能续接的消费入口仍须构造准确输出。Parquet 解码使用上游公开 API，Arrow 内部读页与 kernel 的同步工作留在原库。

## 何时重新评估

- Arrow reader、Iceberg 绑定与 Chunk 能端到端证明嵌套字段跨批值对象身份时，重新评估嵌套字典。
- 新增 Parquet 解码载体、改变解释规则或上游转换语义时，重审允许替换的叶子、缓存政策版本与旧文件证据。
- 真实语句的默认、显式 view、字典、混合编码和 NULL 已有完整原生证据，且消费方实际支持所需载体时，重新评估产品默认及开发入口退役。
