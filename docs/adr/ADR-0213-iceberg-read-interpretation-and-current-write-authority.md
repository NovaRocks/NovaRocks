---
id: ADR-0213
title: "Iceberg reads select Parquet interpretation separately from current write authority"
domain: [provider-spi]
status: active
supersedes: []
superseded-by: null
partially-superseded-by: [ADR-0214]
date: 2026-11-03
provenance:
  - "discussion: 2026-11-02 accepted Iceberg read interpretation and Historical/Current authority design"
code-anchors:
  - "novarocks/fs/src/read.rs (ParquetInterpretationPolicy)"
  - "novarocks/fs/src/cache/parquet_cache.rs (metadata caches)"
  - "novarocks/fs/src/physical_reader/parquet.rs (ParquetMetadataInspection)"
  - "novarocks/connector/iceberg/src/planning_facts.rs (PlanningAuthority)"
  - "novarocks/connector/iceberg/src/metadata_json.rs (bounded complete metadata decoding)"
  - "tests/sql/fixtures/iceberg-read-policy/fixture.scala (MetadataHistoryFixture)"
---

## 问题

同一 Parquet 文件被不同表格式和不同历史快照读取时，哪些事实可以按文件共享，谁决定 Arrow 表示、表字段身份与当前写入权限？

## 背景与执行事实

| 实体 | 权威与生命周期 |
|---|---|
| Parquet 原始 footer | 完整 schema 树、父子路径与顺序、repetition、注解、wrapper、真实 field ID、顺序 KV 与文件统计；不包含表的 selected schema 或默认值 |
| FS 原始缓存 | 按 access domain 与 `FileIdentity` 隔离同一授权文件；持有完整 `ParquetMetaData` |
| FS 解释缓存 | 在同一文件键外另绑定完整 `ParquetInterpretationPolicy` 与版本；标准 Parquet 转换所得的 Arrow 解释不能跨政策复用 |
| Iceberg Connector | 按 selected table schema、真实 footer ID、name mapping、defaults 与 domains 投射读取值；普通、历史、change、delete、ADD FILES 与独立 reader 采用同一读取政策 |
| Current 规划事实 | 当前 schema 与当前默认 partition spec 提供写默认值、写目标类型与分区源 membership |
| Historical 规划事实 | 所选 snapshot/schema 提供只读事实；没有 Current 写权限；历史文件分区解释另保留 `frozen_partition_types` |

`ARROW:schema` 是 Arrow 写端的表示提示，不能代替 Iceberg 表事实。合法重复 KV 的原顺序保留在 footer 中；标准转换器怎样选择提示仍由显式政策控制。缓存淘汰只释放缓存自己的引用，不使已安装 reader 或其他 alias 失效。

## 考虑过的选项

- **始终解释 Arrow 提示：设计否决。** 表示提示可能带来其他 Arrow 类型、丢失真实 field ID 或要求解释不参与 Iceberg 语义的内容，不能成为表格式读取权威。
- **解释失败后重试并忽略提示：设计否决。** 同一表会随提示是否可解析而采用不同语义，且把错误变成隐式回退；应在读取前选择政策。
- **FS 全局忽略所有提示：设计否决。** FS 服务多个格式，不能替其他 Provider 裁决其合法表示需求。
- **复制 Parquet logical converter 或以 supplied schema 覆盖文件解释：设计否决。** 这会增加第二套格式语义，或丢掉真实 footer 与表字段身份的核对。
- **原始事实与按政策解释分层，表投射留给 Connector：采用。** 同一文件的完整物理事实可共享，表示解释按消费者声明隔离，表默认值与快照不会污染文件缓存。
- **用当前默认 spec 解释 Historical 的写权限：设计否决。** 当前分区源可能在所选历史 schema 中不存在；空集合又会把“不可写”伪装成“没有限制”。
- **在此层补齐 Thrift/page-index 通用预分配上限：待评估。** 跳过 Arrow 提示不提供原始格式的完整内存上界；该问题属于所有 Parquet 消费者共享的 FS 与上游边界，不能靠第二套解析器或新增第三方补丁处理。

## 裁决

1. **Provider 先选政策。** Iceberg 的全部 Parquet 读取入口固定跳过 Arrow metadata。有效、无效或缺少提示走同一条标准 Parquet logical conversion；不尝试解码提示，也不在失败后换政策。真实 footer、必需扩展或表事实错误继续失败。
2. **完整文件事实规则。** FS 保留完整原始 footer，不把 leaf ID 清单当作 schema 树，不丢 wrapper、父节点 ID、repetition、注解或重复 KV。提示不能补造无 ID 文件的身份，也不能擦掉真实 ID。
3. **缓存语义隔离规则。** 原始缓存只按授权域和准确文件身份复用；解释缓存额外绑定完整政策与版本。selected snapshot/defaults/domain 映射属于 Connector 的表投射，不进入通用按文件缓存。reader、page-index 和所有 alias 保留其原政策及真实持有生命周期。
4. **Historical 只读规则。** 目录加载请求以中立的 `HistoricalSnapshot` 选择准确 snapshot；分析前即投射它的 schema，不在 Current 列已绑定后只更换执行 selector。规划直接借用已加载、验证的 typed table metadata，不为读取分区源与约束再次解析序列化载体，也不把解析失败转成空事实。所选 snapshot/schema 决定历史关系。写默认值、写目标类型与当前分区源 membership 只能由 Current 事实提供。Historical 的写权威显式不适用，写消费者必须拒绝，不得用空 slice 假装可写且无分区限制。
5. **完整 metadata 的物理深度规则。** logical depth 64 经 Iceberg field/type/array 包装后超过 Serde 默认的 128 个 JSON 容器。Provider 的原迭代扫描器先校验完整语法与有限物理深度（`4 × MAX_VALUE_TYPE_DEPTH + 32`，包含 envelope 与 defaults），随后一次解析原 SDK 类型；不截断历史 schema，不在失败后重试，也不改变逻辑类型准入。REST 完整响应、Hadoop 原 I/O 及 Provider 内冻结 schema/metadata 的再解析共用此入口，原授权、取消与错误分类保留。
6. **验证分层规则。** FS 单测保护两政策同文件隔离、重复 KV 与淘汰后的 alias；Connector 单测保护读取入口和 Current/Historical 接缝；原生 1FE+3BE SQL 用官方 SDK 固定输入与完整行袋校验深 metadata、真实 ID、历史选择及当前写限制。设计记录本身不证明这些运行已经通过。

## 接受的妥协（诚实记录）

Iceberg 不消费只存在于 Arrow 提示中的扩展表示；它必须能够从标准物理事实与表事实得到支持的语义。支持范围不因此扩大，无法准确投射的文件明确失败。

缓存分层增加键、引用与解释对象的维护成本。共享原始 footer 和真实 reader alias 可能在淘汰之后继续保留内存，这是实际生命周期，不以淘汰事件冒充容量已释放。独立 reader 也必须显式选择政策，不能只修热缓存路径。

跳过提示消除了 Iceberg 对 Arrow Schema FlatBuffer 解码的依赖，没有提供 raw Thrift footer 与 page-index 集合预分配的完整上界，也没有改变 Hadoop 测试 catalog 的单写者功能边界（ADR-0196）。

## 何时重新评估

新增 Provider、上游 Parquet 改变解释语义或新增读取表示时，重新审查完整政策与版本键。Iceberg 规范正式赋予新的文件扩展语义时，由 Connector 定义如何从文件和表事实投射，不能隐式恢复 Arrow 提示的权威。

上游提供可直接配置、覆盖 raw Thrift 与 page-index 的完整解码界限时，由 FS owner 统一评估并验证全部消费者；没有上游能力前保持该缺口可见，不把本记录当作已经完成通用解码加固。需要 Historical 写入时，先定义明确的目标版本与写入契约，不能把只读事实升级为 Current 权威。
