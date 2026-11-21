---
id: ADR-0248
title: "Parquet reader sizing and scan output shaping have distinct owners"
domain: [provider-spi, pipeline-execution]
status: active
supersedes: []
superseded-by: null
partially-supersedes: [ADR-0014, ADR-0222]
date: 2026-11-20
provenance:
  - "discussion: 2026-11-20 independent Parquet reader sizing, complete output prefixes and source custody"
related:
  - "ADR-0159-driver-polled-connector-scan-streams.md"
  - "ADR-0195-public-value-access-and-construction.md"
  - "ADR-0222-session-property-declarations-and-required-constant-evaluation.md"
  - "ADR-0247-footer-derived-parquet-reader-representation.md"
code-anchors:
  - "novarocks-server/library/src/provider_manifest.rs (IcebergSessionProperties)"
  - "novarocks/connector/iceberg/src/typed_boundary.rs (freeze_read_execution)"
  - "novarocks/frontend-application/src/query_execution/provider_read_facts.rs (finish_read_from_opened)"
  - "novarocks/fs/src/read.rs (ReaderSizing)"
  - "novarocks/spi/src/connector/read_stack/scan_policy.rs (ENGINE_OUTPUT_POLICY)"
  - "novarocks/value-arrays/src/measure/mod.rs (PrefixPlanner::plan_prefix)"
  - "novarocks/connector/iceberg/src/typed_read/output.rs (OutputCursor)"
  - "novarocks/execution/src/connector/page_adapter.rs (SourcePageConverter)"
---

## 问题

Parquet 解码和最终扫描输出分别由谁确定批大小，跨批共用的存储由谁保管？

## 背景与执行事实

| 责任 | 作者 | 生效位置 |
|---|---|---|
| 每个 Parquet reader 的设置 | Iceberg catalog 和准确语句绑定 | FE 对已经协商的读取关系冻结私有执行设置，BE 按该载荷准备、打开和读取数据文件及 delete 文件 |
| Decoder 批行数 | FS 的 `ReaderSizing` 和实际读取表示估计 | 打开 reader 时，按投影和行组选定。内部页解码由 Arrow 推进 |
| 最终输出分批 | SPI 的 `ENGINE_OUTPUT_POLICY`，由实际 producer 使用 | 选出可见行和最终列后，按 8192 行上限和 1 MiB 拟新建列存储软目标生成完整前缀 |
| 逻辑字节 | 公共值层与原发布位置 | 根据每行实际可达值计量，字典值每次引用均计入，用于诊断 |
| 实际存储与合作推进 | 原 reservation/creator 及实际 Driver | 最后真实持有者退出时释放源责任。Driver 每轮补充合作预算 |

Iceberg 的 catalog 键为 `parquet.max-read-block-row-count` 和 `parquet.max-read-block-size`。会话属性分别为 `parquet_max_read_block_row_count`（INTEGER，128 到 65536，缺省 8192）和 `parquet_max_read_block_size`（VARCHAR，Airlift `DataSize`，非负 `i64` 字节，缺省 `16MB`）。同一 catalog 控制代与绑定的原语句覆盖优先，随后取准确 catalog 的已验证设置和产品缺省。私有载荷显式保留数值存在性，`0B` 属于合法输入。这里补充 [会话属性 ADR-0222](ADR-0222-session-property-declarations-and-required-constant-evaluation.md) 的实例声明，并替代其中关于 BE Iceberg 读设置尚未投影的安排。

## 考虑过的选项

1. **统一会话批大小并一路传给所有消费者。设计否决。** 相同输入规模会因过滤、补列、字典、view 和输出所需的复制产生不同构造量。无 Parquet I/O 的关系也需要自行组批。
2. **FS 解码后按估计体积试切整个批。设计否决。** FS 看不到 delete、谓词、分区常量及最终布局。从已解码数据试复制会引入额外构造，且字节目标无法作为合法宽行的准入条件。
3. **分别确定解码粒度和最终输出前缀。采用。** 冻结的 Parquet reader 设置改善解码粒度，完整行前缀使用 producer 实际可见的数据。需要分别观测源批、逻辑值和实际构造。
4. **随活跃输入动态调整同一 reader 的解码窗口。待评估。** 可适应行宽变化。当前公开 reader 入口在创建时定批，重开行组会重放内容和格式状态。

## 裁决

1. **按准确语句冻结读设置。** 普通与已准备读源汇合后经同一 Provider 入口撰写执行设置。保留原表、快照和选集身份。BE 消费已校验私有载荷。全局扫描预算、通用 `QueryOptions.batch_size` 及重复的计划批限制退役。
2. **在打开时估算解码粒度。** 有正的行宽估计时，取 `min(max_read_rows, max(1, floor(max_read_bytes / estimated_row_bytes)))`，实际计算保留整数单位。零宽或未知取行数上限。按实际解码表示计量固定宽度、字典键、view 描述符和可用的 backing 估计。数据文件采用 [ADR-0247](ADR-0247-footer-derived-parquet-reader-representation.md) 的解释。FS 返回实际 decoder 批，并公开由当前 reader 确定的文件 schema。Iceberg 先按标准 footer 确定读取列，打开后按实际 decoder schema 核对投影，再按字段 ID 绑定子字段和字节载体。ADD FILES 沿同一字段绑定的标准物理解释核对登记输入。此规则和下一条替代 [ADR-0014](ADR-0014-connector-neutral-file-foundation.md) 中 FS 拥有最终批预算的安排。
3. **先确定最终列与行域，再提交共同前缀。** 选择、NULL、绝对位置、补列和 change-window 符号参与同一次计划。对准确源范围借用合法存储，或按所得前缀构造一次。第一行可表示且超过软目标时单独交付。真实表示边界限制可交付前缀。分区常量、rewrite-position、metadata 与零列输出也推进各自的实际行域。sequence、表函数及 FE 系统关系按引擎行数政策组批。
4. **共享来源责任按真实别名续接。** 一次转入共享 hold 的原 reservation 随未消费源与实际引用它的输出续接。独立的新输出有自己的责任。完整复制且已切断源引用时即可释放该输出的源持有。SourcePage、Chunk 及其派生存储保留原 holder。仅独占且确已释放的存储适用原缩减协议。分批字节量也不进入真实容量结算。
5. **在外层安全点合作。** Driver 将实际轮次预算绑定到活动读源，预备状态独立保管。拉取、空批、过滤为空、切换 split 与下一次完整前缀之间在可控制外层合作让出。一次同步前缀计划及实际构造可在同一个获准单位内完成，内部观察真实取消与期限。真实退出由原 I/O、source 和 Driver 工作的看管者观察。
6. **扫描和消费者各自声明准确布局。** 显式 view 的 provider 执行声明用于物理扫描槽，封包核对保持精确。SQL 保留消费者已分析的值类型。需要 offsets 的消费者经现有 Project 接收同域字节布局投影，使用公共值数组构造取得准确的字段、NULL 和载体。BE 在该投影前观测扫描的实际 Chunk；字典仍沿原 Driver 的解码政策适配。

## 接受的妥协（诚实记录）

- 估算使用公开 footer 统计，遇到未知宽度继续按行数读取。单个实际批的物理内存、外部 I/O 和上游库同步工作量取决于原实现与资源。
- 完整前缀构造可以跨越软目标交付一条宽行。它还可能暂时与旧源批共存，原 creator 和 MEM 责任保管这些真实存储。
- 源存储可能在 reader 关闭以后仍由结果持有。多个输出页可能共同依赖同一份大字典。观察到停止与观察到真实资源退出分别取证。

## 何时重新评估

- 上游公开 API 能在不重放已消费内容的前提下调整同一 reader 的窗口，并暴露可协作的格式推进时，重新评估动态解码窗口。
- 真实样本证明 footer 估算或输出构造目标在代表性负载上系统性失配时，重审相应估算与分批参数。
- 新增支持的表格式或 producer 时，核对其读取设置的准确归属、共同前缀规则、来源交接和外层合作证据。
