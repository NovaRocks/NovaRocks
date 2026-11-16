---
id: ADR-0235
title: "Iceberg write layout uses partition affinity and real capacity owners"
domain: [provider-spi]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0023
    scope: "The fixed 16,384 logical writers and 64 MiB aggregate write data-plane budgets only. The existing supersession by ADR-0133 remains"
    link: "ADR-0023-connector-distributed-writer-contract.md"
  - id: ADR-0030
    scope: "The inherited fixed 16,384 writers and 64 MiB aggregate staging budgets only. The existing supersession by ADR-0133 remains"
    link: "ADR-0030-connector-distributed-rewrite-contract.md"
  - id: ADR-0133
    scope: "The fixed fragment and prepared-write-set limits repeated at producer, root ingress and FE receive. Dataflow writers, one resource-governed root, per-driver custody and FE-only commit remain active"
    link: "ADR-0133-dataflow-connector-writer-and-frontend-commit.md"
date: 2026-11-16
provenance:
  - "discussion: 2026-11-16 accepted partition-affine write layout, task-local spreading, file shaping and capacity ownership"
  - "reference: Trino commit 2162c006a71, IcebergPageSink, IcebergConfig and SkewedPartitionRebalancer"
code-anchors:
  - "novarocks/connector/iceberg/src/commit/partition_key.rs (partition_layout, IcebergPartitionKeyEvaluator)"
  - "novarocks/sql/src/planner/distributed/build/contract_lowering.rs (lower_ordinary_write_layout, lower_write_layout_key)"
  - "novarocks/physical-plan/src/plan.rs (NodeKind::WriteLayoutKey, EdgePartitioning)"
  - "novarocks/local-compiler/src/lowering.rs (lower_write_layout_key)"
  - "novarocks/native-adapter/src/compiled_writer_binding.rs (bind_compiled_writers)"
  - "novarocks/execution/src/exec/operators/write_layout_key.rs (WriteLayoutProjectionFactory)"
  - "novarocks/execution/src/exec/operators/write_spread.rs (WriteSpreadState, WriteSpreadDriver)"
  - "novarocks-server/library/src/provider_manifest.rs (IcebergSessionProperties)"
  - "novarocks/query-application/src/sql/session/registry/inputs.rs (SessionPropertyInputs::connector_catalog_overrides)"
  - "novarocks/frontend-application/src/query_execution/write_session/begin_receiving.rs (WriteBeginReceiving::bind_catalog_session)"
  - "novarocks/connector/iceberg/src/commit/write_stack/control.rs (freeze_writer_output)"
  - "novarocks/connector/iceberg/src/commit/data_writer/streaming.rs (StreamingStagedDataFileWriter)"
  - "novarocks/connector/iceberg/src/commit/write_stack/execution.rs (IcebergDataStackWriter::refresh_buffer_charge)"
  - "novarocks/execution/src/exec/pipeline/async_writer.rs (can_accept, try_reserve_input)"
  - "novarocks/frontend-application/src/query_execution/write_computation.rs (interpret_borrowed)"
  - "novarocks/frontend-application/src/query_execution/write_result.rs (RootWriteResultDecoder)"
  - "novarocks/connector/iceberg/src/commit/write_stack/codec.rs (IcebergWriteFragmentDecoder)"
  - "novarocks/trino-adapter/src/error_mapping.rs (open_partition_limit)"
---

## 问题

Iceberg 写入如何按表的分区契约成形文件，又能扩散热点并让每一段数据占用由实际持有者准入？

## 背景与执行事实

分区表的每个 driver 各自维护所见分区的 rolling writer。数据直接落到产出它的 driver 时，一个
分区会被多个 writer 分别写出，小文件数和打开的缓冲随并行度增长。先让分区值相同的行聚拢，
可以减少打开的 writer 和小文件。高度集中于一个分区的输入仍需要多个 writer 承接其数据量。
文件大小、路由和写集生命周期由不同组件拥有。

| 作者 | 事实与责任 |
|---|---|
| Iceberg FE control | 从冻结目标 schema 和当前写入 spec 签发数据分支的分区键配方。把目标文件大小、Parquet row group 字节上限和打开分区上限冻结进 writer handle。 |
| SQL、物理计划与 LocalCompiler | 将分区亲和落实为一条 `NativeHierarchicalV1` 写入交换。局部编译核对 `WriteLayoutKey` 与唯一布局配方，并为路由列保留发送和接收布局。 |
| BE Task 准备与 Execution | 按已准入的 Connector generation 绑定无 I/O 的分区键求值器，按发送端 Task 的实际目的集合和 DOP 计算扩散规模。发送侧用布局键选 Task，接收侧用同一键选 driver。 |
| BE Iceberg writer | 按真实 Iceberg 分区拆分每个页，保管打开的 rolling writer、暂存路径及关闭中的 future。用原 Task 资源账本计入活跃 Parquet 缓冲估计。 |
| BE TableWriter、TableFinish 与 FE 写会话 | 按原数据流传递片段。BE 以实际缓冲与片段计费，FE Internal 结果窗口保管载体与解码事实，完整接收和发布仍沿原写会话。 |

数据流、单 root、每 driver 的暂存所有权和 FE 唯一提交权延续
[ADR-0133](ADR-0133-dataflow-connector-writer-and-frontend-commit.md)。
原业务就绪、累计效果和退出证据遵循 [ADR-0198](ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md)。
完整接收后的 operation 和建表回执附件沿 [ADR-0234](ADR-0234-operation-owned-artifacts-and-bounded-publication-facts.md)。

## 考虑过的选项

1. **分区静态 hash。成本否决。** 实现简单，低流量分区可以稳定聚拢。集中写入时一个分区会独占一个
   writer 的处理通路。接受一次交换的成本以后，再给发送端增加有限的扩散状态可以兼顾这两类负载。
2. **Provider 根据写入热点选择槽位。设计否决。** 槽位数依赖目的 Task 和 engine DOP，属于运行时放置政策。
   Provider 的接口只表达外部系统的分区值，保持同输入同键的纯求值。
3. **完整的写入负载再平衡。成本否决。** 按每个目的 writer 的负载调度会减少 hash 碰撞，并能继续优化接收端
   局部路由，代价是额外的目的端负载状态与指派协议。当前采用发送端局部判断和既有两级 hash。
4. **内存压力触发提前关闭或扩大固定写集额度。设计否决。** 进程资源压力暗中更改文件成形会混合文件政策与
   容量拒绝。固定片段数和写集字节也无法表达实际活跃对象的保留量。Writer 按冻结的文件政策关闭，资源作者
   依据实际预算接纳或报告类型化失败。
5. **维护 SDK 分叉以直接读取分配内存。成本否决。** 现有 SDK 的文件状态和 Provider 自己的成功写出计数
   足以估算尚未落盘的缓冲。引入长期分叉的维护成本高于当前所需精度。
6. **动态扩缩 writer 和按目标表排序。待评估。** 各自需要进一步定义交换代价、活跃 writer 的
   放置或排序内存归属。

Trino 的 Iceberg fanout writer、按写出量检查闲置以及基于处理字节的热点扩散提供了取舍依据。
NovaRocks 在发送端按桶加槽后继续沿用既有两级 hash。

## 裁决

1. **Provider 定义分区等价。** 目标数据分支可声明 `PartitionAffinity`。Iceberg 按冻结写入 spec 选源列，
   复用 writer 的写出适配和 Iceberg SDK 变换，将每行的分区值折成确定的非空 BIGINT 键，NULL 参与折算。
   变换前的值与 writer 经 V/W 适配后拆分所见的值一致。未知变换在规划时类型化拒绝。归属和合法性仍由
   writer、表定义与原提交契约决定。普通写入、CTAS 和 MV 写入为这一偏好建立独立 Spread writer fragment，
   MOR 数据分支在既有路由中选择 hash，分区键只对该分支接收的 change effect 求值。无分区亲和的目标继续用原形状，DV 和 position delete 保留各自的路由要求。
2. **布局键的消费限于路由。** SQL 在发送 fragment 根插入 `WriteLayoutKey`，为这条边导出唯一
   `routing_only` BIGINT。实例级和 driver 级分别用既有 `InstanceV1`、`DriverV1` hash 域。
   接收端在本地 hash 之后删去键，writer 的列映射、字段身份、完成产物与 SQL 结果按原声明处理。
   `ByKey` 仍按其声明的真实输入和归属键校验。计划和 Provider 私有配方按各自 revision 准入，旧形状由各自的
   版本边界明确拒绝。
3. **Execution 决定扩散。** 每个发送端 Task 的 driver 共享 4,096 个桶及总处理字节。每页按各桶的行数
   比例分配输入页内存字节。桶已有 `s` 个槽位、累计 `b` 字节，Task 累计 `B` 字节、预计目的 writer
   数 `W` 为该边的实际目的 Task 数乘发送端实际 DOP。槽位从 1 起、最多为 `W`。只有
   `b ≥ 120 MiB × s` 且 `b × W > B × s` 才加一，槽位只增不减。每个 driver 按桶在页之间轮换槽位。
   同一页同桶使用同一槽位。零号槽直接使用分区键，其余槽按确定性混合得到布局键。
4. **FE 冻结成形政策。** 目标文件大小按表属性 `write.target-file-size-bytes`，缺省取 catalog
   `iceberg.target-max-file-size`（默认 1 GiB）。Parquet row group 字节上限按表属性
   `write.parquet.row-group-size-bytes`，缺省 128 MiB。打开上限取实际语句冻结的 catalog session 属性
   `max_partitions_per_writer`，缺省取 catalog `iceberg.max-partitions-per-writer`（默认 100）。
   session 声明为正 SQL INTEGER，允许范围为 1 到该 catalog 的上限。FE 在属性准入时保留准确控制运行时身份。
   原 `WriteBeginReceiving` 按写入 pin 投影属于该 catalog 与控制运行时的准入值，Provider 在同一写会话内
   冻结相应值并校验表属性。BE 只消费 handle。
5. **限制打开的分区 writer。** 新开一个分区超过上限时携带上限与本次所需打开数的失败事实，Trino 将其
   映射为 `ICEBERG_TOO_MANY_OPEN_PARTITIONS`（`0x0504_0002`）。打开数约束 fanout 的缓冲规模。
   SDK 按目标文件大小滚动。在该 writer 两次检查之间交给存储的成功写出量超过 256 MiB 时，关闭检查区间内
   未收到行且当前文件已写出超过 16 MiB 的分区。关闭结果仍由原槽位接收，同一分区随后可重新打开。
   失败沿原 abort 清理全部未封存对象。已封存文件和发布 Unknown 沿原交接。
6. **活跃写缓冲跟随 Task 账本。** 通过 SDK 的当前文件写出大小扣除相同路径下 FileIO 已接受的字节，
   对打开且已开始写入的文件求和。原 Task 的 `WriterState` 预留在第一次观察到正缓冲量时取得，随后按实际
   估计增长、缩小、归零释放。未配置查询内存上限时，这份预留提供 Task 统计可见性，打开数仍受
   冻结政策保护。账本拒绝增长时原样传播类型化资源错误。
7. **实际容量作者负责页和写集。** Writer 队列为空时接纳一页，包括超出软行数或软字节目标的页，入队
   仍需原 tracker 预留。队列已有数据时按已占用批数、行数和字节施加背压。片段与 prepared set 沿
   BE writer 和 finish 内存跟踪及 FE Internal 结果窗口准入。FE 对复制的载体和 Provider 解码图分别取得
   原作者的回执，解码对象真实退出后归还。写集规模、单片段、Iceberg 片段内部结构以及统计产物的固定
   条数与总量额度退出。格式、身份、字段类型与结构有效性继续验证，写入和 ANALYZE 共用统计产物规则。
8. **分帧和其他所有权独立保留。** TableFinish 输出批与 FE 根结果记录组装各有 32 MiB 分帧边界。
   TableWriter multiplex 行有独立的 16 MiB 分帧界限，统计产物单体与批也有原载体限制。
   这些界限随各自的传输及结果表示一起治理，超出时由原边界报告类型化失败。writer handle、写入
   schema、目标数、辅助通道和 tablet `WriteCommitEvidenceLimits` 的容量分析留给相应载体 owner。
   提交 operation 账本与 CTAS/MV 回执附件持有自己的进程内事实，退出可晚于写集回执。

## 接受的妥协（诚实记录）

- 同一热点的多个槽位可能 hash 到同一 writer。两个分区落入同一桶也可能一起扩散。发送端用自己的
  DOP 估计接收端并行度，各发送端分别观察其输入，因而扩散效果受数据分布与 Task 划分影响。
- 槽位单调增长。热点随时间转移时，所有桶的累计槽位可能高于稳定分布下的估算。闲置检查只能回收具备
  所需写出量的闲置 writer。大量离散分区仍可能达到打开上限，并在暂存了一部分数据后触发 abort。
- 新可用的分区亲和消耗一次交换及一个布局键列的内存。非分区表继续由上游已有并行度写出，其文件数可因
  输入并行度较高而偏多。
- SDK 的 rolling 决定依据当前写出大小，文件大小仍随 row group、写入页与最终编码而变化。缓冲差值是
  Parquet 对未落盘编码大小的估计，包含输出缓冲尾部，查询内存上限以该估计记账。
- 资源额度退出后，写集占用与真实产物规模相关。统计和记录分帧仍可能拒绝某个表示过大的单体。
  查询未设置内存上限时，BE 计量提供可见性并沿实际容量边界工作。

## 何时重新评估

- 热点并行度持续被 hash 碰撞或发送端 DOP 估计限制时，比较负载指派和接收端再平衡。
- 典型写入同时有强烈的冷热变化和长尾小文件时，比较动态 writer 扩缩、slot 收缩和排序写入，
  并先明确相应内存及放置责任。
- SDK 提供稳定的活跃内存读数，或 Parquet writer 纳入受治理分配器时，让同一个 Task 账本消费更直接的
  字节事实，同时保留按真实退出归还的边界。
- 根结果分帧、writer multiplex 或统计单体限制阻挡了合法的大产物时，沿各自的结果或传输表示审查
  分段和准入能力，再调整对应界限。
