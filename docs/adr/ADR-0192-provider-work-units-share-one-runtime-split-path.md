---
id: ADR-0192
title: "Provider work units share one runtime split path"
domain: [provider-spi, pipeline-execution, sql-compiler]
status: active
supersedes: []
superseded-by: null
date: 2026-10-22
provenance:
  - "discussion: 2026-10-21 provider-defined work units, placement-only hints and SQL metadata views"
code-anchors:
  - "novarocks/spi/src/connector/read_stack/system_table.rs (SystemTableUnitHint)"
  - "novarocks/connector/iceberg/src/typed_boundary.rs (RuntimeSystemRelationSource)"
  - "novarocks/connector/iceberg/src/typed_read/system_table.rs (SystemRelationSplit, SystemManifestSplit)"
  - "novarocks/query-application/src/api/native.rs (RuntimeSplitPlacement, NativeScanWork)"
  - "novarocks/frontend-application/src/query_execution/provider_read_facts.rs (placement_of, read_properties)"
  - "novarocks/worker/src/typed_connector_runtime/stream.rs (TypedConnectorScanStream)"
  - "novarocks/connector/iceberg/src/metadata_partitions_view.rs (Provider-derived view)"
related:
  - "ADR-0156-connector-control-and-execution-resources.md"
  - "ADR-0159-driver-polled-connector-scan-streams.md"
  - "ADR-0185-compiled-packages-are-the-sole-static-plan-path.md"
---

## 问题

Provider 怎样同时表达单例 metadata 关系与可拆分文件关系，使工作粒度、Task 放置、计划分布和 driver 并行度各有唯一责任，并且改变放置策略不会改变 SQL 结果？

## 背景与执行事实

关系是一组带类型的行，工作单元是 provider 在冻结输入上定义的一份读取工作，split 是工作单元进入运行期分配的载体。三者不是同一概念：关系只有一个工作单元，不证明所在 fragment 只有一个 Task，也不证明输出具有全局单分布或 scan 后所有算子都只用一个 driver。

| 实体 / owner | 负责的事实 | 不赋予的能力 |
| --- | --- | --- |
| Provider 冻结引用与 split source | 固定 metadata 文件、表身份与所选 snapshot；枚举完整且互不重叠的工作单元 | 不选择查询 Task 数、聚合阶段或下游 DOP |
| FE 放置与分配 | 在 admitted backend 集合中放置 Task，将每个枚举单元交给一个目标 | 不凭单例提示声明关系全局分布，不补读 provider 数据 |
| BE split 接收与 Worker stream | 按分配序列去重，经统一 provider page-source 接口读取本 Task 的单元 | 不重开整关系 reader，不把重复投递当成新工作 |
| Scan driver / pipeline | 一个 Task 的一个 scan 由一个 driver poll；必要时交接到计划指定的下游 DOP | 不按 consumer 数复制 reader；不改变 provider 工作粒度 |
| SQL 计划与聚合 | 以静态分布规划 partial、exchange 和 final；定义空输入和 NULL 语义 | 不依赖放置优化来保证正确性 |

Iceberg 的 `$snapshots`、`$history`、`$refs`、`$manifests` 各枚举一个 `SystemRelationSplit`；`$files`、`$entries` 用 `SystemManifestSplit` 按 manifest 枚举。两类 split 保留不同 payload 与 wire tag，因为读取事实不同，但共同进入相同的分配、page source、stream、取消与退出流程。没有所选 snapshot 时，依赖 snapshot 的文件枚举可以是零单元。单例单元也可能读出零行，单元数不等于结果行数。

`SystemTableUnitHint::{SingleUnit, ManyUnits}` 映射到 FE 的 `RuntimeSplitPlacement::{SingleUnit, Spread}`。它们是放置提示，不进入 frozen physical package 或 native wire。静态 metadata scan 的分布仍是 `Unconstrained`；把单例单元强制放进多 Task 图时，只有获分配的 Task 读它，其余 Task 正常完成零单元 scan，并履行 exchange EOS。既有精确静态工作量 `FrozenUnits` 的事实表达保留；它不为 provider 新建读取通道。

Iceberg Provider 从同一次 `$files` 观察生成 `$partitions` 派生视图。SQL 在独立作用域使用原冻结 binding，过滤 `content=0`，按统一 partition ROW 聚合，或在无分区字段时用全局聚合加 `HAVING COUNT(*) > 0`。因此它只统计数据文件，`SUM` 沿用可空声明，`COUNT(*)` 必填；空输入输出零个分区统计行。普通全局聚合自身仍遵守空输入 `COUNT=0`、`SUM=NULL` 的 SQL 初始状态语义。

改这类代码前，先读 `SystemTableUnitHint` 与 `placement_of` 的提示边界，再读 `TypedConnectorScanStream` 的逐 split 推进与退出；涉及视图时读 Provider 的派生定义与 SQL 的隔离展开。SQL 保留原预绑定身份，外层普通表和 CTE 无法遮蔽冻结来源。

## 考虑过的选项

1. **接回 `WholeRelation` 整关系通道。设计否决。** 它可以让小关系跳过 split 枚举，但会把“provider 只有一份工作”扩大成 fragment 单 Task、静态单分布和 DOP=1 的约束；缺少单元的关系还需要另一套 reader 构造、取消与退出入口。它无法满足放置策略不影响正确性的约束。
2. **显式保留整关系与 split 两种执行模式。设计否决。** 类型比隐式分支清楚，但每个 provider 和消费者仍须维护两套生命周期，新增关系要选择不同的执行权威。使用单例单元即可表达同样的读取粒度，两模式与统一运行期读取契约冲突。
3. **Provider 定义工作单元，所有 provider 运行期读取使用 split。采用。** Provider 决定可拆分粒度，FE 决定放置，BE 用同一流执行。单例关系付出一次枚举和普通计划汇总的成本，换取提示可忽略以及生命周期和错误语义统一。
4. **用单例提示省掉汇总或强制所有算子 DOP=1。设计否决。** 它能减少 exchange，却使正确性和执行形状依赖可变放置偏好。消除汇总必须由独立、可验证的静态分布事实证明，不能从提示推导。
5. **在 FE 预读 metadata 结果行，再序列化给 BE。设计否决。** 它避免 BE 自己读取，却让 FE 成为另一套 provider 执行器，并把准确冻结事实替换为预读数据。FE 可以读取枚举所需的 metadata/manifest-list，但不以结果行绕过 provider split 路径。
6. **工作单元级恢复与多 reader lane。待评估。** 它们可能减少重读或提高单 Task 解码吞吐，但需要定义已消费数据的可见性、聚合与 exchange 状态恢复，以及多流资源责任；本裁决不引入这些能力。

## 裁决

1. **Provider 粒度规则。** Provider 在准确冻结输入上枚举完整、互不重叠的单元；单例关系发一个单元，空工作可以发零个。文件、manifest 或整个小关系可以是不同粒度的单元，但均通过 runtime split 和统一 page-source provider 执行。不得添加 `WorkSource` 判别、整关系工厂或缺 split 时的 fallback。
2. **提示只放置规则。** 单元提示只影响 FE 的放置选择，既不成为 wire 事实，也不约束 Task 数、静态分布或 DOP。忽略提示、强制多 Task 仍须得到相同结果。只有调度得到 Task 的 backend 才是本次查询参与者，不从集群规模推测参与数。
3. **分布独立规则。** `Unconstrained` 的 scan 必须按普通分布规则规划聚合与交换。单例关系仍允许 partial → exchange → final；零单元 Task 正常结束并发送 EOS，不能靠“它应该没有被调度”跳过终止义务。Scan 只有一个推进 driver，下游并行度在 scan 分支交接处按计划兑现，遵守 [ADR-0159](ADR-0159-driver-polled-connector-scan-streams.md)。
4. **分层唯一性规则。** Provider 保证枚举完整且不重叠，FE 将单元分配给一个目标，BE 对本 Task 的重复分配消息按原序列幂等接收。完整消费同一次成功 attempt 时，三层共同避免重复或漏读；这不是跨 attempt 的全局 exactly-once 账本。LIMIT 正常早停允许不读取余下单元。失败后的新 attempt 重新枚举和读取，并重建该 attempt 的聚合、exchange 状态，不在旧输出后直接续读剩余单元。
5. **冻结身份与 SQL 视图规则。** Provider 验证精确 metadata 位置、表 UUID、snapshot 与 schema。共享 schema、分区规则、输出声明及统计解释依据由 Iceberg scan 拥有，一个 Task 绑定一次。manifest split 携带自身描述和原冻结引用，Provider 在打开对象前核对引用、分区规则与投影。Iceberg Provider 用同一次 Files 观察生成 `$partitions` 的 SQL 与精确声明，SQL 在隔离作用域展开并消费原 Files binding，保留限定名、别名和 CTE 边界。统一 partition ROW 按 field ID 构造，展示名消歧保留原字段身份。
6. **资源与错误保留规则。** 统一通道不删除原有非法输入、provider、取消、deadline 和真实资源拒绝。BE 在 Task 准入后绑定执行资源；FE 发现仍是操作控制与普通所有权，遵守 [ADR-0156](ADR-0156-connector-control-and-execution-resources.md)。退出仍须观察在途工作真实结束，不能以 EOS 或 Drop 代替。共享 page-source 接口不新增免准入 reader。
7. **单次契约演进规则。** 删除旧工作来源词汇与整关系读取方法时，同时更新 frozen package、native codec 与 provider 私有 split vocabulary，退役字段/tag 保持 reserved，保留仍有效的 split 类别身份。按 [ADR-0185](ADR-0185-compiled-packages-are-the-sole-static-plan-path.md) 使用唯一 compiled 解释链和准确兼容岛，不通过旧 decoder、协商或混合模式连接新旧进程。

## 接受的妥协（诚实记录）

单例关系在静态层仍是 `Unconstrained`，普通分布式聚合可能比按提示宣称单分布多一次汇总。我们接受这个小关系成本，避免把调度偏好变成结果正确性的前提；若将来消除该成本，必须提供独立的静态证明。

当前不提供工作单元级恢复。一次失败可能重读已经完成的 manifest 或关系；旧 attempt 排空与新 attempt 运行也可能并存。分层去重没有替聚合、exchange 或客户端结果建立跨 attempt 的提交点，不能宣传跨 attempt exactly-once。

统一 split 不自动使单元内部流式或有界。单个 metadata 单元仍可能先构造较大的结果，协作预算也不能抢占正在运行的同步 kernel。我们没有以这个模型证明 RSS 上界、可恢复 OOM 或性能改善；更细粒度、真正流式构造和多 reader 需要另行设计及测量。

共享词汇、wire 与双方消费者一次更新，要求按准确兼容身份协同部署；不保留双模式作为升级桥。本裁决写入时，原生 `1FE+3BE` 场景与 Spark 分区对照尚未完成，文档与静态/局部执行验证不能代替这些证据，也不构成全量产品通过的结论。

## 何时重新评估

- 单例关系的汇总开销在真实负载中成为主要成本，并且规划器具备独立可验证的分布证明时，评估按该证明消除 exchange；提示本身仍不得充当证明。
- 单个 manifest 或 metadata 关系造成显著内存/取消长尾时，评估 provider 内部流式构造或更细的工作单元，保持同一个 runtime split 与资源所有权边界。
- 单 driver 解码饱和且下游空闲，进程资源治理能覆盖多流预读与持有对象时，按 ADR-0159 评估多 reader lane；不能仅增加 reader 数而复用一份窗口责任。
- 重读成本成为恢复瓶颈，并已有聚合、exchange 与结果可见性的统一 checkpoint/提交协议时，评估工作单元级恢复；须先证明部分结果不会重复或丢失。
- 产品要求混合版本升级时，先定义兼容权威与显式测试矩阵，再重评估单次 wire 演进成本；不得以隐式旧 reader 或 decoder fallback 实现。
- 原生多 Task、零单元、取消或 Spark 分区对照发现与模型不符时，保留原始失败并修正对应 owner；在补齐实际证据前持续保留验收缺口。
