---
id: ADR-0177
title: "Maintenance inputs live in one attempt and rewrites consume one frozen plan"
domain: [provider-spi, table-maintenance]
status: active
supersedes: []
superseded-by: null
partially-superseded-by:
  - id: ADR-0251
    scope: "The DV-only execution carrier and rules 3 and 4's OPTIMIZE ref check at writer begin and RefUnchanged on commit"
    link: "ADR-0251-iceberg-maintenance-commits-by-operation.md"
partially-supersedes: [ADR-0028]
date: 2026-10-14
provenance:
  - "discussion: 2026-10-14 attempt-local maintenance inputs and single frozen rewrite plan"
code-anchors:
  - "novarocks/spi/src/connector/process_local.rs (ConnectorProcessLocalPayload)"
  - "novarocks/connector/iceberg/src/catalog_control/metadata_maintenance.rs (IcebergMetadataMaintenanceAdapter::plan_maintenance, payload_for_plan)"
  - "novarocks/connector/iceberg/src/catalog_control/cleanup_maintenance.rs (IcebergCleanupMaintenanceAdapter::begin_async_in)"
  - "novarocks/connector/iceberg/src/distributed_rewrite.rs (IcebergDistributedRewriteControl::build_plan, FrozenRewritePlanContent, canonical_bytes)"
  - "novarocks/connector/iceberg/src/commit/write_stack/control.rs (IcebergWriteSessionControl::admit_async_with_rewrite_scope)"
  - "novarocks/connector/iceberg/src/commit/write_stack/control/publication.rs (WriteRecipe::freeze)"
  - "novarocks/frontend-application/src/query_execution/distributed_rewrite.rs (ConnectorDistributedRewriteSession::from_original_session, validate_rewrite_target_groups)"
  - "novarocks/table-maintenance/src/product.rs (run_rewrite_session_terminal, execute_cleanup)"
---

## 问题

在维护作业只属于当前 FE 尝试的前提下，冻结输入应该存放在哪里，如何保证重写的读取、writer 与提交消费同一份选择？

## 背景与执行事实

维护作业有三类冻结输入：元数据维护的计划载荷、分布式重写的文件组，以及孤儿清理的候选清单。
冻结的作用是让本次尝试始终消费同一个输入，并在产生外部效果前核对表身份与上限。它不授予跨进程恢复权限。
ADR-0110 规定结果未知后停止自动 mutation；ADR-0111 将维护作业、尝试与历史记录定义为进程运行态；
ADR-0133 将分布式 writer 留在 BE 数据流中，而外部提交只归 FE。持久化一份不被 Iceberg 元数据引用的维护输入，
既不能改变这些权限，也会让外部孤儿清理器把它视为无引用对象。

| 输入或对象 | owner 与持有范围 | 消费方式 |
|---|---|---|
| 元数据维护计划载荷 | Iceberg provider 生成，FE 的计划值持有 | 从同一计划的 provider payload 解码；不另写计划对象 |
| `FrozenRewritePlanContent` | Iceberg provider 冻结表头与有序文件组，计划、cohort 与 FE 查询句柄共享 `Arc` | 按 provider 归属与私有类型取回；核对操作 ID、内容摘要、组摘要及表事实 |
| `FrozenCleanupManifest` | 清理计划持有候选记录的 `Arc` 与清单摘要 | 分页、准备、执行均消费内存记录；每次仍重新加载表并核对 UUID |
| 重写写会话 | 从冻结组生成 writer 分支、删除目标与提交替换集合 | 开启时核对目标 ref 的 head 等于起始快照；不重新选文件或切组 |
| BE 执行 | 消费 FE 签发的执行句柄、split 与 writer 配方 | 私有 wire 只传冻结输入的身份，不传冻结内容，也不读取维护计划对象 |

`ConnectorProcessLocalPayload` 只携带 provider binding 与不透明的共享值，没有持久化或 wire codec。
`IcebergRewritePositionDeleteFilesHandle` 在 FE 保留冻结内容，wire 解码后的 BE 句柄不含它。
这些值的相等、摘要与 Debug 只解释身份，不比较共享指针地址，也不展开冻结内容。
元数据维护的 terminal 表可以保留提交结果，但不持有冻结输入。

重写的计划顺序与 writer 分支顺序是正确性事实。冻结文件组按 SPI cohort ID 的顺序排列，
第 i 个 writer 必须写第 i 个 cohort 的组；仅比较数量不能证明它们对应。
写会话在 `admit` 校验起始快照后，由 `rewrite_branches_from_frozen` 生成分支；
FE 的 `try_begin` 再逐个核对目标序号与组摘要。

## 考虑过的选项

1. **在表目录保存私有冻结产物（设计否决）。** 可以把大清单放在 StateStore 之外，但 Iceberg 不引用这些对象，
   外部清理器可以删除它们；当前产品又没有跨 FE 重启的维护恢复契约，持久副本不能成为恢复权威。
2. **另设对象存储命名空间保存产物（设计否决）。** 能避开表目录清理，但仍引入只服务于本次尝试的持久副本、
   回读、生命周期与凭证责任。进程运行态不要求这一条持久路径，也不由它取得恢复能力。
3. **在 provider generation 缓存冻结计划（设计否决）。** 容易按操作 ID 重放，但 generation 的寿命长于尝试，
   会让已结束作业的清单继续驻留；重放旧计划也不能替代针对当前 Catalog 状态的新尝试。
4. **读取计划与写会话各自选文件、只核对组数（设计否决）。** 减少计划传递，却无法证明读取的数据就是提交替换的数据；
   并发提交可以在两次选择之间改变文件集合，组数相同仍不能证明选择相同。
5. **尝试内共享冻结内容，读、写与提交采用同一份计划（采用）。** 冻结内容随最后一个尝试内持有者丢弃而释放，
   去掉私有对象读写；外部并发仍由起始快照校验与 Catalog 提交依赖处理。
6. **目标 ref 变化后重新验证依赖并在新 parent 上准备（待评估）。** 可以减少并发造成的计算浪费，
   但必须独立定义新 parent 的依赖验证与准备契约；本裁决只承诺拒绝变化，不自动改变被冻结的替换集合。

## 裁决

1. **尝试内存规则。** 三类维护输入只存在于当前 FE 尝试的内存。不得写维护计划、候选清单、分片或批次回执到对象存储，
   不进 StateStore，不建立跨重启恢复入口。内容不得被 generation 缓存、作业历史或诊断持有。
2. **实际退出规则。** FE 会话与 cohort 查询派生值可以共享冻结内容；释放由最后一个持有者实际丢弃决定。
   无事可做、成功、确定未提交、结果未知、执行错误与收尾失败都必须丢弃本次会话。
   取消或超时不能替代查询、writer 与 I/O 的实际退出；遵守 ADR-0159 与 ADR-0171 的退出和产物 owner 边界。
3. **身份与失败关闭规则。** 重写身份为操作 ID 加内容摘要，组再带组摘要；编码带领域分隔。
   清理计划按清单摘要绑定冻结记录。缺失、归属或类型不符、摘要不符、组不存在时，在派发、删除或提交前报错。
   现有表 UUID、目标 ref、起始快照、schema 与分区规格校验保留；内容不上 native wire，BE 只解释身份。
4. **单一选择规则。** 重写只规划一次。cohort 的读取集合、writer 的有序分支与提交的替换集合都来自该计划。
   开启时先验证目标 ref 的 head 等于起始快照 S0，再生成任何分支；位置删除所需统计从 S0 读取。
   FE 逐分支核对数量、序号与组摘要。提交保留 `RefUnchanged` 依赖；执行期间的并发提交按冲突失败，
   不把旧数据写回，也不在新 parent 上重新选文件。
5. **有界内容规则。** 保留重写单组 1 MiB、4096 组、总量 64 MiB，以及清理 262,144 条记录、总量 64 MiB 的上限。
   单组按原单组分片的编码形状显式检查。规范编码只用于摘要与大小校验；移除分片不放宽内容上限，也不引入新的进程门限。
6. **结果与物理对象分离规则。** 冻结输入随尝试结束释放，不授予删除实际 writer 产物的权限。
   `CommitUnknown` 后仍遵守 ADR-0110：不重试、不中止、不清理、不自动对账。
   真实输出的 crash 残留由已有安全年龄窗 GC 处理。

本条部分替代 ADR-0028 的持久 operation/plan、`Unresolved`、自动 reconcile、表根私有产物、
SPI/StateStore 仅保存持久句柄、64 个 1 MiB 分片及终态七天回收条款。
ADR-0028 的 FE-only capability、准确 generation、`REWRITE MANIFESTS` snapshot marker 与
`EXPIRE SNAPSHOTS` 原子 marker 条款继续有效。ADR-0110、ADR-0111 与 ADR-0133 的上位边界不变。

## 接受的妥协（诚实记录）

冻结内容会一直驻留到本次尝试及其派生查询实际退出；单次内容有上限，多次并发尝试的总量不由这些上限约束。
选择驻留是因为执行需要同一份输入，而产品没有持久恢复消费者。容量准入与内存归属仍属于各自 owner，
本裁决不以新的全局门限掩盖那部分责任。

FE 重启后无法继续旧维护作业，长任务需要从 Catalog 当前状态重新开始；已写出的真实数据文件可能等待 GC。
共享内存并没有提高恢复能力，只去掉了当前恢复模型不消费的持久副本。

目标 ref 在规划后改变就拒绝或在提交时冲突，允许的并发也可能让已完成计算白跑。
接受这个成本，是为了保证读取与替换同源；在新 parent 上验证与准备尚不属于本裁决。

## 何时重新评估

- 产品正式要求跨 FE 重启继续维护作业，并已定义符合 ADR-0110 的外部恢复与副作用权限协议。
- 真实工作负载持续触及冻结内容上限，或多尝试驻留造成容量准入压力；先评估冻结组定义、分批与内存 owner，不能静默放宽上限。
- 需要把冻结内容交给另一个进程消费；必须先定义有明确生命周期和权限的传递契约，不能恢复一个共享查表缓存。
- 目标 ref 的合法并发导致不可接受的重复计算，且已提出保持起始选择不变的新 parent 依赖验证与重新准备模型；届时评估选项 6。
