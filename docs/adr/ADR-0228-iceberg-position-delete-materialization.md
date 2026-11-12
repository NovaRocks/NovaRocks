---
id: ADR-0228
title: "Iceberg v3 materializes each file's position effects in one BE writer"
domain: [connector, iceberg, execution]
status: active
supersedes: []
superseded-by: null
date: 2026-11-11
provenance:
  - "RWM-6 approved spec v5 and plan v3 at 8f767eac6e2ade0eec3b1dc5215cc6f6b052a2f8"
code-anchors:
  - "novarocks/sql/src/planner/distributed/build/contract_lowering.rs (lower_change_stream_writers)"
  - "novarocks/execution/src/exec/operators/position_shard.rs (PositionShard)"
  - "novarocks/connector/iceberg/src/commit/write_stack/execution/materialization.rs (MaterializedDeleteWriter)"
  - "novarocks/connector/iceberg/src/commit/write_stack/control/publication/materialized.rs (MaterializedDeleteStep)"
  - "novarocks/connector/iceberg/src/fs_io/managed_create.rs (ManagedFileWrite)"
---

## 问题

v3 的 DELETE、MOR UPDATE/MERGE 和增量 MV 可能由多个贡献路线处理同一文件。每次变更如何按实际文件身份聚合位置，保留可见性、来源和对象归属，并发布唯一的最终效果？

## 裁决

- **固定变更输入。** Provider 把目标对象、ref、snapshot、原 manifest list 和解释 schema 固定在原请求里。定位行携带原文件路径、位置、历史 spec ID 和分区值。选择的行可见性、分区解释和新 artifact 都从这个目标导出。UPDATE 的后像照常按目标写入规则分区。
- **分布式聚合，单 writer 物化。** 原 Router 统计逻辑效果。所有同组贡献沿同一文件 hash 分布，在实例内再按同一文件键归到一个实际 Driver。汇总输出完整、连续的 Roaring container。最终 gather 使用 MustDrain，普通 Singleton DOP1 writer 以接收端认证的 Task 和调度冻结的实际 DOP 核验 Driver。原 Root 继续接收 writer results 和 Router counts。
- **逐文件输出。** BE 用本次新位置与旧位置计算并集及新增删除数。有适用 DV 时按 Iceberg 优先级消费 DV。完整覆盖可移除数据文件和它独占的旧位置项。其余目标写一个最终 Puffin blob，并保留共享 Parquet、其他 Puffin blob 和 equality delete 的引用。v2 继续分布式写出本次位置，历史分区随行进入新 manifest。
- **按作用域观察和发布。** BE 查询当前文件所需范围，FE 一次观察整步骤实际触及的目标并核对 locator、独占旧位置项和完成效果。原始 manifest 按已授权对象缓存，绑定结果仍按真实 list、完整 descriptor 和来源语境区分。提交重用 ADR-0225 的按源过滤与标准合并，Catalog 发布、拒绝、未知及清理继续由原 attempt 和接收责任持有。
- **受管对象条件创建。** ADR-0077 的归因原则扩展至 INSERT、CTAS 和受管数据、删除及 Puffin 输出。Local 完成私有文件后原子安装。S3 使用原签名、RetryLayer 和条件 PUT 或 CompleteMultipartUpload。结果不明确时，用冻结标记核验归属，再按实际版本或 ETag 流式核对长度和 SHA-256。上传退出沿原 HTTP 请求观察，只有完整对象及已证实退出才可封存。归属未证实时保留原对象，确认归属但内容不完整时由原 owner 处理。

## 接受的妥协

- 最终 writer 为单实例 DOP1。并行位置聚合降低网络与汇总成本，单文件位图和现有资源限额仍限制可处理范围。
- Java 可能在提交时合并重复 DV。NovaRocks 在同一目标收到多个最终效果时拒绝提交，唯一效果由物理计划和原 writer cohort 共同保证。
- SDK 仍整对象解码 manifest。缓存逐出后可重新读取。SDK 对外部混合历史 bounds 的限制沿 ADR-0225 记录。

## 何时重新评估

当实际负载证明单 writer 限制吞吐，评估按文件授权多个最终 writer。对可复用 partial 聚合、metadata DELETE 和大表少量目标成本，分别沿后续 RWM 项验证。
