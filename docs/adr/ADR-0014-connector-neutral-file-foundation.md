---
id: ADR-0014
title: "Connector-neutral file access and columnar decoding foundation"
domain: [provider-spi, filesystem]
status: active
supersedes: []
superseded-by: null
partially-superseded-by: [ADR-0245]
date: 2026-07-29
provenance:
  - "discussion: 2026-07-29 connector read and file foundation separation"
code-anchors:
  - "novarocks/fs/src/lib.rs (crate boundary)"
  - "novarocks/fs/src/read.rs (FileReadRequest, ReaderSizing)"
---

## 问题

多个 table-format Connector 共享的文件访问与 Parquet/ORC 物理解码，是否应拥有 Connector identity，以及应由哪个依赖层拥有？

## 背景与执行事实

Iceberg、未来的 Paimon/Hudi 都需要 local、HDFS、S3、OSS 文件访问和 Parquet/ORC 解码。历史实现把这些能力放在 core，并以 `hdfs` Connector identity 作为 Iceberg 物理执行 owner；这使 Connector 无法在不依赖 core 私有类型的情况下迁出。文件访问本身不知道 catalog、table、snapshot 或 manifest，也不能完成 position/equality delete、deletion vector、schema evolution 等 table-format correctness。

## 考虑过的选项

1. 保留 HDFS Connector 作为共享 reader。实现改动小，但把存储 transport 错当成 table/catalog provider，继续迫使 Iceberg 降低为另一个 identity。
2. 在 core 内建立共享 facade，再逐步迁出。短期切分较容易，但会形成双 owner、长期 facade 或复制 reader，无法用 Cargo DAG 证明 Connector 独立性。
3. 建立无 Connector identity 的独立文件基础 crate，并让生产 consumer 原子切换。迁移面较大，但 owner、contract 与依赖方向可由 Cargo 直接约束。

## 裁决

采用选项 3。`novarocks-fs` 拥有受绑定的文件访问、range/cache、Parquet/ORC 物理解码、物理 pruning DTO、cancellation/deadline 观察和中立指标。它不注册 Connector provider，不依赖 core 或 SPI，不拥有任何 table-format correctness。原批预算归属由 [ADR-0248](ADR-0248-parquet-reader-sizing-and-scan-output-policy.md) 部分替代。FS 确定 reader 打开时的批粒度，最终输出前缀由实际 producer 按引擎政策组装。

Connector 负责把启动期 credential/config 绑定为进程内 access handle，并在其自身 reader 内完成 snapshot、delete/DV、schema evolution 和 virtual column correctness。Core 只消费 Connector 已完成 correctness 的 `RecordBatch`，并负责 SQL execution semantics。

## 接受的妥协（诚实记录）

首个迁移 PR 仍暂时保留历史 HDFS provider 和 core correctness adapter，以避免把 native identity/delete-DV 语义重写与物理 reader 迁移混为一个不可审查变更。该过渡只允许保留 Connector adapter，不允许 core facade、双 reader 或 foundation 内的 HDFS Connector identity。

Core 也会继续因为 writer 和 StarRocks format 直接依赖 Parquet/OpenDAL；因此“core 不依赖这些第三方库”不是本决策的目标。唯一性约束是通用 Parquet/ORC 生产 reader 只由 `novarocks-fs` 拥有。

## 何时重新评估

- Arrow/Parquet/ORC 依赖需要拆成多个可独立版本演进的基础 crate；
- 新格式无法在不引入 table-format 语义的情况下复用当前 reader contract；
- async-only execution 成为全系统约束，需要把同步 `FileBatchReader` 改为 async contract；
- cache 生命周期需要跨进程共享，而不再是单 BE 进程内资源。
