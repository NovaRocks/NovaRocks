---
id: ADR-0082
title: "Full-coverage statistics outrank incremental ones on the same snapshot"
domain: [connector-spi]
status: superseded
supersedes: []
superseded-by: ADR-0135
date: 2026-08-17
provenance:
  - "PR: pending (ancestor statistics reads and measured-snapshot publication)"
  - "discussion: 2026-08-15 统计新鲜度语义与 FE durable 记录边界"
code-anchors:
  - "novarocks/connector/iceberg/src/commit/statistics.rs (commit_statistics_file)"
  - "novarocks/connector/iceberg/src/stats_assembler.rs (StatisticsCoverageMark)"
---

## 问题

同一个快照上有两个统计写入者（ANALYZE 的全量扫描，和写入路径的增量并集），
而 Iceberg 一个快照只保留一份统计文件、`set_statistics` 是替换语义。谁应该赢？
提交冲突后重试时，还能沿用第一次的判断吗？

## 背景与执行事实

NovaRocks 的 collect-on-write **早已存在并默认运行**：`commit/write_io.rs` 算 sketch，
`commit/fast_append.rs` 的 `register_puffin_stats` 做第二次 metadata-only 提交，
`stats_assembler.rs` 把父快照 sketch 与新数据并集。它产出的是**增量并集**，
覆盖范围取决于父快照有没有统计。

ANALYZE 产出的是**全部可见行扫描**的结果。

改动前二者：

- 使用不同路径方案（`snap-{id}-statistics.puffin` 固定路径 vs `snap-{id}-statistics-{operation}.puffin`）；
- 提交都是一次 `update_statistics().set_statistics(...)`，**无重试、无冲突后重新判定**；
- 已登记的条目**无法判别**是全覆盖还是增量。

一旦发布不再要求目标快照是 current（`ADR-0081`），两个写入者瞄准同一快照的窗口显著变宽，
last-writer-wins 会让一次全表扫描的结果被一次增量并集覆盖掉。

## 考虑过的选项

**A. Last-writer-wins。**
零成本，且 Iceberg 本身就是这个语义。但它允许信息量少的结果替换信息量多的结果，
而两者的差距不是「新旧」而是「是否见过全部行」。

**B. 先到者胜（已存在即不替换）。**
避免了覆盖，但也让重新 ANALYZE 无法更新统计——把陈旧固化了。

**C. 按覆盖度排序，同级后到者胜（本裁决）。**
需要一个可判别的标记，以及冲突后重新判定。

**D. 允许同一快照并存多份统计，读侧合并。**
Iceberg 元数据模型不支持（一个快照一份），需要自建旁路索引，成本远高于收益。

## 裁决

采用 **C**。

1. **可判别性是前提**：每份统计文件在其 Puffin blob 的 `properties` 上带
   `novarocks.statistics.coverage`（`all-visible-rows` / `incremental-union`）。
   没有该属性的条目一律读作增量——保守方向是让本侧让步。

2. **覆盖度优先**：全覆盖结果优先于增量并集。增量写入遇到同快照上已有全覆盖条目时**放弃登记**，
   并记为一次正常跳过（`debug`），不是失败。

3. **同级后到者胜**：两个全覆盖之间、或两个增量之间，后提交者替换先前条目。
   二者 basis 相同、行集合相同，较新的采集只是更新的 evidence。

4. **重试必须重新判定**：提交冲突后重新加载表元数据并重新执行第 2、3 条，
   **不得** last-writer-wins 盲写。这是本 ADR 的承重条款——盲重试恰好会制造它要防的那次覆盖。

5. **路径带尝试标识**：增量登记不再使用仅由 snapshot id 决定的固定路径。
   两个并发尝试会向同一对象写出不同字节，而竞争失败方可能仍在读它。
   未被引用的文件交由既有 orphan 清理。

## 接受的妥协（诚实记录）

1. **标记落在 Puffin blob 属性上，是 NovaRocks 私有约定。** 其他引擎写的统计文件没有它，
   一律被读作增量，因此本侧永远让步。这在互操作场景下偏保守——Spark 写的全量统计会被我们的增量替换。
   接受它是因为反过来（默认读作全覆盖）会让我们错误地放弃自己的全量结果，那个方向的错更糟。

2. **仲裁只在提交点生效，不是分布式锁。** 两个全覆盖写入者仍可能互相替换，
   只是不会出现「增量盖掉全量」。这不是线性一致的发布顺序，只是一个偏序。

3. **重试上界 5 次是个数字，不是从并发模型推出来的。** 统计提交是 metadata-only、
   重做便宜，但也永远不值得阻塞——它描述的数据早已发布。超过上界即放弃，只留日志。

4. **本 ADR 只裁决仲裁，不修 collect-on-write 的已知缺陷。** 父快照无 Puffin 时它会把仅覆盖新增数据的
   sketch 当作整表 NDV 发布——那份结果会被标成 `incremental-union` 从而不会覆盖全量，但它自己仍然是错的。
   修复属于独立工作；本裁决不应被读作「增量路径已经正确」。

## 何时重新评估

- **出现第三类统计写入者时**（例如采样、后台自动采集）：二元覆盖度标记将不够，
  需要真正的偏序而不是两档。
- **跨引擎互操作变重要时**：需要一个标准化的覆盖度表达，或至少一条「信任外部全量统计」的显式规则，
  而不是当前的一律让步。
- **若实测出现频繁的 5 次重试耗尽**：说明同快照并发写入远比预期密集，
  那时要重新审视是否该让增量路径在检测到活跃 ANALYZE 时直接不写。
- **collect-on-write 的覆盖度缺陷修复后**：`incremental-union` 的语义会变强（真正的整表并集），
  届时第 2 条的优先级是否仍然成立需要重新确认。
