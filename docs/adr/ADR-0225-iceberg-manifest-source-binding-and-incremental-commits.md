---
id: ADR-0225
title: "Iceberg commits preserve manifest sources and bind entry facts at observation"
domain: [connector, iceberg]
status: active
supersedes: []
superseded-by: null
date: 2026-11-10
provenance:
  - "discussion: 2026-11-10 manifest source views, commit merging, and historical bounds encoding"
code-anchors:
  - "novarocks/connector/iceberg/src/manifest/source.rs (ManifestSource)"
  - "novarocks/connector/iceberg/src/commit/manifest_transform.rs (ordinary_manifests)"
  - "novarocks/connector/iceberg/src/commit/manifest_encoding.rs (ManifestWriteEncoding)"
---

## 问题

Iceberg 提交如何保留文件的历史身份，在只触及少量条目时复用已有 manifest，并让新增 manifest 的数量受标准布局政策管理？

## 背景与执行事实

| 对象 | 解释与责任 |
|---|---|
| 原始 manifest | SDK 解码出物理条目和原 header。条目可能省略 sequence 或 first row ID，原件可以在有效读域内共享。 |
| 来源视图 | 准确 list 与完整 descriptor 决定当前已提交解释。当前步骤尚未写入 list 的 manifest 使用待分配解释。两者均保留原始序位。 |
| 准备输入 | 目标位置查询、身份查找、完整存活观察和历史窗口各自证明自己的范围。选择结果只保存所需身份、冻结事实及 locator。 |
| 布局与发布 | FE 原提交准备按来源过滤，按 operation 捕获的标准属性装箱，在原账登记对象，由 SDK 写最终 list。成功 stage 后下一步骤读取真实已完成的 list。派发与清理继续沿 ADR-0171、ADR-0179。 |

## 考虑过的选项

- **全表展开后重新分组（成本否决）。** 处理路径直接，但小改动会反复解码、跨源保留完整文件及重写无关来源。全覆盖操作仍可显式观察全集。
- **过滤后直接追加（成本否决）。** 写入触及的来源较少，持续小提交会把成本移给 manifest-list 和规划。标准合并处理选中的箱，并保留其余来源。
- **按对象路径缓存绑定条目（设计否决）。** 继承的 snapshot、sequence 和 row ID 受 descriptor 与绑定语境影响，同一物理对象可在不同 list 中有不同结果。
- **自建流式 Avro codec（待评估）。** 可以降低单来源解码峰值，需要增加格式解释责任并与 SDK 保持一致。当前使用 SDK 的单对象边界。

## 裁决

- **按物理序位继承。** 在选择前解析条目。显式 row ID 不推进 cursor，DELETED 不占用存活数据范围。已提交来源缺 manifest row-ID 起点时，绑定结果为 null。待提交来源可保留显式值。
- **按观察范围证明。** T 完成绑定准确 list、目标及已处理的候选来源。位置删除与 DV 共用目标 spec、typed partition 和 sequence 规则。物理新增历史沿完整祖先窗口识别 ADDED，包含 REPLACE。完整存活观察和局部查询各自保留自己的证据。
- **先过滤，再标准合并。** 未选来源保留引用。普通准备对 data/delete 候选按 spec 和真实长度从尾装箱，首个候选所在箱应用 min-count。REWRITE MANIFESTS 与零文件槽操作沿自身布局。普通快照按最终列表统计新建、保留和替换的 manifest。替换只统计确实被过滤或合并的旧来源，旧删除清单自然退出及中间写出单独看待。取消或首错后由原 FileIO 窗口排空已开始工作。
- **由真实历史 schema 写出。** 核验原条目身份和原存储事实后，仅在写出副本上规范化分区值和必要的 bounds。字段 ID、界的方向和数值语义保留。v3 DATE 到 TIMESTAMP 或 TIMESTAMP_NS 分别按微秒或纳秒受检换算，输出可由同一 SDK 读取。
- **用实际 list 推进 prefix。** SDK 的成功写出决定序列和行范围。原 staging 成功后才能向下一步骤公开这些 descriptor。外部提交结果及产物处理沿原生命周期。

## 接受的妥协（诚实记录）

- SDK 仍整份解码一个 manifest/list，也整份写出一个合并箱。并发窗口约束在途工作，无法给出 SDK 深层内存上界。
- 外部 timestamp header 若含合法的四字节 DATE bounds，当前 SDK 无法完成解码。修复 Nova 写出的 manifest 只覆盖可以按原 header 读到的输入。若没有真实 schema 能解释所有已删除字段的可选 bounds，已知字节仍随本次写出保留，SDK 后续重读可能忽略缺字段定义的历史 bounds。
- 旧删除输入限制及成功尝试的保守产物保留分别由原 owner 继续处理。按源布局没有转移 Catalog 提交权或授权提前回收。

## 何时重新评估

- 上游 SDK 支持正确解码历史 DATE bounds、按类型和溢出规则完成升级时，重新验收外部混合输入。见 [Iceberg Rust #3387](https://github.com/apache/iceberg-rust/issues/3387)。
- 上游支持流式读取或实际观察表明单对象成本需要独立治理时，重新评估单来源和单箱物化。
- 产物 owner 能在准确来源下证明稳定步骤、完整引用和回收时，重新评估跨轮复用及未发布中间产物的退休。
