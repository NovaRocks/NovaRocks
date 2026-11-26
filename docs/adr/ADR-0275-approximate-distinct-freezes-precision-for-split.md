---
id: ADR-0275
title: "Approximate distinct freezes precision for split execution"
domain: [sql-compiler]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0259
    scope: "Item 2 on default-p11-only ordinary approximate distinct exchange state."
    link: "ADR-0259-trino-aggregate-state-and-published-semantics.md"
date: 2026-11-26
provenance:
  - "Discussion: 2026-11-26, approximate distinct argument and merge semantics"
code-anchors:
  - "novarocks/functions/src/builtin/approx_distinct_kernel.rs (fixed_distinct_precision)"
  - "novarocks/functions/src/builtin/approx_distinct_sketch.rs (Sketch)"
  - "novarocks/sql/src/agg_mergeability.rs (scalar_aggregate_mergeability)"
---

## 问题

近似去重接受逐行误差实参时，怎样保证单阶段和分阶段聚合使用相同精度与状态语义？

## 背景与执行事实

通用 `approx_distinct` 的第一参按完整逻辑类型绑定 Trino 哈希配方。默认误差 `0.023` 对应 p11。显式误差的闭区间为 `[0.0040625, 0.26]`，首个 FILTER 入选且两参均非 NULL 的行决定精度。BOOLEAN 用出现位，UNKNOWN 返回零。两者仍按实际精度参数的空值条件选行。

| 实体 | 职责 |
| --- | --- |
| Functions | 从已定型 DOUBLE 常量签发固定 p，准备实际哈希域、参数化 HLL、更新、发射与合并。 |
| SQL 拆分规则 | 只在原语句具备两阶段资格时引入 Partial 和 Final，保留原逻辑实参与 FILTER 的实际行域。 |
| BE 准备和 driver | 从原封装请求复核资格。本地拆分消费同一已准备资格和状态契约。 |

## 考虑过的选项

- 每个工作者按自己的首贡献行建草图。动态参数会让结果依赖分片顺序，且状态可能具有不同 p。设计否决。
- 让单参和双参沿各自的 HLL 作者和状态版本推进。显式默认精度与默认调用易因算法或晋升变化产生差异。设计否决。
- 对动态参数先作全局同步以确定首贡献。需要独立定义分布式行顺序并扩大排序、通讯和资源成本。待评估。

## 裁决

1. **用原常量证明固定精度。** 通用单参使用 p11。显式精度只有已定型、非 NULL、范围合法的 DOUBLE 常量授予拆分资格。动态、NULL 或无效参数沿 Single 的实际首贡献规则处理，保留原实参错误与空态结果。
2. **共享状态与确定的稀疏晋升。** 单参和双参通用重载使用同一 HLL 算法及状态格式。晋升按精度与哈希前缀内容决定，独立于本地分片、合并顺序和分配余量。精度与估计采用 Airlift 451 的对应规则。空贡献是合并的中性元素。
3. **在交换时复验来源。** 版本、表示、p 和准确哈希配方随状态交换，接收者核对原重载和 ExactSignature。旧普通 p11 状态与新格式通过原生兼容岛切换。Iceberg IMV 的持久资格由持久聚合作者签发。

## 接受的妥协（诚实记录）

动态参数保留单阶段成本。内容驱动的晋升以固定输入对照 Airlift 的单节点行为，某些转换边缘可能受对方的分配布局影响。记录复现差异时保留固定输入和两端原始结果。相同固定 p 的本地 Single 和分片合并必须得到相同结果。

## 何时重新评估

参考版本改变公开哈希、参数语义或估计方法时复核。动态参数是主要生产形态且产品确认可用的全局行顺序时，评估确定精度的分布式准备协议。新增可持久读取的用途时，单独审查原持久作者和格式演进。
