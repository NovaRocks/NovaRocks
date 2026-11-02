---
id: ADR-0027
title: "SQL owns runtime-filter planning and roles exchange wire facts"
domain: runtime-filter
status: active
supersedes: []
superseded-by: null
partially-superseded-by: [ADR-0209]
date: 2026-08-02
provenance:
  - "discussion: 2026-08-02 SQL runtime-filter planning ownership"
code-anchors:
  - "novarocks/core/src/sql/planner/runtime_filter/mod.rs (SQL-private planning closure)"
  - "novarocks/core/src/query_execution/preparation/runtime_filter_view.rs (sealed borrow-only handoff)"
  - "novarocks/frontend/src/runtime_filter/compiler.rs (Frontend deployment projection)"
  - "novarocks/backend/src/runtime_filter/install_decode.rs (Backend semantic install decode)"
---

## 问题

Runtime Filter 的候选生成、图验证、activation 决策和等待证明应由 SQL planner 还是运行时部署层共同拥有？

## 背景与执行事实

Runtime Filter 的 graph、activation、join build frontier 与等待环是 SQL 物理计划和 fragment 拓扑的派生事实。它们必须在 `DistributedPlan` seal 前完成，并与 explain、节点 binding ID 和 planner validation 使用同一份不可变事实。

运行时部署和 Backend 则只需要已封存的 channel、binding、coverage、role、progress 与 placement，以生成安装视图、路由 shard 和 native wire 数据。让运行时模块持有可编辑的规划图会使 SQL 与执行侧同时成为语义 owner，并允许从安装或 wire 数据反推规划状态。

## 考虑过的选项

1. 保留 runtime-filter model 作为 SQL 和运行时共享 graph。调用点少，但规划状态跨越 FE/BE 角色，封存后的唯一来源无法成立。
2. 新建共享 runtime-filter planning crate。表面上消除复制类型，但会把 SQL planner 与 Backend execution 绑定到同一语义 surface，继续模糊角色边界。
3. SQL 私有地拥有规划闭包；封存后通过显式的一向投影生成 deployment/install 与 wire 所需值。运行时保留自己的 port 和安装事实，不重建 graph。

## 裁决

选择选项 3。SQL planner 私有拥有 Runtime Filter 的 contract、coverage、graph、activation、progress、wait graph、validation 和 sealed carrier。`DistributedPlan` 与 `PreparedFragmentSet` 共享 sealed carrier 的不可变 handle。

部署编译器只消费 sealed SQL facts，并在 `planning_adapter` 中逐字段映射为 runtime/install values；native plan encoder 从 prepared SQL binding facts 产生既有 wire schema。Backend 只通过 native wire 接收执行事实，既不依赖 SQL graph，也不从 install 或 wire 数据重建规划图。

## 接受的妥协（诚实记录）

SQL 与 runtime/install 层保留同宽但不同的 value 类型，投影需要显式穷举 match，短期增加了代码和测试夹具的维护成本。选择它是为了让 semantic ownership、封存边界和进程角色可证明，而不是因为复制类型本身更优。

部署编译仍位于 core，以便 FE 协调代码复用纯 placement kernel；它因此显式依赖 SQL sealed facts。这个依赖是单向的，不能反转为 Backend 对 SQL planner 的依赖。

## 何时重新评估

- 若 native wire 协议需要承载新的规划语义，先裁决该事实是否应在 seal 前冻结，再增加一向 wire projection。
- 若 deployment compiler 被拆到 frontend crate，保持输入为 sealed SQL facts 或 wire DTO，不能迁回共享 graph。
- 若新的执行端需要本地 policy 或 artifact state，新增 runtime port value；不得把 SQL graph 暴露给 Backend。
