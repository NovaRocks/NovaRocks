---
id: ADR-0274
title: "Window setup precedes tracked batch output"
domain: [sql-compiler, pipeline-execution]
status: active
supersedes: []
superseded-by: null
date: 2026-11-26
provenance:
  - "Discussion: 2026-11-26, window aggregate admission and batch output"
code-anchors:
  - "novarocks/functions/src/window_kernel.rs (PreparedWindowKernel, WindowKernelPartition)"
  - "novarocks/functions/src/builtin/aggregate_window_adapter_tracked.rs"
  - "novarocks/execution/src/exec/operators/compiled_window_batch.rs (WindowBatchCursor)"
---

## 问题

窗口调用怎样在保留帧语义和必需错误顺序的同时，按实际输出预算发射变长结果？

## 背景与执行事实

| 实体 | 职责 |
| --- | --- |
| SQL 与 `WindowCallSpec` | SQL 按已选重载核对普通 OVER、DISTINCT 和函数排序的资格。封装保存逻辑实参、独立的函数排序根、排序方向、NULL 顺序、Single 聚合的状态解释，以及 OVER 的根与帧。LocalCompiler 核对同一绑定和根使用点。 |
| Functions | 原聚合作者完成分区 setup，按物理帧保留紧凑的受检事实和真实输入。分区保管当前可重复发射的状态，暴露选定行的结果几何。 |
| Execution | 原算子保管源批次和分区续体。宿主按 pipeline 行目标及列的真实偏移形态选取可发射前缀。实际写出使用同一查询 allocator。 |

OVER 的排序决定帧。函数内排序决定送给聚合的帧内次序。两套根分别求值，并按原调用的 Value 使用点和效果校验。

## 考虑过的选项

- 整分区完成全部结果后交付切片。它保持首错次序，但累计变长结果会把整列的逻辑体积与偏移上限同时变成前置容量条件。设计否决。
- 第一次请求结果时才更新和检查该批的帧。它缩短首批延迟，却会使是否观察到晚帧错误依赖下游需求。设计否决。
- 支持任意滑动帧撤回和 spill。它可能改善大的滑动帧和长分区，成本取决于各重载的重现性和合并契约。待评估。

## 裁决

1. **从原调用签发窗口资格。** Window 生命周期携带修饰词事实。聚合的 DISTINCT 键采用逻辑实参的规范身份和碰撞复核，先保留当前帧第一次出现的实际值，再按已绑定的函数键排序。DISTINCT 的独立函数排序键在 SQL 分析时返回 `EXPRESSION_NOT_IN_DISTINCT`。
2. **在需求之前完成必需工作。** 确定的帧错误与单值布局拒绝由 setup 完成。普通窗口在同一次输入关闭的分区组完成必需工作后发首批。与 BY 混用时按真实调用次序完成原 BY 校验和分区 setup。空选区或稀疏选区仍观察到原来的必需错误。
3. **按实际几何和容量发射。** 使用原 `EngineOutputPolicy` 行目标和可达 Arrow i32 偏移选择最长前缀。大偏移与固定宽度载体沿自身布局。构造前向原 allocator 申请真实用量，来源由续体和实际结果别名分担保管。重复帧或确定的前缀扩展可以复用状态，其余帧按原聚合规则重建。
4. **同一逻辑类型决定偏移两源。** 三参 `lag/lead` 消费定型后的共同 T。存在目标时取目标行的值，包含有效的 SQL NULL。越界时取当前行的默认值。两源使用受检的行选择和同一发射预算。

## 接受的妥协（诚实记录）

必需工作会推迟大分区的首批交付。分区保管输入和所需索引，逐批减少变长结果的同时驻留。滑动帧缺少撤回时可能重复计算。LISTAGG 的计量与实际发射共用选行和截断规则，允许在两次工作中复用相同帧结果。

## 何时重新评估

实际 workload 中滑动帧重建或长分区输入驻留成为主导成本时，评估支持确定重放的删除算法、索引或有界 spill。改变首批交付的失败时刻须先确认用户可观察的输出与错误合同。
