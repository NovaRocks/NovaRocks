---
id: ADR-0246
title: "Trino public functions and internal aggregate identities"
domain: [sql-compiler]
status: active
supersedes: []
superseded-by: null
date: 2026-11-21
provenance:
  - "discussion: 2026-11-20 Trino 484 function catalogue and retained compatibility exceptions"
code-anchors:
  - "novarocks/sql/src/functions/mod.rs (public_aggregate_target, resolve_sql_aggregate_call)"
  - "novarocks/functions/src/aggregate_types.rs (DistinctAggregate, resolve_distinct_aggregate)"
  - "novarocks/sql/src/analyzer/resolve_expr.rs (analyze_function_from_metadata)"
  - "novarocks/sql/src/planner/distributed/build/contract_lowering.rs (aggregate lowering)"
---

## 问题

公开 SQL 应如何发现和绑定函数，才能遵循 Trino 的调用契约，同时让规划器和执行器精确保留内部聚合的身份与状态？

## 背景与执行事实

| 作者 | 职责 |
|---|---|
| Parser 和 SQL | 解释函数语法和修饰词。普通聚合与窗口分流通过公开目录识别聚合，对未知调用报告 `sql.analyze.unknown_function` |
| Functions | 定义公开可见性、重载、结果与中间状态。标准的 `COUNT`、`SUM`、`AVG` 带 DISTINCT 时，通过类型化入口选择内部身份 |
| 编译计划与本地编译器 | 消费原选中的函数身份、重载、值实参、排序通道和状态格式，按现有阶段执行 |
| Trino HTTP | 将分析期未知函数投影为 `FUNCTION_NOT_FOUND` |

公开目录的对照基准是 Trino 484 core。独立插件提供的函数使用插件自身的契约。开发环境可以用同版镜像确认实际加载的目录和执行预期。

## 考虑过的选项

1. **把内部名字保留为公开别名。设计否决。** 用户可以绕过标准聚合修饰词直接选内部签名，旧名字和状态算法会变成外部接口。
2. **退役独有调用，同时统一改写全部 DISTINCT 聚合。待评估。** 通用分组改写可扩大组合空间，需要独立核对元组 NULL、类型、内存和原生部分／合并行为。
3. **独立演进公开目录与内部身份。采用。** SQL 根据原调用语义触发，Functions 从已有声明签发准确重载。计划继续携带绑定和中间状态。

## 裁决

1. **公开候选尊重目录可见性。** 普通和窗口调用先发现用户可见的候选。移除函数时同时收回别名、预转换、宏和独有 kernel，避免分析特设绕过未知函数拒绝。
2. **内部聚合按身份选择。** 已准入的 DISTINCT 修饰词与原实参共同驱动内部重载。零实参、排序通道和可用窗口形态由实际调用及原声明约束。部分聚合和合并继续消费相同状态格式。
3. **由保留消费者决定实现寿命。** 公开和内部生命周期独立。仍支撑 `ndv`、`approx_count_distinct`、`array_agg(DISTINCT ...)`、`percentile_approx`、`date` 或 Iceberg 的共享计算继续由原作者持有。
4. **退役时承接测试性质。** 保护公开拒绝的用例验证准确错误分类。混合语料沿原查询结果、类型、数据准备和生命周期验证保留的性质。生产拓扑使用同候选的 FE 与 BE，观察原控制和实际退出。

## 接受的妥协（诚实记录）

`sleep` 继续显式公开，生命周期验证使用它的可取消等待。`murmur_hash3_32` 与 `array_sortby` 在各自的字节签名和数组排序迁移中承接原语料后退役。现有内部 DISTINCT 状态保留，公开语义的后续对齐仍需单独验证。

## 何时重新评估

迁移保留的函数族时，核对真实签名、控制效应、测试装载与实际消费者。选择通用 DISTINCT 改写或更换内部状态时，先以原输入复验类型、NULL、重复、资源边界和分阶段行为。
