---
id: ADR-0276
title: "Plan freezing uses frontend planning memory"
domain: [memory-governance, sql-compiler]
status: active
supersedes: []
superseded-by: null
date: 2026-11-27
provenance:
  - "2026-11-27 design discussion on frozen fragment memory ownership"
code-anchors:
  - "novarocks/frontend-application/src/native/fragment_encoder/frozen.rs (freeze_package)"
  - "novarocks/frontend-application/src/query_execution/package_freeze.rs (encode_freeze_buffer)"
  - "novarocks/frontend-application/src/metrics/task_creation.rs (statement_static_fragments_frozen)"
---

## 问题

FE 用哪个资源域构造并保留 Native 静态片段。常量折叠能把短 SQL 扩成大计划，旧作者从语句结果窗口申请包缓冲，导致冻结是否成功取决于窗口大小和当时已保留的结果数据。

## 背景与执行事实

FE 从受检物理包冻结片段，并由同一 backing 支持 Task 创建和精确重放。计划模板、待发 Task、在途请求各自可能持有字节别名。结果窗口约束结果数据。必需常量求值和系统表快照构造已经使用原语句的构造授权。

## 考虑过的选项

- **设计否决。** 继续从结果窗口申请并调大预算会让计划与结果争用容量，并扩大结果持有者的缓冲。对单个大值的硬拒绝仍取决于结果类别。
- **设计否决。** 为冻结另设每语句计划字节限额。Native 单报文上限约束单条请求，无法作为整条语句计划总量的依据。
- **待评估。** 在进程级权威下给 FE 语句设完整内存账户。当前 FE 规划侧依赖进程余量与结构界，完整 allocator 治理按下面的条件统一重评。

## 裁决

普通读和 Internal 语句的静态片段冻结使用 FE 规划侧内存。冻结前核对原语句 Root，创建作用域保留原编译控制检查点。内外层整段编码缓冲先可失败预留，失败使用现有 typed 资源错误。接收方结构预检、Native 报文上限与 BE Task 准备资源各自在原边界生效。

每次完整冻结记录全部片段的字节分布。片段保留 gauge 与 backing 同寿命，最后字节别名退出时回落。两个指标只观测。必需常量和系统表快照的原构造授权继续生效。

## 接受的妥协

FE 没有整条语句的计划字节包络。折叠放大的值、同一语句同时持有的多个片段和每次 Task 发送的传输副本会消耗进程余量。两层整段缓冲的可失败预留覆盖各自分配，其他规划对象和传输库内部仍依赖现有结构界和进程余量。

## 何时重新评估

结构界无法可靠建立，或结合片段保留 gauge 和按语句分布仍出现持续不可解释的 FE 内存增长时，重评 FE 规划侧治理。届时一并核对规划对象、冻结产物及传输副本的实际 owner 与退出时点。
