---
id: ADR-0265
title: "MV publications bind exact source facts and storage mappings"
domain: [frontend-mv, provider-spi, sql-compiler]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0259
    scope: "Items 4 and 5 on published result semantics, legacy interpretation layouts and contract rebinding after an engine change."
    link: "ADR-0259-trino-aggregate-state-and-published-semantics.md"
  - id: ADR-0263
    scope: "MV publication eligibility and the definition format transition described under version evidence."
    link: "ADR-0263-trino-scalar-values-and-publication-eligibility.md"
date: 2026-11-24
provenance:
  - "RWM-10b Spec v4 and Plan v3, approved Obsidian snapshot f9d697ed6a63585aade579b9ee493907e93f3aec"
code-anchors:
  - "novarocks/spi/src/connector/source_type.rs (ProviderSourceType)"
  - "novarocks/sql/src/planning/mv_persistence.rs"
  - "novarocks/frontend-application/src/mv/domain/persistence/source_bindings.rs"
  - "novarocks/mv-application/src/persistence/keyed_documents.rs (build_keyed_versions)"
  - "novarocks/connector/iceberg/src/storage_inspector/storage_mapping.rs"
---

# ADR-0265 物化发布绑定准确源事实与存储映射

## 问题

刷新、直接读取和查询改写应当依据哪些长期事实识别物化结果，以及应当在什么时候全量重算？

## 背景与执行事实

定义 D2 保存有效 SQL、方言、原解析上下文与创建时刻。SQL 从完成分析的语句交出按顺序区分的关系读取、字段路径、视图依赖和输出事实。FE 沿原语句保留的 catalog 观察取得表身份、视图版本、类型与当前数据版本。同一个表的多个读取分别进入 B2，字段身份和类型取自原 Provider。

Iceberg 源类型使用版本化的结构表示，保留嵌套字段 ID、required 和原始类型参数。SQL 逻辑输出仍使用冻结的值类型。Provider 从实际建表或现表 schema，沿普通写入与读取作者签发 M1，说明物理字段、可空性、内容契约和推下资格。L3 表达实际物化行的解释，包括聚合状态、物理列、分支和目标布局。聚合调用以已选绑定和状态格式寻址。

B2 同时引用 D2、L3 和 M1。当前 P 从原快照记录引用 B2，水位按该 B2 的关系读取顺序解码。CREATE 和全量重建由原发布作者附上对应世代。配置槽与 StateStore Accelerator 沿既有 owner 保管。旧 D1、L2、B1 对象可在系统表发现，原管理闸门只授予经湖上身份核对的 DROP。

## 考虑过的选项

- 逐重载记录标量与聚合结果行为，部署升级时由目录维护语义代次。该方案能自动找出行为变化影响的结果，每次修复都需要同步维护发布记录及作者版本，当前按成本否决。
- 持久化整个计算的规范投影，利用精确等价跳过仅修改视图格式或注释后的重建。维护规范投影需要为所有表达式和查询构造定义跨版本身份，当前按成本否决。
- 持久化实际解析绑定、依赖、输出声明和物理解释，由现有刷新与发布任务观察并比较。改版后的用户值语义依赖运维全量重算。

## 裁决

刷新在原定义上下文中重新分析并比较绑定、依赖、输出、L 和 M。依赖或布局变化且输出可维持时，沿新捕获的表与快照全量重算。确定的解析或输出错误标记定义失效。暂时读失败和控制结论沿原错误返回。已有发布的标记作用于当前 P。首发前的标记作用于取得正面未发布证据的 B。成功发布同时移除已消化的标记。EXPLAIN 保持只读。

查询改写从当前 P 的 B 检查依赖、快照、标记和实际聚合绑定，使用原语句保留的源观察。缺少资格证据时继续基表计划。持久定义按默认计算语义分析和刷新，具有不同有效计算语义的查询跳过候选。

## 接受的妥协

项目接受由运维处置引擎升级带来的函数行为变化。D、L 和 B 退出逐调用的标量与聚合结果语义记录及计算身份 C。被引用视图即使只改动注释或格式也会形成新版本并触发重建。状态或布局推导变化仍由 L 和 M 发现。普通函数定型、聚合状态交换与 Native 兼容岛继续沿各自合同。

- 升级后的函数行为变化由版本说明传达。绑定、依赖和布局相同时刷新照常运行，历史行可能保留旧行为直到全量重算。
- 受影响对象需要全量重算。CREATE OR REPLACE MATERIALIZED VIEW 可用之前，按原管理入口执行 DROP、CREATE、REFRESH，随后核对新发布。
- 可按已知函数名搜索 `system.metadata.materialized_views.definition`。版本说明还应指明受影响的隐式转换、运算符和间接视图调用，便于追查源码定义。

普通标量、UUID 和函数准入继续沿 [ADR-0263](ADR-0263-trino-scalar-values-and-publication-eligibility.md)。普通聚合的行域、重载、结果和状态交换继续沿 [ADR-0259](ADR-0259-trino-aggregate-state-and-published-semantics.md)。

## 何时重新评估

- 发布说明和现有强制重建入口长期无法覆盖真实的函数行为升级时，重新评估可执行的受影响对象索引及自动重建边界。
- 频繁改版的视图发生有测量依据的重建开销，并且已有稳定的完整结构化查询身份时，重新评估跳过外观变化的依据。
