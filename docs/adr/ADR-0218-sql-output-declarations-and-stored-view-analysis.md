---
id: ADR-0218
title: "SQL owns output declarations and stored-view relation analysis"
domain: [sql-compiler, sql-language, error-contracts]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  ADR-0095: "MV carriers preserve every original AnalyzeError; the former historical MV String-facade exemption no longer authorizes erasing one. SQL code/span and protocol ownership remain unchanged."
date: 2026-11-04
updated: 2026-11-05
provenance:
  - "discussion: 2026-11-04 accepted SQL output naming and stored-view declaration design"
  - "discussion: 2026-11-05 follow Trino's standard Iceberg view persistence; retire the private source header and definition column list extension"
code-anchors:
  - "novarocks/sql/src/analysis/schema.rs (AnalyzedOutputField)"
  - "novarocks/sql/src/compiler/root_output.rs (RootOutputSemantics)"
  - "novarocks/sql/src/compiler/completion_catalog.rs (CatalogCompletionState)"
  - "novarocks/sql/src/analyzer/view.rs (analyze_view_relation)"
  - "novarocks/spi/src/connector/view_metadata.rs (ConnectorViewMetadataValue)"
  - "novarocks/connector/iceberg/src/catalog_control/views.rs (materialize_view_metadata)"
  - "novarocks/query-application/src/api/command.rs (CommandError)"
---

## 问题

查询的自然输出名称、内部计划标签与持久视图的公开声明，为什么必须由 SQL 在分析边界分别拥有，应用层又应怎样取得它们需要的目录事实？

## 背景与执行事实

字段有几种不同用途，只有同一 ordinal 的分析结果才能建立它们之间的关系。

|事实|作者与用途|不能代替的事实|
|---|---|---|
|可空自然名称|分析中的 `AnalyzedOutputField.field_name`，标识符或显式别名提供名称，计算表达式可匿名|内部表达式打印、优化器标签|
|内部标签与 ColumnId|计划构造和规则匹配；ColumnId 表示实际符号|客户端列名或持久 schema|
|冻结根声明|`RootOutputSemantics` 在优化前捕获原 ordinal、完整类型与自然名称，最终 presentation 为原名或 `_colN`|优化后的 descriptor 展示文本|
|视图公开声明|查询本身具名的输出名称与逻辑类型；Trino 的视图类型不附加 SQL 非空承诺|内部标签、查询时重新推测的旧声明|
|目录观察|application 保管实际 lease、I/O 与 statement memo；SQL 仅接 table/view/missing facts|查询时重新读取 latest 的第二套权威|

`CatalogCompletionState` 复用原 SQL 关系和表达式 collector，在收到 view fact 后发现保存上下文内的新依赖，闭包完成后才分析。`analyze_view_relation` 校验公开列数、自然名称和类型可赋值性，必要时用原值转换作者及 Subquery/Project 建立声明关系。`ConnectorViewMetadataValue` 的 SQL、version、schema 和上下文来自同一观察。

真实 Java REST 首次 CREATE 会重建版本 summary，不能把完整声明放进客户端提交的 summary。ADR-0176 已记录这一服务端行为；本轮真实 CREATE 后 GET 也确认自定义 source-format、source header 与正式类型 map 均未保留。此前采用私有 summary 的方案因此退役，改为 Trino 483 的标准 SQL representation、schema 与版本上下文字段。

这里记录长期设计。纯 SQL 内核、SPI/Provider 和错误载体已有实现与定向证据；所有应用消费者的接线、旧 walker 退役及原生组合验证仍分别核对。ADR 的 active 状态不证明端到端能力或测试准入已完成。

## 考虑过的选项

1. **统一改写所有内部标签作为客户端列名。设计否决。** 一个字符串看似能省去交接，却让优化器改写、重复 occurrence 和匿名表达式展示成为语言事实，也让定义和规则互相依赖打印方式。
2. **应用层先将视图展开成派生表，再强制改成保存列名。设计否决。** 它绕过公开名称和 schema 校验，无法识别同宽 drop/add，并为表达式子查询、CTE、保存上下文和递归建立第二套 SQL walker。
3. **从物理 Arrow schema 反推持久声明或独立传入第二份 DDL 列表。设计否决。** 同一 carrier 可表示不同名义领域，推测不能恢复其正式语义；两个公开声明作者会在持久往返中漂移。
4. **SQL 分离自然名称、内部标签和冻结根；目录 owner 提交同版本完整视图 facts，由 SQL 分析声明关系。采纳。** 生命周期和外部访问仍留在应用及 Provider；SQL 消费不可变纯事实。
5. **把持久视图改成对象 ID 绑定或可升级的 typed IR。待评估。** 这可能减少重解析，但属于不同的持久语言、跨版本和对象生命周期合同，当前不以它替代有效 SQL source。
6. **用 SQL 注释、字段 doc 或全局 properties 封装额外声明。方向否决。** 当前目标是 Trino 兼容；先采用其标准声明与类型支持边界，不为非 Trino 的定义列列表或类型扩展增加持久格式。

## 裁决

1. **原根声明规则。** 两条优化入口和逻辑消费入口都从原分析根冻结同一 ordinal 声明。最终客户端匿名名 `_colN` 不回填自然名称，也不参与内部规则匹配；定义不能把它当成用户给出的名称。
2. **定义校验规则。** 定义查询按 ordinal 检查缺名及大小写不敏感重名；既有大小写裁决保持。按 Trino 483 拒绝 `CREATE VIEW v(cols)`，用户在 SELECT 本身写 AS 别名。普通关系、CTE、VALUES 与 UNNEST 的列别名列表保留；视图调用方别名仅在声明验证成功后应用。
3. **单一视图分析规则。** application 只加载并保管目录事实、原 lease 和实际退出；SQL 在保存 catalog/database 的独立 child context 中重新解析、发现依赖并分析，不继承调用方 CTE、alias 或 correlation。Read、EXPLAIN、DML、CTAS、MV 和 SchemaOnly 定义必须沿同一 capture owner。显式历史选择不能被默认为 Current。
4. **同版本声明规则。** SPI 保留原 SQL、正式解析上下文、不可变版本身份及公开名称与类型。Iceberg 从同次 current version 的 SQL representation、所引用 schema 和上下文字段投影；不依赖 Nova 私有 summary 标识。类型按 Trino 483 的标准映射准入，不可表达的类型在发布前明确 Unsupported，不借 int/binary 猜回 Nova 领域。表的 nominal 类型作者不受影响。prepare/create 使用原 generation，不增加 CREATE 后补写。原 REPLACE operation-id、新版本及 `-1` 选择规则继续遵守 ADR-0176。
5. **分层失败规则。** 首次创建引用不存在的自身保留 TABLE_NOT_FOUND；替换已有视图的直接自引用为 VIEW_IS_RECURSIVE；通过已保存视图形成闭环，由该分析边界对外保留 INVALID_VIEW 及原递归原因。body 语义失效、声明 stale、必要转换能力缺口和真实 source/control 分别保留，不靠错误文本推类。
6. **原转换规则。** store-assignability 由 SQL 决定，计算仍使用原 cast 作者。没有准确语义或失败保证的必要转换返回 Unsupported，不增加 view 专用 cast mode。视图 TypeId 不带 SQL 非空承诺：顶层、Struct 字段、List 元素及 Map 值可空，Map key 非空；Iceberg required 不授予额外视图约束。该规则不放宽表、原分析值或执行合同。
7. **错误保管规则。** 通用 carrier 保任意原 AnalyzeError、原 source/control 和外部效果事实；已提交或提交未知不能伪装成纯准入失败。协议层只映射登记 code。保存 body 的字节位置只能作为内层原因；向调用者投影时使用真实引用位置，不能混用两份 source 的坐标。
8. **既有预算规则。** 依赖发现只有严格新增 need 才获得额外 Catalog discovery 回合；沿原 distinct/exchange bytes/work/control 预算，不新设无依据的视图深度 cap。完整类型的结构字节计算描述保留材料，不授予容量或访问权。

## 接受的妥协（诚实记录）

保存公开类型会使原来仅有列名的 Provider contract 变大，仍须准确计算 catalog exchange 的实际结构成本。视图查询必须自己具名，公开 schema 即名称声明，不另保可匿名 source header。标准 Iceberg 映射不能表达的 Nova 类型明确拒绝，不为保留扩展增加第二种持久声明。

兼容的类型也可能没有准确可用的转换。当前某些整数窄化作者用 NULL 表示溢出，无法保证视图所需的运行失败，因此即使具体常量较小，也在必要转换处明确 Unsupported。接受可见能力缺口，不以数据碰巧可转替代静态合同。

本地视图仍是 ADR-0117 的 process-runtime registry，外部视图由 Provider 持久化；共同分析合同不会赋予本地重启恢复能力。本次不迁移旧 Nova 定义；读取只消费可证明的标准事实，不恢复已丢失的私有声明。5a 仍按原文法迁移路线写 starrocks dialect，完整文法及持久 dialect 翻转留给 5e。

有效原 SQL 与创建上下文继续遵守 ADR-0090。语义等价但原文不同的定义可以有不同版本身份；版本 token 是同一观察的身份，既不是 lease，也不授权重试或资源容量。

## 何时重新评估

1. 原值转换作者能够保证窄化溢出等准确失败，或递归 ROW/list/map 的类型与布局转换实现完整后，重新评估明确 Unsupported 的转换，而非改动声明校验。
2. Provider 开始承诺存量用户升级或额外引擎类型时，先核对 Trino 与标准格式的支持边界，再制定显式迁移和失败合同；不得在 reader 内猜回旧类型。
3. 引入对象 ID 绑定或可升级 typed IR 时，重新评估持久定义及版本身份，同时明确 drop/recreate、权限、跨引擎和 source 诊断行为。
4. 真实规模证据显示现有 distinct/bytes/work 预算不足以表达依赖图成本时，按资源作者重新评估；不能用任意 view 深度替代证据。
