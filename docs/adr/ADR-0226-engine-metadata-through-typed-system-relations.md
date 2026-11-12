---
id: ADR-0226
title: "Engine metadata is exposed through typed system relations"
domain: [sql-compiler, catalog-attachment, frontend-mv]
status: active
supersedes: []
superseded-by: null
date: 2026-11-10
provenance:
  - "discussion: 2026-11-09 accepted Trino-standard system catalog, retained product commands and null last-fresh time"
  - "discussion: 2026-11-10 accepted typed batch snapshots, original statement custody and source-missing freshness semantics"
  - "reference: Trino 484, commit e2d50bb733e193607c44be84ad80ce2430ba1d01"
code-anchors:
  - "novarocks/sql/src/system_table.rs (SystemTableDeclaration, SystemTableBinding)"
  - "novarocks/query-application/src/system_tables/registry.rs (SystemTableRegistry)"
  - "novarocks/frontend-application/src/server.rs (serve_ready_frontend_session_factory)"
  - "novarocks/frontend-application/src/query_execution/completion_facts/system_source.rs (FrontendSystemTableFacts)"
  - "novarocks/frontend-application/src/system_tables/provider.rs (FrontendSystemTableProvider)"
  - "novarocks/physical-plan/src/system_relation.rs (SystemRelationData)"
  - "novarocks/execution/src/exec/pipeline/builder/compiled_system_relation.rs (CompiledSystemRelationSourceFactory)"
---

## 问题

引擎元数据与状态怎样成为可连接、过滤和聚合的普通 SQL 关系，同时保留类型、领域事实来源与执行资源的原有所有者？

## 背景与执行事实

系统关系的声明、语句快照和执行载体属于不同层次。名称解析不应发起 Provider I/O，启动期静态注册表也不能持有某一次语句的资源。

| 实体 | 负责的事实 | 生命周期与边界 |
|---|---|---|
| SQL 系统表声明 | catalog 绑定、列名、完整 FunctionValueType、键列 | 纯名称与类型事实，不拥有外部访问能力 |
| Query Application 注册表 | 封闭的标准声明及合法扩展，重复和标准名冲突检查 | 启动期组合；不保存某句的 pin、CPU、IO 或快照 |
| FE 原语句 Source 与 Provider | 当次声明的目录行、MV 产品事实与 Current 观察 | 保留当次 typed planning pin、Connector IO、CPU 和同 Root 结果窗口，直到真实退出 |
| 类型化快照 | 明确行数、所需列的类型化 RecordBatch 序列、原存储责任 | FE 在执行前冻结；零列快照也保留真实行数 |
| compiled SystemRelation | 完整列布局和类型化批序列 | 独立计划节点；BE Source 持游标逐批输出，不整份拼接 |

`SystemTableRegistry` 的静态声明与 `FrontendSystemTableFacts` 的语句能力分开。SQL 经 catalog/声明补全确定关系，再按实际所需列和保守键约束发出快照需求。Provider 返回的批序列经过类型、NULL、宽度、Arrow 几何与布局核对后才能进入计划包。

普通 SQL 算子继续执行过滤、连接和聚合。键约束仅减少元数据访问，不替代原 SQL 谓词。该路径遵循 [ADR-0185](ADR-0185-compiled-packages-are-the-sole-static-plan-path.md) 的唯一 compiled package 执行入口，与 [ADR-0198](ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md) 的原逻辑执行、资源和真实退出边界一致。

## 考虑过的选项

**A. 为每种系统表或 SHOW 写专用 FE 求值器。** 单表场景接入快，但每次增加过滤、排序、连接或聚合都要复制 SQL 语义。**设计否决**：系统元数据需要作为普通 SQL 关系参与同一套类型检查和算子执行，专用求值器不能成为第二种查询语义。

**B. 将快照变成 SQL 常量池或 VALUES。** 能复用现有载体，但把目录规模变成常量表达式数量，继承与系统数据无关的常量和 StaticValues 政策，并要求整份物化。**设计否决**：系统数据是语句执行输入，不是 SQL 常量；换一个节点名也不能抹掉实际存储责任。

**C. FE 按语句冻结类型化批序列，BE 通过独立 SystemRelation 游标消费。** 采纳。继续复用 Arrow、IPC、类型与 compiled 执行校验，目录和 MV 事实分别由原产品服务提供。

**D. BE 按需远程分页消费 FE 元数据。** 可减少 FE 整份保留，但需要新的数据面、流控制和取消/退出合同。**待评估**：当前保留批序列快照；当实际测量显示整份 FE 保留或传送成为约束时再由原数据面所有者设计。

## 裁决

1. **Static-declaration rule。** 启动期注册表仅保存描述。Provider 在原语句 Source 下绑定当次能力；不得将 FE 聚合服务、某次 planning pin 或资源 owner 放入长期注册表。
2. **Typed-batch rule。** 系统快照使用类型化批序列和显式行数。完整 FunctionValueType、NULL 和宽度语义不从 Arrow 展示值猜测；空所需列集仍保留行数。系统关系退出常量池与 VALUES，不整份合并为一个 RecordBatch；BE 每次推进原游标消费批次。
3. **Two-round rule。** 名称/描述补全与快照应答分开。普通 EXPLAIN 只展示关系、所需列和键约束，不取快照；EXPLAIN ANALYZE、普通读取及 INSERT/CTAS 的源在执行前取得快照。只下推键列上顶层合取中的字面量等值/IN；SQL 谓词仍保留。
4. **Reserved-name rule。** `system` 是保留的内置 catalog，优先于外部 catalog 注册。创建同名 catalog 报 `ALREADY_EXISTS`，删除报 `NOT_SUPPORTED`；静态声明或持久配置的同名冲突明确失败。`USE system[.schema]` 依据真实注册 schema 检查，未知 catalog/schema/table 保持各自类型化错误。
5. **Catalog-binding rule。** `<catalog>.information_schema.<table>` 绑定到该 catalog，两段名绑定会话 catalog；没有会话 catalog 时沿现有默认 catalog 规则处理。System 自身同样可解析 information_schema。默认系统普通 schema 仅 `metadata`；`runtime` 在有表注册后出现。当前 `schemata` 仍不列 information_schema 自身，完整 Trino 列集合与自身行对齐另行演进。
6. **Naming rule。** 表和列使用小写下划线。元数据扩展放 `system.metadata`，运行状态放 `system.runtime`；扩展不占用 Trino 484 标准表或过程名称，不放入 `jdbc` 或 catalog-local information_schema。重名和标准名单冲突在组合时拒绝。
7. **Read-only rule。** system 与各 catalog 的 information_schema 不允许建 schema、建表或写入，目标拒绝为 `NOT_SUPPORTED`。允许的 INSERT/CTAS 源与其写目标分开判断。DELETE/UPDATE/MERGE 子查询源与统计任务中的系统表暂不接入快照，明确报 `NOT_SUPPORTED`；将来接入由这些流程的原所有者负责。
8. **View-enumeration rule。** `information_schema.tables` 列出基表、物化视图和可列举的视图；表/物化视图为 `BASE TABLE`，视图为 `VIEW`。仅当 Provider 明确不支持视图列举时省略 VIEW 行；其他类型化错误不得变为空结果。HMS 仍按 [HMS 版 ADR-0216](ADR-0216-hms-uses-upstream-catalog-with-read-fallbacks.md) 接受全名称、BASE TABLE 类型回退，不借本规则改变其只读、缺表错误和 metadata 访问妥协。
9. **Freshness-fact rule。** `system.metadata.materialized_views` 为十列标准表，键为 catalog_name/schema_name/name；last_fresh_time 类型为 `timestamp(9) with time zone`，当前恒 NULL。无发布为 STALE；每个原 relation occurrence 的基表均存在且对象/数据版本与发布输入相同为 FRESH；数据变化、对象替换或明确的基表不存在为 STALE；MV 自身 UNAVAILABLE 为 UNKNOWN。其他读取失败保留原类型化错误，不能伪装为 UNKNOWN 或 Missing；调度合法性不替代查询的新鲜度事实。
10. **Original-custody rule。** 取消借用 future 不销毁原 Source、capture、CPU job 或 pin。原错误、原接收格、外部工作与存储持有分别保留，真实关闭后才能退休和退款；超时、终态和停止等待不代替实际退出。
11. **Surface-migration rule。** SHOW VIEWS、SHOW BASIC STATS META、SHOW HISTOGRAM STATS META 退出语句集合；非标准 information_schema.materialized_views 不再声明。SHOW MATERIALIZED VIEWS、SHOW BACKENDS、SHOW TABLE STATS 保留原服务；SHOW ANALYZE JOBS、SHOW ALTER TABLE OPTIMIZE 与 MV 管理过程也不因标准系统表而变更。

## 接受的妥协（诚实记录）

- FE 仍需在执行前保留整份快照及其真实资源责任。批序列避免额外整份拼接，不代表任意规模快照都能在当前资源配置下运行。
- information_schema 的现有列与自身行尚未完整对齐 Trino；无 catalog 时的两段名仍沿原默认规则。HMS 的条目类型回退继续存在。
- last_fresh_time 暂不从当前时间或成功刷新时间猜值。填值需要 MV 原持久事实与 Connector 对基表变化时间的实际支持。
- 本记录固化长期合同；组件、Native、SQL/System 与全工作区验证按各自范围取证，不把局部测试等同于全部调用面的最终准入。

## 何时重新评估

- 真实测量显示 FE 整份快照保留或传送成为约束时，评估 BE 按需消费的元数据数据面，明确分页、背压和真实退出所有者。
- 原 MV 持久化与 Connector 能提供可证明的首次变化时间时，再接 last_fresh_time；不以近似时间替代。
- 新增标准 runtime/jdbc 表、完整 information_schema 或允许变更/统计源使用系统关系时，由相应所有者原子演进声明、准备与执行合同。
- HMS 上游可列举真实条目类型时，按 HMS 版 ADR-0216 的条件重新评估 BASE TABLE 回退。
