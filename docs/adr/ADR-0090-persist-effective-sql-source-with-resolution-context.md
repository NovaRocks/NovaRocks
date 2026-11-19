---
id: ADR-0090
title: "Persist effective SQL source with its resolution context"
domain: [sql-language]
status: active
supersedes: []
superseded-by: null
date: 2026-08-19
provenance:
  - "PR: #941 — raw MV and View definition persistence"
  - "discussion: 2026-08-19 SQL definition persistence boundary"
code-anchors:
  - "novarocks/parser/src/ast/mod.rs (RawQuerySlice)"
  - "novarocks/frontend/src/mv/domain/persistence/definition.rs (StoredMvDefinition)"
  - "novarocks/frontend/src/mv/domain/persistence/descriptor.rs (MvDescriptorV2)"
  - "novarocks/frontend/src/common/persisted_query_definition.rs (PersistedQueryDefinition)"
---

## 问题

需要跨重启重新解析的 MV 与 View 定义，为什么必须持久化用户有效 SQL 原文及其创建时解析上下文，而不能持久化
normalizer 或 AST printer 生成的 canonical SQL？

## 背景与执行事实

NovaRocks 的 MV 与 View 都需要在 refresh、query rewrite、Frontend restart 或 lake rebuild 时重新获得 query定义。旧路径在
CREATE 时先运行 StarRocks compatibility normalizer 或修改 `sqlparser` AST，再把 AST 的 Display 输出写入 StateStore、MV
lake descriptor 或 Iceberg View SQL representation。这个输出不是用户输入：它会改变大小写、空白、quote、extension
写法与对象名资格化，还可能包含只供内部分析使用的 marker。

`sqlparser` 是 0.x dependency，AST shape 与 Display 都不是 NovaRocks拥有的稳定数据格式。只要其 printer行为或
normalizer顺序变化，同一持久定义就可能在重启后产生不同文本、错误位置或 parse结果；内部 marker也可能被带入 durable
metadata与用户可见 SHOW/error。

仅保存 raw query body仍不完整。对象自身的 target namespace 与 query 创建时的 default database可以不同；未限定表名必须
继续按 CREATE 时的 catalog/database解析。若重启时改用 target namespace或调用者当前 session，定义会静默绑定到另一个表。

NovaRocks 当前处于开发期，没有需要迁移的存量用户或存量 MV。旧本地 StateStore、lake metadata与测试 fixture可以删除后
重建，因此没有理由让旧 printer输出继续污染长期合同。

## 考虑过的选项

1. **继续持久化 normalized/canonical AST Display。** 读取简单，且对象名可以预先资格化；代价是第三方 printer与
   normalizer成为事实上的 storage protocol，原文位置、extension语法和内部 marker无法可靠恢复。拒绝。
2. **只持久化 raw query body，重读时使用 target或调用期 session context。** 能摆脱 printer，却会让未限定对象名在
   CREATE与refresh/restart之间漂移；这是结果正确性问题，不是展示差异。拒绝。
3. **同时持久化 raw与canonical两份文本。** 可以让旧执行路径继续读canonical，但会产生两个定义authority；两份文本的
   一致性、hash、recovery与未来parser cutover都需要额外状态机。拒绝。
4. **持久化effective user query source、dialect与创建时resolution context，所有其它文本/AST按请求临时派生。**
   能保留用户语言事实与name-resolution语义，并让parser/printer/normalizer作为可替换实现。采纳。
5. **为旧canonical记录增加兼容reader或自动canonical→raw migration。** 旧值已经丢失原文，任何转换都只能把printer
   输出重新标成“原文”；在没有存量用户的开发期为此建立长期compat层没有价值。拒绝。

## 裁决

需要持久化的 MV/View query definition以 effective user source为唯一文本authority：session已完成变量替换，但尚未运行
任何normalizer、AST mutation、name qualification或printer。持久范围是`AS`后的query token range，不包含CREATE envelope、
终止分号或token范围外trivia；范围内部的comments、hints、quotes、case与排版全部保留。

durable contract必须把该raw source与dialect、创建时default catalog、创建时default database/namespace及format version一起
保存。target identity不能代替source-resolution context。refresh、rewrite、SHOW、restart与lake rebuild只从这一份合同读取；
legacy compiler所需normalized SQL、qualified AST或printer输出只能在一次请求内派生，不得写回任何durable owner。

MV StateStore definition与lake descriptor同步切换到新合同；NovaRocks创建的session View与external Iceberg View也写入raw
query representation。为可靠获得View query source，View command grammar与`RawQuerySlice`由NovaRocks parser统一拥有，不能
在Frontend增加第二个`AS`探针或从normalized文本反推source位置。

本次变更是开发期hard cut：新格式单写单读，旧schema/descriptor/fixture不迁移、不双读、不fallback。发现不兼容测试数据时
删除并按新DDL重建。第三方Iceberg View仍按provider提供的dialect/default namespace在访问时解析，不视为NovaRocks待迁移
存量对象。

[ADR-0241](ADR-0241-sole-trino-sql-dialect-and-closed-syntax-extensions.md) 规定新查询定义和 Iceberg SQL representation 的持久方言。有效 SQL 原文和创建时解析上下文仍按本 ADR 保存。

## 接受的妥协（诚实记录）

raw文本比canonical Display更不利于去重：语义相同但排版不同的定义会有不同bytes与content hash，也可能更早触碰durable
record预算。我们接受这一点，因为definition identity首先表达用户提交的语言事实，而不是printer判定的语义等价；真正需要
semantic fingerprint时应由analyzed IR另行产生，不能复用storage文本冒充。

把View command family提前切入自有parser增加了本次变更的实现与测试面。我们接受这个成本，不是因为它更省代码，而是因为
任何临时source extractor、normalize source map或双parser header都会建立第二个语言authority，并把迁移债务带到持久格式中。

开发期直接拒绝旧数据牺牲了升级兼容性与在线repair能力。这里的真实理由是系统尚无存量用户，兼容工程无法恢复已经丢失的
原文，只会固化错误格式；若产品开始承诺持久数据升级，这个取舍必须重新裁决，不能机械沿用。

## 何时重新评估

1. NovaRocks首次形成有存量用户、跨版本升级SLA或不能删除的生产MV/View metadata之前。
2. 引入第二SQL dialect、lossless CST、可版本化typed Query IR或需要逐字符复现完整CREATE statement时。
3. raw definition使StateStore/Iceberg metadata预算在真实workload中成为限制，需要外置定义或content-addressed storage时。
4. name-resolution语义改为schema binding ID、object ID或其它不再依赖default catalog/database的持久合同之后。
5. 第三方Iceberg View互操作需要明确writer provenance、dialect version或跨引擎round-trip保证时。
