---
id: ADR-0055
title: "Row-DML consumers read the provider-signed strategy; SQL predicate legality stays in Core"
domain: [provider-spi, frontend-dml]
status: active
supersedes: []
superseded-by: null
partially-superseded-by:
  - id: ADR-0171
    scope: "Compromise 5: forward written-version prediction; source facts and provider strategy ownership remain effective"
    link: "ADR-0171-commit-operation-model.md"
date: 2026-08-11
provenance:
  - "discussion: 2026-08-11 row-DML physical routing closeout"
  - "implementation: row-DML consumer cutover onto the provider-signed row-mutation strategy"
code-anchors:
  - "novarocks/connector/iceberg/src/commit/validation.rs (row_mutation_strategy_from_metadata)"
  - "novarocks/spi/src/connector/row_mutation.rs (ConnectorRowMutationPreparation)"
---

## 问题

ADR-0049 已把 row-mutation 的 strategy、identity、route 与 cohort 判给 Provider。但如果 DELETE / UPDATE /
MERGE 的调用方仍然各自加载具体表并重新判定同一件事，这个所有权在生产路径上并不成立。那么：

1. 物理策略的判定应该只剩几处，判定结果由谁签发？
2. 当调用方那份判定与 Provider 那份**语义不等价**时，以哪一侧为准，代价是什么？
3. "哪些 WHERE 子句是合法的 DELETE 条件"属于 Provider 还是 SQL？

## 背景与执行事实

ADR-0049 的契约已经完整实现：`ConnectorRowMutationPreparation` 本身携带 Provider 签发的 `strategy`，
其 legacy 实现也已从 frozen admitted metadata 推导它。但**没有任何 row-DML 调用方读取该字段**——
全仓唯一读取点是 Provider 自身的 copy-on-write 断言。

调用方各自重新判定：DELETE 调用 `classify_sql_delete_strategy`，UPDATE / MERGE 调用
`select_iceberg_update_mode`，三者都先从具体 registry 取 catalog、load 出具体表。

两份判定不等价，共三处差异：

1. **DELETE 策略**：调用方用 `format_version == V3 || write.row-lineage=true` 选 deletion vector；
   Provider 只看 `format_version == V3`。而 deletion vector 是 format-v3 特性，其 commit 路径对非 v3
   直接报 `RowDeltaDvCommit requires an Iceberg v3 table`。因此一张 v2 表若声明
   `write.row-lineage=true`，今天的 DELETE 会选 deletion vector、跑完整个分布式 staging、在 commit 期硬失败。
2. **写入支持性 guard**：default sort order 可解析、partition spec 无 variant、sort order 无 variant
   这三个 fail-fast 检查只在调用方路径生效，Provider 的策略推导一个都不跑。
3. **MERGE matched-DELETE**：调用方在 MERGE 可删除匹配行时强制 merge-on-read（即使表声明
   copy-on-write），因为 copy-on-write 的 rewrite 无法表达 matched delete。Provider 的推导只读表属性，
   不看 effect set。

另有两项相关事实：

- UPDATE / MERGE 的目标列类型由调用方从 Iceberg schema 解码，其中 variant → `LargeBinary`、
  binary → `Binary` 的覆盖与 Provider `metadata.schema` 给出的读取类型**不等价**。这个 SELECT 与 DML
  的类型分歧是既有现状（见 ADR-0052 所属的 write-default facts 工作留下的记录）。
- DELETE 的 WHERE 校验器接受 `sqlparser` AST，函数体内有 104 处 AST 使用，而 Provider crate 没有
  `sqlparser` 依赖；其唯一生产调用点丢弃返回值，实际过滤由 distributed SELECT planner 拥有。

## 考虑过的选项

**选项 1：保留两份判定，让 Provider 也认 `write.row-lineage=true`。** 行为逐字节不变。但把一条 Iceberg
规范上不成立的组合固化进 Provider 判定，并永久保留一个"跑完整个分布式写再失败"的路径。

**选项 2（针对 WHERE 校验）：建立中立 predicate IR，把校验搬进 Provider。** 忠于"表格式知识归 Provider"
的直觉。但需要一个能承载相同 accept / reject 集合的新跨 owner 契约面；IR 一旦有损，被拒绝的 WHERE 集合
会静默改变。

**选项 3（针对 WHERE 校验）：删除该校验。** 结果本就被丢弃。但今天 fail-fast 的 WHERE 子句会变成继续执行，
改变失败语义。

**选项 4（采用）：调用方只读 `preparation.strategy()`；三处差异按"哪一侧正确"逐条收敛；WHERE 校验留在
SQL owner 并只做类型中立化。**

## 裁决

采用选项 4。

1. **物理策略只有一个可到达的判定结果**，即 Provider 签发的 `preparation.strategy()`。调用方不再调用
   `classify_sql_delete_strategy` / `select_iceberg_update_mode`，也不再为 DELETE 指定
   `ConnectorWriteInputRequest` 的物理 variant。判定规则收敛到 Provider 的
   `row_mutation_strategy_from_metadata` 一处。

2. **DELETE 策略以 format version 为唯一键。** v2 表即使声明 `write.row-lineage=true` 也走 position
   delete。这让上述"跑完再失败"的路径变成成功路径，是本裁决**唯一**有意接受的用户可见行为变化。

3. **三个写入支持性 guard 在 Provider 的策略推导内先行执行**，对 DELETE / UPDATE / MERGE /
   equality delete 一致生效；拒绝行为与可见性不变。

4. **MERGE 可删除匹配行时判定 merge-on-read**，规则移入 Provider，用 row-mutation intent 已携带的
   effect set 表达，契约无需扩展。

5. **variant / binary 的写入目标类型成为 Provider 签发的列级 facts**，逐列保留现有覆盖。SELECT 与 DML
   对同一列给出不同 Arrow 类型的分歧被保留，只是所有权变清晰；统一它需要独立任务。

6. **SQL 谓词合法性属 SQL owner。** DELETE 的 WHERE 校验留在 Core，只把内部的 Iceberg 具体类型换成中立
   词汇，accept / reject 集合与拒绝时机逐项不变。理由是它的输入是 SQL AST（不能跨 SPI 边界，
   见 ADR-0049 的同一条边界），其主体是 AST 遍历与字面量解析而非表格式知识，且它回答的是 statement
   合法性问题。

7. **语句已指定物理形式时不存在可委派的策略选择。** `ALTER TABLE ... ADD EQUALITY DELETE` 保留其声明的
   equality-delete 形式，不进入 strategy 判定——否则 Provider 会改选 deletion vector 或 position delete，
   改变语句语义。

## 接受的妥协（诚实记录）

1. **裁决 2 改变了失败语义，而同期的交付边界要求"失败语义逐项不变"。** 这是一条自觉列出的例外，不是疏漏。
   选择它而非选项 1，是因为被改变的路径今天百分之百以 commit 期硬失败结束——保留它等于把一个已知缺陷
   永久化。

   **实测修正（重要）**：尝试为这条变化写端到端 SQL 用例时发现，`format-version=2` 且
   `write.row-lineage=true` 的表**根本无法 INSERT**——写入侧按 row-lineage 模式产出行血统，而 v2 manifest
   list 没有 `next-row-id`，提交时报 `Manifest list row lineage mismatch`。这是与本裁决无关的既有缺陷。

   因此裁决 2 的用户可见影响比初稿描述的**窄得多**：该表形态在 NovaRocks 里拿不到数据，DELETE 那条路径
   实际不可达（外部引擎也不会产出这个组合，因为 row lineage 本身是 v3-only）。裁决 2 仍然正确——它把
   一条永远失败的分支换成了正确语义——但它修的是一个**理论上的**缺陷，不是用户会撞到的缺陷。相应地，
   本项妥协所担心的"有用户依赖该失败现象"也不成立。

   端到端用例因此没有留在仓库里：它测的组合不可达，且会因为上述无关缺陷而失败。覆盖由
   `spi5h_delete_strategy_is_keyed_on_format_version_alone` 在判定层完成。
   **v2 + row-lineage 表不可写这件事本身值得单独立项。**

2. **裁决 6 是按"输入形态"而非"知识归属"划线的。** "哪些 WHERE 能被某个表格式表达"确实带有表格式色彩；
   把它留在 Core 是因为 SQL AST 不能进 Provider，而建立足够丰富的中立 IR 的成本与静默改变拒绝集合的风险
   都超过收益。这是**成本与风险驱动的划线，不是因为 Core 更适合拥有它**。若未来确有 Provider 侧谓词下推
   需求，这条线应重新画。

   裁决 6 还带来第二处用户可见变化：该校验的两条拒绝错误原本把 Iceberg 类型的 Debug 直接嵌进文本
   （`Decimal { precision: 10, scale: 2 }`、struct/list/map 的 `Type` Debug）。中立化后改用 Arrow 词汇
   渲染（`Decimal128(10, 2)`）。accept / reject 集合逐项不变，只有类型名的渲染变化。被否决的替代是为
   "错误文本保真"单独签发一处列级 fact——那会把 table-format 词汇重新引回 Core 的错误路径。

3. **裁决 5 让 SELECT 与 DML 对 variant / binary 列继续给出不同 Arrow 类型。** 这个分歧本身是缺陷。本次
   只把它从"调用方各自解码 Iceberg schema"变成"Provider 显式签发"，没有修复它。选择不修，是为了让本次
   变更的行为面只有裁决 2 一处，便于回归归因。

4. **legacy 与 Provider 的重复实现在此之后继续并存。** 本裁决只收敛 consumer 面；FE control factory 仍未
   切换，row-mutation control 仍只有一份 legacy 实现。这是已接受的临时成本，必须在同一 arc 内消除。

5. **新增了四处有界 facts，远多于立项时的估计（一处）。** 分区 source 列、写入目标类型、base version
   ordinal、written version ordinal 各一处。四者都挂在既有 capability 上、不新增 capability trait，但数量
   本身说明立项时"consumer 面只是换调用"的判断偏乐观——第四处是在合并 commit 词汇时才暴露的：
   merge-on-read writer 必须在 commit 存在之前就知道被重写的行属于哪个版本。

   两处 ordinal 成对（一个向后看、一个向前看）且都 digest-bound，这限制了滑坡；但分区 facts 一旦开口，
   仍有被扩成不受限 property map 的风险。任何超出"与 Arrow ordinal 对齐的 source column + typed 值"的
   扩展请求都应重新设计，而不是加字段。

## 何时重新评估

- **Provider crate 开始服务 row-mutation control 时**：目前 `ConnectorWriteControl` 的 row-mutation
  prepare / activate 只有一份 legacy 实现，Provider crate 不 override 它们。当那份实现迁移过去时，
  裁决 1、3、4 的规则会随之移动，需要确认它们没有在迁移中被复制成两份。
- **出现第二个 table-format provider 需要行级 DML 时**：裁决 6 的划线会被真正检验——如果两个 provider
  对同一 WHERE 子句的可表达性不同，则 Core 侧单一校验不再成立，必须重新考虑选项 2。
- **variant / binary 类型分歧被单独立项修复时**：裁决 5 的列级 facts 应随之退役，而不是长期保留两套映射。
- **有用户报告依赖 v2 + row-lineage 的 DELETE 失败行为时**：重新评估裁决 2 的妥协 1。
- **分区 facts 收到超出"与 Arrow ordinal 对齐的 source column"范围的扩展请求时**：那是滑坡的信号，应
  重新设计而不是加字段。
