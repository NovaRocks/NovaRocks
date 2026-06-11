# OQ-2:Join key 自动 NULL filter 推导 — 设计文档

- 日期:2026-05-30
- 状态:设计已评审，待实现
- Roadmap:Optimizer Plan Quality Roadmap → OQ-2
- 依赖:无（与 OQ-1 独立，收益可叠加）
- StarRocks 对应规则:`JoinPredicatePushdown.deriveIsNotNullPredicate`

---

## 1. 背景与动机

在 PR #200 收尾的 join suite `-j 1` 对比中，标杆 query `join_one_key` q22：

```sql
WITH w1 AS (SELECT * FROM opt_probe.t1 WHERE k1 < 100)
SELECT count(1), count(t1.k1), count(t1.c_tinyint_null)
FROM opt_probe.t1 t1
LEFT SEMI JOIN w1 t2 ON t1.c_tinyint_null = t2.c_tinyint_null;
```

`c_tinyint_null` 列约 11/12 行是 NULL。等值 join 的 NULL key 永远不可能匹配
（`NULL = NULL` 不为真），但 NovaRocks 当前把 build 侧所有 NULL 行都建进 hash
table；StarRocks 则在 build/probe scan 上自动加 `IS NOT NULL` 谓词，把 NULL 行在
扫描阶段就过滤掉。

OQ-2 的目标：对「NULL key 不可能参与匹配」的 join 类型，自动在 join key 列上推导
`IS NOT NULL` 谓词并下推，缩小 hash table、减少带宽与 probe 行数。roadmap 预估单独
收益为 join suite wall_time **-10% ~ -20%**。

### 设计取向

本设计严格遵循 roadmap 的「照抄 StarRocks，不发明新规则」原则。覆盖范围采用
**StarRocks-faithful 保守集**：只推导 StarRocks `deriveIsNotNullPredicate` 实际推导
的三种情形。这样做的最强理由是 OQ-2 的验收 gate（roadmap PR checklist #3）要求
NovaRocks 的 EXPLAIN 与 StarRocks plan 收敛——若推导得比 StarRocks 多，plan 反而
不收敛、过不了 gate。更激进的安全扩展（semi 两侧、anti build 侧、outer 的
null-supplying 侧）记录在 §8 作为已知扩展点。

---

## 2. 目标与非目标

### 目标

- 新增逻辑改写规则 `DeriveJoinNotNullPredicate`，对 Inner/LeftSemi/RightSemi join
  在安全侧的等值 key 列上推导 `IS NOT NULL`。
- 复用现有 predicate pushdown，让推导出的谓词尽量下沉到 scan。
- 支持 `SET disable_optimizer_rules='DeriveJoinNotNullPredicate'` 单独关闭。
- 提供单元测试、golden plan 测试、suite 回归与 FE plan diff 验收。

### 非目标

- 不更新 cardinality / `EXPLAIN COSTS` 行数估算（属 OQ-3）。
- 不处理表达式 key（`f(a)=b`）、`OR`、非等值条件（v1 留作扩展）。
- 不推导 anti / outer / cross join（见 §3 正确性边界）。
- 不消解 `SubqueryAlias`（属 OQ-6），不改 join 顺序（JoinReorder 已有）。

---

## 3. 正确性模型（核心）

### 3.1 推导表

只实现 StarRocks `deriveIsNotNullPredicate` 的三种安全情形：

| `JoinKind` | 左侧 key 加 `IS NOT NULL` | 右侧 key 加 `IS NOT NULL` | 理由 |
|---|:---:|:---:|---|
| `Inner` | ✓ | ✓ | 两侧都必须匹配，NULL key 永不匹配 |
| `LeftSemi` | ✗ | ✓ | 只过滤 build（右）侧，缩小 hash table |
| `RightSemi` | ✓ | ✗ | 对称 |
| `LeftAnti` | ✗ | ✗ | **陷阱**：左行 NULL key 无匹配 → 反而要被输出，不能过滤 |
| `RightAnti` | ✗ | ✗ | 对称 |
| `NullAwareLeftAnti` | ✗ | ✗ | **陷阱**：NOT IN 语义下右侧出现 NULL 会让全部左行不合格，两侧都不能碰 |
| `LeftOuter` / `RightOuter` / `FullOuter` | ✗ | ✗ | 保留侧行必出；即便某侧安全也不照抄，以保 plan 与 StarRocks 收敛 |
| `Cross` | ✗ | ✗ | 无 join key |

对应 StarRocks 源码（`JoinPredicatePushdown.java`，约 328–374 行）：

```java
if (joinType.isAnyInnerJoin() || joinType.isRightSemiJoin()) { /* 推导左侧 key */ }
if (joinType.isAnyInnerJoin() || joinType.isLeftSemiJoin())  { /* 推导右侧 key */ }
```

### 3.2 单列推导的四条 gate

一个 key 列要被推导，必须同时满足：

1. **join 类型 + 侧** 在 §3.1 表中为 ✓。
2. 该 equi-conjunct 是 `plain_col = plain_col` 形式（穿透 `Cast` / `Nested`），从
   `join.condition` 的**顶层 AND 链**抽取。`OR`、非等值、表达式 key 一律跳过。
3. 该列当前**可空**（操作数 `TypedExpr.nullable == true`）——非空列加
   `IS NOT NULL` 无意义。
4. 该列**尚未被 child 谓词脊保证非空**（见 §4.4 幂等）——同时承担幂等职责。

四条过滤后若某侧无列可推，对该侧不动；两侧都无 → 规则对该 join 返回
`Unchanged`。**任何不确定一律不推**（fail-safe：宁可不优化，绝不改变语义）。

---

## 4. 规则结构与算法

### 4.1 文件布局与 helper 抽取

- 新文件 `src/sql/optimizer/rewrite/rules/derive_join_not_null.rs`，实现
  `DeriveJoinNotNullPredicate`。
- 将 `src/sql/optimizer/rewrite/rules/ukfk.rs` 中的 equi-pair 抽取逻辑
  （`Side`、`collect_join_equality_pairs`、`classify_column_ref`，以及
  `join_equality_pairs` 的核心遍历）抽到共享处（`rewrite/rules/utils.rs`，已含
  `combine_and`）。
- **泛化**该 helper：除现有的列名对 `(String, String)`，再返回 join-key 操作数
  表达式对 `Vec<(TypedExpr, TypedExpr)>`（底层 `BinaryOp::Eq` 的两个子表达式本就
  在手）。`ukfk.rs` 两个调用点改用泛化版（它只需名字，从操作数对派生即可）。

> 这是「顺手改进所工作的代码」，不是无关重构：OQ-2 与 `PruneUkFkJoin` 共享同一套
> equi-pair 抽取，抽公共 util 消除重复实现。

### 4.2 `IS NOT NULL` 的构造：克隆 ON 操作数

照抄 StarRocks 的 `new IsNullPredicateOperator(true, c.clone(), true)`：直接 **克隆**
ON 子句里的 join-key 列操作数 `TypedExpr`，包进 `ExprKind::IsNull { negated: true }`。
克隆来的操作数已携带正确的 `data_type` / `nullable` / `qualifier` / `column_id`，
**无需 schema 查找、无需重建 qualifier**。

> 备选：`low_cardinality_dict/rewriter.rs:1245` 的
> `plan_output_columns(&LogicalPlan) -> Vec<OutputColumn>` 可查任意 child 的列类型；
> 但克隆操作数更简单更稳，作为首选。

### 4.3 `apply()` 算法

规则 `matches` 裸 `LogicalPlan::Join(_)`；`apply`：

1. 按 §3.1 表查 `join.join_type` → `(derive_left, derive_right)`；两者皆 false →
   `None`。
2. 从 `join.condition` 顶层 AND 链抽 equi-key 操作数对（复用泛化 helper）；抽不到
   → `None`。
3. 对每个可推导侧，逐 key 操作数过滤：
   - (a) `operand.nullable == true`（非空列跳过）；
   - (b) 该列未被对应 child 的谓词脊保证非空（§4.4）。
4. 过滤后留下的操作数，各自 `clone()` 包进 `ExprKind::IsNull { negated: true }`。
5. 该侧多个 `IS NOT NULL` 用 `combine_and` 串成一个谓词，用 `LogicalPlan::Filter`
   包到对应 child 上。
6. 两侧都没新增 → `None`；否则返回
   `Some(Join { left: new_left, right: new_right, ..join })`。

### 4.4 幂等机制（不给 JoinNode 加字段）

步骤 3(b) 沿 child 的**谓词脊**下行（穿透单输入的 `Filter` / `Project` /
`SubqueryAlias` / `Sort` / `Limit`，直到根 `Scan`），收集 `Filter.predicate` 与
`ScanNode.predicates` 的 AND 拆解项；若已存在某 `IsNull { negated: true }`，其操作数
与目标 key 列**同一身份**（优先比 `column_id`，回退比 qualified 名），则该列已保证
非空、跳过。

收敛过程：规则首次包出 `Filter(IS NOT NULL)` → 同 stage 的 pushdown 把它下沉/合并
→ 再次访问该 join 时步骤 3(b) 命中 → 返回 `Unchanged`。用户原本就写了
`col IS NOT NULL` 时也会被正确识别并跳过。

### 4.5 Pipeline 注册

在 `src/sql/optimizer/rewrite/registry.rs::query_rewrite_pipeline` 的
`PredicatePushdownPostJoin` stage 的 rule 列表**追加** `DeriveJoinNotNullPredicate`
（仅此一处，不进 `PredicatePushdownPreJoin`）。

`RewritePipeline::rewrite`（`pipeline.rs`）对每个 stage 跑到 fixed-point、stage 间
顺序执行一次。因此 derive 与现有 `PushDownPredicateScan` / `PushDownPredicateProject`
在 PostJoin 的同一 fixed-point loop 内收敛：derive 包出 Filter → pushdown 下沉到
scan → 下一轮 derive 命中幂等 → 停。

需同步更新 `registry.rs` 的 `query_pipeline_contains_migrated_query_rules` 测试
期望名单（新增一个 `DeriveJoinNotNullPredicate`）。

### 4.6 disable 接线

规则 `name()` 返回 `"DeriveJoinNotNullPredicate"`，经
`is_known_rewrite_rule_name`（`registry.rs` / `optimizer/mod.rs`）自动纳入校验；
`SET disable_optimizer_rules='DeriveJoinNotNullPredicate'` 即可关闭。**不**引入额外
session 变量（StarRocks 的 `cbo_derive_join_is_null_predicate` 对应物，YAGNI）。

---

## 5. 数据流：q22 before → after

左 = `t1`（probe），右 = `t2 / w1`（build）。`LeftSemi` → 只在右/build 侧 key 推导。

**Before**
```
Aggregate  count(1), count(t1.k1), count(t1.c_tinyint_null)
└─ Join LEFT SEMI  ON t1.c_tinyint_null = t2.c_tinyint_null
   ├─ Scan t1                         ← probe（左）
   └─ SubqueryAlias t2 (= w1)
      └─ Filter k1 < 100
         └─ Scan t1                   ← build（右）：NULL 行（11/12）全进 hash table
```

**After**（derive 在右 child wrap 出 `Filter(c_tinyint_null IS NOT NULL)`，同 loop
被 pushdown 下沉合并）
```
Aggregate  ...
└─ Join LEFT SEMI  ON t1.c_tinyint_null = t2.c_tinyint_null
   ├─ Scan t1                         ← 左侧不动（照抄 StarRocks）
   └─ SubqueryAlias t2
      └─ Filter k1 < 100
         └─ Scan t1  predicates=[k1 < 100, c_tinyint_null IS NOT NULL]
```

**Caveat（必须在实现与 PR 描述中写明）**:能否「下沉到 scan 谓词」取决于现有
pushdown 是否穿透 `SubqueryAlias`（OQ-1 / OQ-6 的地盘）。即便穿不透，缩小 build
hash table 的核心收益也由 §4.3 步骤 5 的 wrap 本身保证（filter 已在 build 输入前
执行）；下沉到 scan 只是把独立 filter 算子变成 scan 谓词，并让 plan 与 StarRocks
完全收敛。**收益不与「下沉到 scan」绑死。**

---

## 6. 边界与 error handling

| 情形 | 处理 |
|---|---|
| 复合 key `a1=b1 AND a2=b2` | inner 两侧各推全部 key（所有 conjunct 必须成立 → 各 key 非空），语义正确 |
| `OR` / 非等值 / 表达式 key `f(a)=b` | helper 抽不出 plain-col 对 → 跳过，v1 不做 |
| Cross / 无 ON 条件 | `None` |
| `NullAwareLeftAnti` | §3.1 表中两侧皆 ✗，完全排除 |
| 非空 key 列 | 步骤 3(a) 跳过 |
| 用户已写 / 上轮已推 `IS NOT NULL` | 步骤 3(b) 幂等跳过 |
| self-join（q22 即是） | 按 per-side 操作数分类解决（操作数对已分别归属左右 child） |
| 与 `PruneUkFkJoin` 共存 | OQ-2 包在 join child 上，`root_scan` 穿透 `Filter`，不破坏其 `Project(Join)` 匹配与表消除；FK 路径重复加的 not-null 被幂等/pushdown 去重，无害 |
| cardinality 显示 | OQ-2 不更新 `EXPLAIN COSTS` 行数（OQ-3）；golden 只断言 `IS NOT NULL` 谓词出现，不断言行数 |

**error handling**:规则为纯结构变换，返回 `Option<LogicalPlan>`，永不报错；
任何不确定一律 `None`（fail-safe）。不引入新失败模式。

---

## 7. 测试与验收

1. **单元测试**（规则级，直接对应 §3.1 表）:
   - Inner 两侧 / LeftSemi 仅右 / RightSemi 仅左 / LeftAnti 无 / RightAnti 无 /
     NullAwareLeftAnti 无 / Outer 三种无 / Cross 无；
   - 复合 key 全推；非空列跳过；**幂等（apply 两次 == 一次）**；
   - `OR` / 表达式 key 跳过；self-join 操作数分类正确。
2. **Golden plan**（`sql-tests/optimizer/`）:
   - `derive_join_not_null_inner.sql`、`derive_join_not_null_leftsemi.sql`，用
     `-- @explain_contains` 断言 `IS NOT NULL` 落在正确侧；
   - 一个 anti-join 负向 case，断言**不出现** `IS NOT NULL`。
3. **Suite 回归**:join / cte / aggregate / filter 跑 `-j 1 --mode verify` 无回归。
   - ⚠️ **前置**:OQ-1（PR #208）今天刚带「documented regressions」落地，改动同一
     条 RBO pipeline。须先建立当前 join suite 基线，再衡量 OQ-2 的 wall_time delta
     （roadmap 目标 -10% ~ -20%），并把新 wall_time 记入 roadmap 进度 section。
4. **FE plan diff**（roadmap PR checklist #3，PR 时人工 gate）:
   - q22（`join_one_key`）、q31（`join_linear_chained`）、一个简单 INNER count(\*)
     三条标杆；
   - 用 `starrocks-fe-on-novarocks` skill 对比（StarRocks FE 跑 9030，NovaRocks 跑
     `$NOVA_ENV_MYSQL_PORT`，经 `docker/iceberg-rest/runtime/current/env.sh`）；
   - 确认 `IS NOT NULL` 落点与 StarRocks 一致。
5. **disable 测试**:`SET disable_optimizer_rules='DeriveJoinNotNullPredicate'` 后
   plan 回退到无推导形态。

测试环境一律 source `docker/iceberg-rest/runtime/current/env.sh`，用
`$NOVAROCKS_STANDALONE_CONFIG` / `$NOVAROCKS_SQL_TEST_CONFIG`，不写死端口 9030
（9030 留给对比用的 StarRocks FE）。

---

## 8. 风险与已知扩展点

### 风险

- **OQ-1 基线不稳**:#208 自述带 correctness regression，OQ-2 改同一 pipeline。
  缓解:实现前先建立 join/cte/aggregate/filter 四套的当前基线快照。
- **pushdown 穿透深度**:若 pushdown 不穿 `SubqueryAlias`，`IS NOT NULL` 停在 alias
  之上而非 scan 谓词，导致与 StarRocks 的 plan 在「谓词落点」上不完全一致。缓解:
  golden 与 FE diff 断言「谓词出现在 build 输入之前」而非死扣 scan 节点；彻底收敛
  依赖 OQ-1 alias 列传播 / OQ-6 alias fold。

### 已知扩展点（v1 不做，照抄 StarRocks 故意留下）

- LeftSemi 的左（probe）侧、RightSemi 的右侧:安全且有收益，StarRocks 未推。
- LeftAnti / RightAnti 的非保留（build）侧:安全。
- LeftOuter / RightOuter 的 null-supplying 侧:安全。
- 表达式 key（`f(a)=b`）的 null-rejecting 推导。

启用任一扩展都会让 NovaRocks plan 偏离 StarRocks，需相应放宽 plan-convergence gate，
故单独作为后续任务评估。

---

## 9. StarRocks 参考出处

- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rewrite/JoinPredicatePushdown.java`
  - `deriveIsNotNullPredicate`（约 328–374 行）— 推导主逻辑与 join 类型分支。
  - `pushdownOnPredicate`（约 233–271 行）— 调用点；equi-conjunct 与其他谓词分离。
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/JoinHelper.java`
  - `getEqualsPredicate` / `isEqualBinaryPredicate`（约 270–336 行）— equi-conjunct
    抽取与左右 child 归属。
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/PushDownPredicateJoinRule.java`
  — 触发壳。
- session 变量 `cbo_derive_join_is_null_predicate`（默认开）。

---

## 10. 实现步骤概览（交由 writing-plans 细化）

1. 抽取并泛化 equi-pair helper 到 `rewrite/rules/utils.rs`，适配 `ukfk.rs` 调用点。
2. 实现 `DeriveJoinNotNullPredicate`（join 类型表、四条 gate、克隆操作数、谓词脊
   幂等）。
3. 注册进 `PredicatePushdownPostJoin` stage，更新 pipeline 名单测试。
4. 单元测试（覆盖 §7.1 全部分支）。
5. Golden plan 正/负向 case。
6. Suite 回归 + 基线快照 + FE plan diff，更新 roadmap 进度。
