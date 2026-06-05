# OQ-9 · Residual predicate factoring and placement design

- 日期: 2026-06-05
- 对应 roadmap: `OQ-9 · Residual predicate factoring and placement`
- 状态: Spec - pending implementation plan
- 目标验收查询: `tpc-ds/q85` 在 dev profile 下可在 `--query-timeout 180` 内 verify 通过
- 参考实现: `/Users/harbor/project/starrocks`, FE optimizer predicate pushdown / predicate move-around

## 1. 一句话

把 NovaRocks 当前局部的 `Filter(Join)` predicate pushdown 升级为 StarRocks-style 的通用 join predicate placement 主线: top-level AND 拆成 predicate group, OR group 保持原子, 基于 `ColumnId` 判断最低合法覆盖位置, 对 inner/cross join 做安全的 range/equivalence/OR side-filter 派生, 并在 join reorder 前后多轮运行, 让 stats、runtime filter 和 distribution search 更早看到真实过滤。

`tpc-ds/q85` 是硬验收目标, 但实现不能为 q85 特判表名或列名。

## 2. 当前基线和问题

本 worktree 在 dev profile 下复现:

- standalone-server 使用 `target/debug/novarocks`.
- `tpc-ds/q85` 运行 132.80s 后失败:
  `pipeline pull from operator EXCHANGE_SOURCE ... failed: exchange timeout waiting for senders`.
- `EXPLAIN VERBOSE` 显示 OQ-8 已经让 q85 出现 `PARTITIONED` join 和 `ShuffleJoin` exchange。
- 仍然存在 OQ-9 症状:
  - demographics OR residual 位于较高的 join `other` predicate。
  - demographics residual 文本出现重复组合。
  - `customer_demographics cd2` 侧只看到 `IS NOT NULL`, 没看到由 `cd1` branch filter 和 `cd1 = cd2` 派生出的 side filter。
  - `web_sales` scan predicate 已有部分 range 过滤, 但 OR/AND 结构和 envelope 派生需要规范化。

现有 NovaRocks 入口:

- `src/sql/optimizer/rewrite/rules/predicate_pushdown/push_to_join.rs`
- `src/sql/optimizer/rewrite/rules/predicate_pushdown/mod.rs`
- `src/sql/optimizer/rewrite/registry.rs`
- `src/sql/optimizer/rewrite/rules/utils.rs`

当前 pipeline 是:

```text
PredicatePushdownPreJoin
JoinReorder
PredicatePushdownPostJoin
AggregatePushdown
TagRequiredColumns
ColumnPruning
LowCardinalityDictionaryRewrite
```

这个顺序已有 pre/post join reorder pushdown, 但缺少 StarRocks 的 join-on pushdown、range/equivalence derive、predicate move-around 后再次 pushdown 的补偿阶段。

## 3. StarRocks 参考结论

本设计参考 StarRocks 的结构, 不逐行复制 Java 实现。

关键观察:

1. `Utils.extractConjuncts` 只拆 top-level AND, 不拆 OR。OR group 保持原子 predicate。
2. `PushDownPredicateJoinRule` 只是薄 wrapper, 具体逻辑在 `JoinPredicatePushdown`。
3. `JoinPredicatePushdown` 同时处理 filter predicate 和 join on predicate:
   - 对 inner/cross join, 合并 join on、filter、join predicate 做 range/equivalence derive。
   - 按 child output `ColumnRefSet` 分类到 left/right/join residual。
   - 对 outer/semi/anti/null-aware anti join 有明确 guard。
   - 对可消除 nullable side NULL 行的 outer join, 可安全转 inner。
4. StarRocks 在 logical rewrite 后段运行 `OnPredicateMoveAroundRule`, 根据 join on 中的等值/范围谓词和 child domain property 派生另一侧 filter, 然后再次运行 predicate pushdown。
5. StarRocks 有 `SplitJoinORToUnionRule`, 但它只处理 join ON 中 OR 的每个 disjunct 都是跨侧等值谓词的窄场景, 不适合作为 q85 主路径。
6. StarRocks TPC-DS 同族测试 `expressionExtract` 断言可以从复杂 OR 中提取事实表 range 边界, 例如 `sales_price` 和 `net_profit` 的保守 min/max envelope。

设计结论:

- NovaRocks 首版不把 predicate group 纳入 Cascades memo/search。
- 继续以 logical rewrite 为主, 但要从单条局部规则升级为可复用 join predicate pushdown 子系统。
- OR 不做 DNF 展开。只做安全 side-filter / range envelope 派生。

## 4. 目标

1. top-level AND predicate 能拆成独立 predicate group。
2. OR predicate group 不被任意展开, 作为 placement 原子。
3. 基于 `ColumnId` / output id 判断 predicate group 可覆盖位置, 避免裸列名误判 self-join。
4. `Filter(Join)` 和 `Join.condition` 都进入同一套 join predicate pushdown 逻辑。
5. inner/cross join 支持:
   - 单侧 predicate 下推。
   - 跨侧 predicate 保留为 join residual。
   - cross join 有 join condition 时升级 inner join。
   - range/equivalence derive。
   - OR branch 安全 side-filter 派生。
6. outer/semi/anti/null-aware anti join 有显式 guard, 不改变 SQL 三值逻辑。
7. canonical dedup 防止 q85 当前的重复 residual。
8. rewrite pipeline 增加 move-around / derive 后再次 pushdown 阶段, 让派生 predicate 影响 stats 和 physical search。
9. `tpc-ds/q85` 在 dev profile 下 verify 通过。

## 5. 非目标

- 不为 q85 硬编码表名、列名或 plan shape。
- 不做任意 DNF/CNF 爆炸式重写。
- 不把 predicate group 作为 Cascades memo/search 的一等成员。
- 不在 OQ-9 首版迁移 StarRocks 的完整 materialized view predicate split 体系。
- 不把 `SplitJoinORToUnion` 作为 q85 主路径。
- 不改变表达式执行语义或 SQL 三值逻辑。

## 6. 推荐方案

采用 StarRocks-inspired 方案 2: predicate group + 多阶段 join predicate pushdown。

放弃的两个方向:

- 只增强现有 `PushDownPredicateJoin`: 改动小, 但无法覆盖 join-on derive 和 post-reorder move-around, q85 仍容易依赖偶然 join order。
- 深度并入 Cascades search: 理论更完整, 但会牵动 memo、cost、stats 和 extract, 超出 OQ-9 首版的风险边界。

推荐方案的核心结构:

```text
PredicateGroup
  -> PredicateClassifier
  -> JoinPredicateDeriver
  -> JoinPredicatePushdown
  -> PredicateMoveAroundRule
  -> existing fixed-point pushdown to scan/project/aggregate
```

## 7. 组件设计

### 7.1 PredicateGroup

新增内部模型, 建议放在:

- `src/sql/optimizer/rewrite/rules/predicate_pushdown/predicate_group.rs`

职责:

- 拆 top-level AND。
- 保持 OR group 原子。
- 记录 referenced `ColumnId` set。
- 记录 canonical key。
- 标记 origin:
  - `Filter`
  - `JoinCondition`
  - `Derived`
- 标记 derived kind:
  - `None`
  - `Equivalence`
  - `RangeEnvelope`
  - `OrSideFilter`
  - `NotNull`

字段草案:

```rust
struct PredicateGroup {
    expr: TypedExpr,
    referenced_ids: BTreeSet<ColumnId>,
    key: PredicateKey,
    origin: PredicateOrigin,
    derived: PredicateDerivedKind,
}
```

`PredicateKey` 第一版可以使用稳定 debug rendering + normalized AND/OR child ordering。实现时要避免把不同语义的表达式错误合并。

### 7.2 PredicateClassifier

职责:

- 输入 join type、left/right output `ColumnId` set、predicate groups。
- 输出:
  - `left_pushdown`
  - `right_pushdown`
  - `join_residual`
  - `remain_above_join`

分类原则:

- referenced ids 为空的常量 predicate:
  - inner/cross 可推两侧或保留为 join residual。
  - outer/full/semi/anti 走 guard。
- ids 完全属于左 child: 候选 left pushdown。
- ids 完全属于右 child: 候选 right pushdown。
- ids 同时涉及左右 child: join residual。
- ids 不完全被当前 join 覆盖: remain above join。

所有外连接和 semi/anti join 都必须通过 safety guard。

### 7.3 JoinPredicateDeriver

职责:

- 基于 join condition、filter groups、child existing predicates 做安全派生。
- 输出 derived `PredicateGroup`, 不直接改 plan。

首版支持:

1. 等价派生

```text
a = b AND a = const  -> b = const
a = b AND a IN (...) -> b IN (...)
```

2. range 派生

```text
a = b AND a BETWEEN low AND high -> b BETWEEN low AND high
a = b AND a >= low -> b >= low
```

3. OR branch side-filter 派生

对 OR 的每个 branch 独立分析, 只生成必要条件:

```text
(a=b AND a='M') OR (a=b AND a='S')
  -> b IN ('M', 'S')       -- derived side filter
  原 OR 保留为 join residual

(x BETWEEN 100 AND 150)
OR (x BETWEEN 50 AND 100)
OR (x BETWEEN 150 AND 200)
  -> x BETWEEN 50 AND 200  -- range envelope
```

q85 目标:

- demographics OR:
  - 原 OR 保留在最早覆盖 `cd1/cd2/web_sales` 的 join。
  - 派生 `cd2` 的 marital/education side filter。
  - 派生 `web_sales.ws_sales_price` envelope。
- address OR:
  - 原 OR 保留在最早覆盖 `customer_address/web_sales` 的 join。
  - 派生 address side filter。
  - 派生 `web_sales.ws_net_profit` envelope。

4. not-null 派生

沿用现有 `DeriveJoinNotNullPredicate` 思路, 但要和 canonical key 去重。

### 7.4 JoinPredicatePushdown

建议重构当前 `push_predicates_through_join` 为内部 struct/function, 同时支持:

- `Filter(Join)` 入口。
- `Join.condition` 入口。

处理步骤:

1. 构建 predicate groups。
2. 构建 derive context。
3. 执行 derivation。
4. 合并原始 groups + derived groups 并去重。
5. 分类 placement。
6. 重建 join:
   - left/right child 需要下推时包 `LogicalPlan::Filter`。
   - join residual 合并到 `JoinNode.condition`。
   - remain above join 用 `wrap_remaining_filter` 包回上层。
7. 对 cross join, 如果 condition 非空则升级 inner join。

### 7.5 PredicateMoveAroundRule

新增 logical rewrite rule, 建议放在:

- `src/sql/optimizer/rewrite/rules/predicate_pushdown/move_around.rs`

职责:

- 在 post-join pushdown 后运行。
- 看 join condition 中的等值/范围 predicate。
- 看 left/right child 已有 filter/scan predicates 的 domain。
- 推导另一侧可安全下推的 predicate。
- 生成 child `Filter`, 交给现有 pushdown fixed-point 下沉。

首版 domain 可以保守:

- 从 child filter/scan predicate 提取:
  - equality
  - IN list
  - BETWEEN
  - >= / > / <= / <
  - OR branch envelope
- 不支持的函数表达式跳过。

## 8. Rewrite pipeline 调整

当前 pipeline:

```text
PredicatePushdownPreJoin
JoinReorder
PredicatePushdownPostJoin
AggregatePushdown
...
```

目标 pipeline:

```text
PredicatePushdownPreJoin
JoinReorder
PredicatePushdownPostJoin
PredicateMoveAround
PredicatePushdownAfterMoveAround
AggregatePushdown
TagRequiredColumns
ColumnPruning
LowCardinalityDictionaryRewrite
```

说明:

- pre-join pushdown 让 join reorder 看到更多 scan-side filter。
- post-join pushdown 修复 join reorder 改变 tree 后的最低覆盖位置。
- move-around 基于最终 join tree 做跨侧派生。
- after-move-around pushdown 把派生 predicate 下沉到 scan/project/aggregate。

新增规则名要能通过 `disable_optimizer_rules` 关闭:

- `JoinPredicateMoveAround`
- 如果拆成独立 deriver 规则, 使用 `JoinPredicateDerive`。

现有 `PushDownPredicateJoin` 名称继续保留, 但内部行为增强。

## 9. 语义安全

### 9.1 OR 安全

- OR group 是 placement 原子。
- 不做任意 DNF 展开。
- 只做必要条件派生:
  - range envelope。
  - equality / IN side filter。
  - branch 全覆盖时才派生。
- 原 OR residual 必须保留在最低覆盖 join, 派生 predicate 不替代原 predicate。

### 9.2 Inner / cross

- 单侧 predicate 可下推。
- 跨侧 predicate 留 join residual。
- cross join 若 condition 非空可升级 inner。
- 派生 predicate 允许推到两侧, 但需要 canonical 去重。

### 9.3 Outer join

- preserved side 不做会减少输出行的错误下推。
- nullable side 只有在 ON 语义或 null-elimination 证明安全时下推。
- filter predicate 如果能证明消除 nullable NULL 行, 可转 inner。
- full outer 第一版最保守, 常量 predicate 之外不做 aggressive pushdown。

### 9.4 Semi / anti

- semi join 可推参与侧, 但要保留输出语义。
- anti join 保守, 不把会改变 no-match 语义的 predicate 推到错误侧。
- null-aware anti 单独 guard。无法证明安全时不下推。

### 9.5 非确定性表达式

- 含非确定性函数的 group 不复制、不派生。
- 可以在语义等价位置移动, 但首版默认保守保留。

### 9.6 ColumnId 优先

- 侧归属必须优先使用 `ColumnId`。
- `ColumnId::UNSET` 或缺失时, 只能走保守 fallback。
- 裸列名不能作为 self-join 正确性的唯一依据。

## 10. q85 预期 plan 变化

验收不要求和 StarRocks 文本完全一致, 但需要满足:

- 不再出现重复 demographics residual。
- demographics OR residual 不再靠近 top aggregate, 而是落在最早覆盖 `cd1/cd2/web_sales` 的 join。
- `customer_demographics cd2` 或其上方 filter 能看到由 `cd1 = cd2` 和 OR branch 派生的 marital/education side filter。
- `web_sales` 能看到 `ws_sales_price` 和 `ws_net_profit` 的保守 range envelope。
- address OR residual 继续落在较低的 address/web_sales 相关 join 附近。
- `EXCHANGE_SOURCE` timeout 不再出现。

## 11. 测试计划

### 11.1 Rust unit tests

新增 focused tests:

- `PredicateGroup`:
  - top-level AND 拆分。
  - OR 保持原子。
  - canonical key 去重。
- `PredicateClassifier`:
  - inner/cross left/right/join/above 分类。
  - self-join 使用 ColumnId 分类。
  - outer preserved side guard。
  - semi/anti/null-aware anti guard。
- `JoinPredicateDeriver`:
  - equality derive。
  - range derive。
  - OR branch equality side filter。
  - OR branch range envelope。
  - 非确定性表达式不派生。
- `JoinPredicateMoveAround`:
  - child domain -> opposite side filter。
  - redundant predicate removal。

### 11.2 Optimizer SQL golden

新增 `sql-tests/optimizer` cases:

- residual placement lowest-cover join。
- OR group 不展开。
- OR side filter derivation。
- range envelope derivation。
- outer join preserved side guard。
- semi/anti guard。
- `disable_optimizer_rules` 覆盖新增规则。

### 11.3 q85 and focused suite validation

命令示例:

```bash
cargo test --lib predicate_group
cargo test --lib join_predicate
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite optimizer --mode verify
```

启动 dev standalone-server:

```bash
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost \
cargo run -- standalone-server --config "$NOVAROCKS_STANDALONE_CONFIG"
```

q85 验收:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite tpc-ds --only q85 --mode verify --query-timeout 180 -j 1
```

同族 sanity:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite tpc-ds --only q48,q64,q85 --mode verify --query-timeout 180 -j 1
```

## 12. 风险和缓解

### 风险: OR side filter 过强

缓解:

- 原 OR residual 永远保留。
- derived predicate 只生成必要条件。
- 每个 OR branch 都必须能贡献同一目标侧 predicate, 否则不派生。

### 风险: outer/semi/anti 语义回归

缓解:

- 首版 guard 保守。
- 为每类 join 添加 plan golden。
- null-aware anti 无法证明安全时不下推。

### 风险: duplicate predicate 导致 plan 膨胀

缓解:

- canonical key 去重。
- derived predicate 标记 origin。
- 合并 join condition / child filter 前去重。

### 风险: rewrite fixed-point 不收敛

缓解:

- derived predicate 必须标记并去重。
- 对同一 join / child 不重复派生相同 key。
- 依赖现有 `max_iterations` 作为最后防线。

### 风险: stats 变化导致 join order 波动

缓解:

- golden 只断言关键 shape, 不锁死整棵 plan。
- q85 是执行验收, 不是文本完全一致验收。
- 必要时用 `disable_optimizer_rules` bisect。

## 13. 实施顺序建议

1. 新增 `PredicateGroup` 和 canonical 去重, 只接入单测。
2. 抽出 `PredicateClassifier`, 用现有 `PushDownPredicateJoin` 行为做等价迁移。
3. 接入 `Filter(Join)` 新实现, 保持现有 optimizer tests 通过。
4. 增加 join condition 入口, 处理 ON predicate pushdown。
5. 增加 equivalence/range deriver。
6. 增加 OR side-filter / range envelope deriver。
7. 增加 `PredicateMoveAroundRule` 和 pipeline stage。
8. 加 optimizer SQL golden。
9. 跑 q85 dev verify, 根据 plan 差异补最小必要修正。

## 14. 验收标准

- 新增 design 对应实现不含 q85 表名/列名特判。
- `cargo test --lib predicate_group` 通过。
- `cargo test --lib join_predicate` 通过。
- `sql-tests/optimizer` verify 通过。
- `tpc-ds/q85` 在 dev profile 下 `--query-timeout 180` verify 通过。
- q85 `EXPLAIN VERBOSE` 满足:
  - demographics residual 无重复。
  - OR residual 位置比当前 top join 更早。
  - cd2 / web_sales / address side filter 派生可见。
- focused q48/q64/q85 sanity 不出现新 timeout 或 correctness failure。
