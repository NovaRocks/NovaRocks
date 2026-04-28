# Iceberg 聚合 MV 增量维护 Phase 2（APPEND + DELETE）设计

**Status:** Draft for user review
**Date:** 2026-04-29
**Builds on:**
- `docs/superpowers/specs/2026-04-23-mv-on-iceberg-phase1-design.md`
- `docs/superpowers/specs/2026-04-24-mv-on-iceberg-phase2-design.md`
- `docs/superpowers/specs/2026-04-26-mv-on-iceberg-aggregate-ivm-design.md`

> 注：本设计中的 "Phase 2" 是 iceberg-IVM 系列内部编号（即 aggregate IVM 的第二阶段），
> 与 `2026-04-24-mv-on-iceberg-phase2-design.md` 中的 Phase 2（projection/filter）不是同一编号空间。

## 1. 目标

把 NovaRocks 的单表 Iceberg 聚合物化视图增量维护从 append-only 扩展到支持 base 端
**APPEND + position-DELETE** 的混合变化集，落地一个最小可用的 IVM 形态：

- base 表是 iceberg v2，allowed snapshot operation：`append`、`delete`、`replace`（compaction，按 no-op 跳过）；
- MV 形状仍然是 `GROUP BY` + 可逆聚合（`count` / `sum`）；
- `REFRESH MATERIALIZED VIEW` 把 base lineage 上的 append/delete 翻译成 base 端 Change 流，
  经聚合算子产生 ΔAgg，再 LEFT OUTER JOIN 现有 MV 行得出 ΔMV（DELETE+INSERT pair），
  最后由新的 `AggregateApplyChanges` 算子原子写回；
- 全程 **STRICT fail-fast**：不识别的 snapshot operation、broken lineage、equality-delete、
  v3 deletion vector、schema evolution 一律拒绝刷新，不做 fallback。

## 2. 范围

### 2.1 In-scope（本期）

- 单个 iceberg v2 base table 的聚合 MV；
- snapshot lineage 上：`append` / `delete` / `replace`（仅 compaction，校验后跳过）；
- 可逆聚合：`count(*)` / `count(col)` / `sum(col)`；
- 新增 `CREATE MATERIALIZED VIEW … PRIMARY KEY (col, …)` 子句，PK 列用作 base 端 ROW_ID 来源；
- 手动 `REFRESH MATERIALIZED VIEW`，幂等：同一 `current_snapshot` 重复 refresh 等价 no-op。

### 2.2 Out-of-scope（后续 phase）

- equality-delete / v3 deletion vector；
- `overwrite` snapshot；
- 不可逆聚合（`min` / `max` 等）；
- JOIN MV、multi-source MV、nested MV；
- schema evolution 自动追随；
- query rewrite、async / scheduled refresh、partition-level refresh。

## 3. 用户可见语义

```sql
CREATE MATERIALIZED VIEW agg_mv
PRIMARY KEY (order_id)              -- 本期新增；base ROW_ID 来源
AS
SELECT customer, count(*) AS c, sum(amount) AS s
FROM iceberg.shop.orders
WHERE amount > 0
GROUP BY customer;
```

`PRIMARY KEY` 子句的列必须是 base table schema 中的列，且 NOT NULL。
PK 列**不要求**出现在 SELECT 列表，但 lowering 阶段会把它作为隐式投影列下推。

`REFRESH MATERIALIZED VIEW agg_mv` 行为：

| 条件 | 行为 |
|---|---|
| 没有 stored snapshot | 走 phase 1 全量 refresh |
| `current_snapshot == stored_snapshot` | no-op |
| lineage 上全是 `append` / `replace`（compaction） | 走本期 IVM 路径，replace 跳过 |
| lineage 上含 `delete`（position-delete） | 走本期 IVM 路径，反向投影 base 行 |
| lineage 上含 `overwrite` / 未知 op / equality-delete / DV | 拒绝，返回错误 |
| lineage broken（`previous_snapshot` 已 expire） | 拒绝，返回错误 |

## 4. 整体数据流

```text
REFRESH MV
  │
  ├─ plan_changes(table, prev_snap, pk_cols)
  │     └─ 沿 lineage 遍历 → IcebergChangeBatch
  │           { inserts: [data files],
  │             deletes: [position-delete refs] }
  │
  ├─ Base-side Change scan
  │     ├─ INSERT 路径:scan data file → 投影 visible+PK → action=Insert
  │     └─ DELETE 路径:读 pos-delete → 反查原 base 行
  │             → 复算 WHERE → 投影 visible+PK → action=Delete
  │
  ├─ Aggregate operator(可逆)
  │     输入:base 端 (cols, action) Change 流
  │     输出:ΔAgg(group_keys, count_delta, sum_delta, …)
  │
  ├─ LEFT OUTER JOIN ΔAgg ⨝ current MV ON hash(group_keys)
  │     输出 ΔMV:对每个命中行 emit pair
  │       { Delete(old MV row, row_id),
  │         Insert(new MV row, row_id) }   # 当 cnt0>0 且 cnt1>0
  │     边界处理:cnt0==0 仅 Insert;cnt1==0 仅 Delete;两者皆 0 跳过
  │
  └─ AggregateApplyChanges
        按 (row_id, action) 排序后顺序应用到 MV 物理表
        原子提交 last_refresh_snapshots[base] = current_snap
```

## 5. plan_changes 接口与 lineage 分类

替换现有 `plan_append_delta`，新模块 `src/connector/iceberg/changes.rs`：

```rust
pub(crate) struct IcebergChangeBatch {
    pub previous_snapshot_id: i64,
    pub current_snapshot_id: i64,
    pub inserts: Vec<DataFileRef>,
    pub deletes: Vec<PositionDeleteRef>,
}

pub(crate) fn plan_changes(
    table: &iceberg::table::Table,
    previous_snapshot_id: i64,
    pk_columns: &[String],
) -> Result<IcebergChangeBatch, ChangeError>;
```

从 `current_snapshot` 沿 parent 链回溯到 `previous_snapshot`：

| Operation | 处理 | 备注 |
|---|---|---|
| `append` | 收 `added_snapshot_id == this` 的 data files 进 `inserts` | 与原 plan_append_delta 一致 |
| `delete` | 收 position-delete files + 受影响 base 文件引用进 `deletes` | 拒绝 v3 DV、equality-delete |
| `replace`（compaction） | **跳过**，校验通过即可 | 见 §5.1 |
| `overwrite` | **拒绝** `UnsupportedOperation` | phase 6+ |
| 其他 / 未知 | **拒绝** `UnsupportedOperation` | |

### 5.1 为什么 REPLACE 可以安全跳过

REPLACE 重写后的新 file 的 `added_snapshot_id = REPLACE snap`，
本期 lineage 遍历对 REPLACE 不收集 inserts；老 file 的 `added_snapshot_id`
仍指向原 APPEND，那次已经在历史 refresh 中被计入。所以跳过 REPLACE
**既不丢数据也不双计**。

REPLACE snapshot 必须通过校验：

- `total-records` 不变；
- `added-data-files` 与 `removed-data-files` 同时非空；
- 没有 schema-id change。

任一不满足 → `ReplaceValidationFailed`，STRICT fail-fast。

## 6. ChangeError 错误分类

```rust
pub(crate) enum ChangeError {
    LineageBroken { previous_snapshot: i64 },
    UnsupportedOperation { snapshot_id: i64, op: String },
    EqualityDeleteUnsupported { snapshot_id: i64 },
    DeletionVectorUnsupported { snapshot_id: i64 },
    SchemaEvolutionUnsupported { detail: String },
    ReplaceValidationFailed { snapshot_id: i64, reason: String },
    PrimaryKeyMissingFromBase { pk_col: String },
    PrimaryKeyNullable { pk_col: String },
    PrimaryKeyValueNull { row_info: String },
    InternalInconsistency(String),
}
```

CREATE-time 错误（`PrimaryKey*`）在 `mv_ddl.rs` 校验阶段抛出。运行时错误在
`plan_changes` 或扫描阶段抛出。所有错误一律 STRICT fail-fast，不自动 fallback。

## 7. 端到端示例

### Setup

```sql
CREATE TABLE iceberg.shop.orders (
    order_id BIGINT NOT NULL,
    customer STRING,
    amount BIGINT
) USING iceberg;

CREATE MATERIALIZED VIEW agg_mv
PRIMARY KEY (order_id)
AS
SELECT customer, count(*) AS c, sum(amount) AS s
FROM iceberg.shop.orders
WHERE amount > 0
GROUP BY customer;
```

### 初始（snapshot s1，首次全量 refresh 完成后）

```text
Base @ s1:
  order_id=1, customer=A, amount=100
  order_id=2, customer=A, amount=200
  order_id=3, customer=B, amount=50
  order_id=4, customer=B, amount=-10   ← 被 WHERE 排除

MV agg_mv 物理:
  __row_id__   customer  c    s
  hash(A)      A         2    300
  hash(B)      B         1    50
```

### 增量：`DELETE FROM orders WHERE order_id = 1` → snapshot s2

`REFRESH MATERIALIZED VIEW agg_mv`：

```text
1. plan_changes(table, s1, ["order_id"])
   → IcebergChangeBatch {
       previous=s1, current=s2,
       inserts=[],
       deletes=[PositionDeleteRef { delete_file, affected=file_001 }],
     }

2. Base-side DELETE scan:
   - 读 pos-delete → [(file_001, 0)]
   - 反查 file_001 pos=0 → (order_id=1, customer=A, amount=100)
   - 复算 WHERE amount>0 → 通过
   - 投影 visible+PK → (customer=A, amount=100, order_id=1)
   - 计算 row_id = hash(order_id=1)
   - emit: cols=(A, 100), row_id=hash(1), action=Delete

3. Aggregate ΔAgg:
   GROUP BY customer, sum(±count) / sum(value × ±1)
   → (customer=A, count_delta=-1, sum_delta=-100)

4. LEFT OUTER JOIN ΔAgg ⨝ MV ON hash(customer):
   命中 customer=A:
     旧 (c=2, s=300) → 新 (c=1, s=200)
     cnt0=2, cnt1=1, 都 >0 → emit pair

5. ΔMV:
   Delete { (A, 2, 300), row_id=hash(A) }
   Insert { (A, 1, 200), row_id=hash(A) }

6. AggregateApplyChanges:
   sort by (row_id, action), 顺序应用
   更新 last_refresh_snapshots[orders] = s2

Final:
  __row_id__   customer  c    s
  hash(A)      A         1    200
  hash(B)      B         1    50
```

### 关键细节

1. PK 列在 SELECT 输出里没有也无妨：`order_id` 不在 visible cols 里，但 plan_changes / base scan 必须把它拉出来用于 row_id 计算。
2. **DELETE+INSERT pair**（不是 UPSERT），跟 StarRocks IVM 模型对齐；同一 row_id 的 [Delete, Insert] 由 AggregateApplyChanges 内部按序原子应用。
3. 没有命中 MV 的 ΔAgg 行（cnt0=0）→ 跳过 Delete，仅 Insert；cnt1=0 → 仅 Delete；两者都 0 → 整组跳过。
4. WHERE 在 retract 路径必须 **重新 apply**：DELETE 的行如果原本被 WHERE 过滤，根本不进 ΔAgg，自动正确。

## 8. CREATE MV 校验（DDL 阶段，fail-fast）

位置：`src/lower/mv_ddl.rs`（新增）或扩展现有 MV DDL 校验入口。

校验顺序（任一失败立即返回对应 `ChangeError`）：

1. PK 列存在于 base table schema → `PrimaryKeyMissingFromBase`
2. PK 列在 base 必须 NOT NULL → `PrimaryKeyNullable`
3. PK 列类型必须是可哈希标量（BIGINT/INT/STRING/DATE/DATETIME/DECIMAL），不接受 ARRAY/MAP/STRUCT/JSON
4. base table 必须是 iceberg v2 → 否则 `UnsupportedOperation { op: "iceberg-format-v<n>" }`
5. query 中没有非确定性函数（rand/now/uuid 等） → `SchemaEvolutionUnsupported`
6. 顶层是 GROUP BY + 可逆聚合（`count` / `sum`），其他 → `UnsupportedOperation`

PK 列即使不出现在 SELECT 列表，lowering 也必须把它追加为 base scan 的隐式投影列，标记 `required_for_row_id = true`：

- 不出现在 MV 物理 schema；
- 不参与聚合 group key 之外的语义；
- 仅用于 base 端 Change 的 row_id 计算。

## 9. RuntimeState 扩展

`src/runtime/runtime_state.rs` 增 query 级别字段：

```rust
pub ivm_context: Option<IvmContext>,

pub struct IvmContext {
    pub mv_id: i64,
    pub last_refresh_snapshots: HashMap<String, i64>,
    pub pk_cols_per_base: HashMap<String, Vec<String>>,
    pub strict_mode: bool,   // 默认 true
}
```

- `last_refresh_snapshots` 由 refresh 调度器从 MV catalog 读取后注入；
- 算子（base scan、aggregate、apply changes）通过 `runtime_state.ivm_context` 获取；
- `strict_mode == true` 表示所有 ChangeError 直接抛出，不 fallback。

## 10. 测试策略

### 10.1 单元测试

| 模块 | 用例 |
|---|---|
| `plan_changes` lineage 遍历 | append → ok / delete → ok / replace → skip / overwrite → err / lineage broken → err |
| `plan_changes` REPLACE 校验 | total-records 不变 ok / 不变但 schema-id 变 → err / records 变 → err |
| `mv_ddl` CREATE 校验 | PK 缺列 / PK nullable / PK 非标量 / v1 表 / 非确定函数 |
| Aggregate ΔAgg | INSERT-only / DELETE-only / 混合 / 全部抵消 / 新 group key |
| AggregateApplyChanges | DELETE+INSERT 同 row_id 顺序 / 不存在 row_id 的 DELETE → err |

### 10.2 集成测试（sql-tests 新 suite `mv-ivm`）

布局：`tests/sql-test-runner/cases/mv-ivm/`

```
01_create_mv_basic.sql            CREATE/SELECT/DROP smoke
02_create_mv_pk_invalid.sql       各种 CREATE 失败路径
03_refresh_append_only.sql        仅 INSERT 的 base 变化
04_refresh_delete.sql             base DELETE → MV 收缩
05_refresh_mixed.sql              INSERT + DELETE 同 refresh
06_refresh_replace_skip.sql       compaction 后 refresh = no-op
07_refresh_overwrite_err.sql      base OVERWRITE → 拒绝
08_refresh_lineage_broken_err.sql snapshot expire → 拒绝
09_refresh_idempotent.sql         同 snap 二次 refresh = no-op
10_refresh_concurrent_writes.sql  refresh 期间 base 又前进一步
```

每个 `.sql` 配套 `.expected`，走 `--mode verify`。

### 10.3 手动验证

debug build 跑 `09 + 04` 作为最小回归集；release build 跑全 `mv-ivm` suite。

## 11. PR 拆分

每 PR 独立 mergeable，前 5 个 PR 不暴露用户可见 IVM；PR-6 一并打开：

1. **PR-1**：`mv_ddl.rs` PRIMARY KEY 解析 + 校验，新错误类型，CREATE-time fail-fast；只跑 `02_create_mv_pk_invalid`。
2. **PR-2**：`changes.rs` 重构（plan_append_delta → plan_changes），REPLACE / UNSUPPORTED 分类；不接 delete 数据路径，仅 lineage 报告。
3. **PR-3**：base 端 Change 生成（position-delete 反向投影 + WHERE 复算 + row_id 计算）；接到现有聚合算子。
4. **PR-4**：Aggregate 可逆聚合 ΔAgg 输出；新算子 `AggregateApplyChanges`。
5. **PR-5**：MV catalog 中 `last_refresh_snapshots` 字段 + refresh 调度器接线 + idempotency（case 09）。
6. **PR-6**：完整 sql-tests `mv-ivm` suite 通过。

## 12. 路线图占位

| Phase | 范围 | 状态 |
|---|---|---|
| 1 | append-only IVM（已有 plan_append_delta） | 已存在 |
| **2（本期）** | append + delete，可逆聚合，REPLACE skip，PK 子句 | 设计中 |
| 3 | overwrite 增量；min/max（带回退） | 占位 |
| 4 | equality-delete + v3 DV | 占位 |
| 5 | JOIN MV（带 PK from each side） | 占位 |
| 6 | multi-source MV、nested MV、schema evolution | 占位 |

每个后续 Phase 落地前都需要单独 spec；本设计严格停在 Phase 2。
