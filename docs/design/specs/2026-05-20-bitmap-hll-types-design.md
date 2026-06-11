# BITMAP / HLL Types — Design (PR-B2)

- 创建日期：2026-05-20
- 关联 TODO：PR-B2（BITMAP 类型支持）
- 解锁用例：`analytic_test_bitmap_union_window`、`analytic_test_window_hll_bitmap`
- 后续依赖：INT-3（AGGREGATE KEY 状态列端到端 + INSERT VALUES 含函数路由）

## 1. 背景

NovaRocks 当前执行层已经具备 BITMAP / HLL 的绝大部分能力：

- StarRocks SeriV2 二进制格式 helpers：`src/exec/expr/function/object/bitmap_common.rs`
- BITMAP scalar 函数：`to_bitmap` / `bitmap_empty` / `bitmap_from_string` / `bitmap_count` /
  `bitmap_min` / `bitmap_max` / `bitmap_and` / `bitmap_has_any` / `sub_bitmap` /
  `bitmap_subset_limit` / `bitmap_subset_in_range` / `bitmap_to_binary` /
  `bitmap_from_binary` / `bitmap_to_base64` / `bitmap_to_array` / `bitmap_to_string` /
  `array_to_bitmap`
- HLL scalar 函数：`hll_empty` / `hll_serialize` / `hll_deserialize` / `hll_cardinality` /
  `hll_hash`
- BITMAP / HLL aggregate：`bitmap_union` / `bitmap_union_int` / `bitmap_agg` /
  `bitmap_union_count` / `hll_union` / `hll_raw_agg` / `hll_union_agg`
- Window function：`WindowFunctionKind::BitmapUnion` / `BitmapUnionCount`
- StarRocks reader：`AggOp::BitmapUnion`

真正缺的入口只有两处：

1. **SqlType 注册**：`SqlType::Bitmap` / `SqlType::Hll` 不存在，
   `CREATE TABLE ... bm BITMAP` 报 `unsupported data type: BITMAP`。
2. **AGGREGATE KEY 列修饰符**：`ColumnAggregation::BitmapUnion` /
   `HllUnion` 不存在，`bm BITMAP BITMAP_UNION` 没法解析。

另外还有：

3. **5 个二元 BITMAP scalar 函数** 缺失：`bitmap_or`、`bitmap_xor`、`bitmap_andnot`、
   `bitmap_contains`、`bitmap_intersect`。
4. **Analyzer 拒绝**：BITMAP / HLL 在 ORDER BY / GROUP BY / 比较 / DISTRIBUTED BY /
   PRIMARY KEY 中应 fail fast 并给出对齐 StarRocks 的错误消息。

## 2. 目标

让 NovaRocks 把 `BITMAP` / `HLL` 作为一等列类型支持：CREATE TABLE 可以
声明、INSERT / SELECT 能流通、`AGGREGATE KEY` 表能写 `BITMAP BITMAP_UNION` /
`HLL HLL_UNION` 状态列、并对不合法用法 fail fast。

## 3. 非目标

- 不修改现有 SeriV2 二进制格式。
- 不引入新的 arrow extension type；继续用 `DataType::Binary`。
- 不实现 `INSERT VALUES (..., to_bitmap(1), hll_hash('a'))` 这条 routing；它属于
  INT-1 / INT-3 共享的 "INSERT VALUES 含非字面量函数调用" 路径。
- 不实现 stream_load / partial update 路径（属于 INT-5）。
- 不实现 `AGGREGATE KEY` 表端到端的 read-time merge 验证（属于 INT-3）。
- 不为 BITMAP / HLL 实现"集合等价"语义；任何 `=` / `!=` 等比较一律 analyzer reject。

## 4. 设计

### 4.1 SqlType 注册

`src/sql/parser/ast/mod.rs`：

```rust
pub enum SqlType {
    // ... existing variants ...
    Bitmap,
    Hll,
}
```

`src/sql/parser/dialect/mod.rs` 的 `convert_sql_type`：
sqlparser 不识别 `BITMAP` / `HLL`，会进入 `sqlast::DataType::Custom(name, _)` 分支，
在 Custom match 中新增：

```rust
"bitmap" => Ok(SqlType::Bitmap),
"hll"    => Ok(SqlType::Hll),
```

### 4.2 Arrow / 物理表示

- `SqlType::Bitmap` → `arrow::datatypes::DataType::Binary`
- `SqlType::Hll` → `arrow::datatypes::DataType::Binary`

这与现有 `to_bitmap` / `bitmap_to_binary` / `hll_serialize` / `hll_hash` 输出/输入
保持一致；无需新增 Arrow extension。

`SqlType ↔ Arrow` 映射（`src/lower/type_lowering.rs` 或 sql codegen 中相应函数）
新增对应分支。

### 4.3 ColumnAggregation 修饰符

`src/sql/parser/ast/mod.rs`：

```rust
pub(crate) enum ColumnAggregation {
    Sum, Min, Max, Replace,
    BitmapUnion,
    HllUnion,
}
```

`src/sql/parser/dialect/create_table.rs` 的 `parse_column_aggregation`：

```rust
} else if peek_word_eq(parser, 0, "BITMAP_UNION") {
    parser.next_token();
    Some(ColumnAggregation::BitmapUnion)
} else if peek_word_eq(parser, 0, "HLL_UNION") {
    parser.next_token();
    Some(ColumnAggregation::HllUnion)
}
```

下游 DDL 处理（managed-lake 列 schema 写入）需要把 `BitmapUnion` / `HllUnion`
映射到 StarRocks 列侧的 `BITMAP_UNION` / `HLL_UNION` aggregate operation，
让 reader 端能正确按 `AggOp::BitmapUnion` 走 read-time merge。

### 4.4 缺失的 5 个 BITMAP 二元 scalar 函数

在 `src/exec/expr/function/object/bitmap_functions.rs` 补：

| 函数 | 签名 | 语义 |
|---|---|---|
| `bitmap_or` | `(BITMAP, BITMAP) → BITMAP` | 集合并 |
| `bitmap_xor` | `(BITMAP, BITMAP) → BITMAP` | 对称差 |
| `bitmap_andnot` | `(BITMAP, BITMAP) → BITMAP` | 差集 |
| `bitmap_contains` | `(BITMAP, BIGINT) → BOOLEAN` | 是否包含元素 |
| `bitmap_intersect` | `(BITMAP, BITMAP) → BITMAP` | 集合交（scalar 版本；与同名 aggregate 不同名空间） |

每个函数：
- 先 decode 入参 SeriV2 binary 到 `RoaringBitmap`（复用 `bitmap_common::decode_*`）
- 在 `RoaringBitmap` 上做对应集合运算
- encode 回 SeriV2 binary 并 push 到 `BinaryBuilder`
- NULL 入参 → NULL 输出

挂上 `dispatch.rs` 的 `OBJECT_FUNCTIONS` / `OBJECT_METADATA` / canonical match。

注意：`bitmap_intersect` 在 StarRocks 既是 scalar 也是 aggregate。本 PR 只实现
scalar 形态；aggregate 形态如有需要后续单独处理。

### 4.5 Analyzer 拒绝清单

在 `src/sql/analyzer/` 下找到相应解析点（`resolve_order_by` /
`resolve_group_by` / `resolve_comparison` / `resolve_primary_key` /
`resolve_distribution`），对每个上下文检查列类型，遇到 `Bitmap` / `Hll` 时
fail fast。错误消息形态：

```
ORDER BY:        "BITMAP/HLL columns cannot appear in ORDER BY"
GROUP BY:        "BITMAP/HLL columns cannot appear in GROUP BY"
比较运算:        "comparison operator `=` is not supported for BITMAP/HLL"
DISTRIBUTED BY:  "BITMAP/HLL columns cannot be used as distribution key"
PRIMARY KEY:     "BITMAP/HLL columns cannot be part of PRIMARY KEY"
```

（具体措辞实现时按 StarRocks 现有消息再对齐一次）

### 4.6 文件改动概览

```
src/sql/parser/ast/mod.rs                                  + SqlType::{Bitmap,Hll}; ColumnAggregation::{BitmapUnion,HllUnion}
src/sql/parser/dialect/mod.rs                              + Custom("bitmap"/"hll") branch
src/sql/parser/dialect/create_table.rs                     + parse_column_aggregation BITMAP_UNION/HLL_UNION
src/lower/type_lowering.rs                                 + SqlType::{Bitmap,Hll} → DataType::Binary
src/sql/codegen/...                                        + managed-lake ColumnType 映射 BITMAP / HLL
src/connector/starrocks/managed/...                        + 列 aggregation BitmapUnion/HllUnion 写入 schema (consume side)
src/sql/analyzer/...                                       + 5 类 fail-fast 检查
src/exec/expr/function/object/bitmap_functions.rs          + eval_bitmap_or / xor / andnot / contains / intersect
src/exec/expr/function/object/dispatch.rs                  + 5 个函数挂载
sql-tests/function/bitmap_binary_ops.sql                   新增
sql-tests/function/bitmap_hll_type_restrictions.sql        新增
sql-tests/analytic/result/analytic_test_bitmap_union_window.result  录制
sql-tests/analytic/result/analytic_test_window_hll_bitmap.result    录制
```

## 5. 测试

### 5.1 解锁的 analytic case

- `analytic_test_bitmap_union_window`（5 步）：覆盖 `to_bitmap` / `bitmap_empty` /
  `bitmap_to_string` / `bitmap_union` window function over global / partition /
  ordered window。
- `analytic_test_window_hll_bitmap`（5 步）：覆盖 `BITMAP BITMAP_UNION` /
  `HLL HLL_UNION` 列修饰符，`to_bitmap` / `hll_hash` 在 INSERT VALUES 中调用（**这条
  依赖 INSERT VALUES 含函数调用路径，若未通则需在 INT-3 完成后再录制**），
  `lag/lead` on BITMAP/HLL 列，`HLL_CARDINALITY` / `BITMAP_COUNT` wrapper。

> 实施时如果 `analytic_test_window_hll_bitmap` 因 INSERT VALUES 路由问题无法通过
> 录制，本 PR 只 record `analytic_test_bitmap_union_window`，把
> `analytic_test_window_hll_bitmap` 留到 INT-3 阶段闭环。

### 5.2 新增 SQL case

1. `sql-tests/function/bitmap_binary_ops.sql`
   ```sql
   SELECT bitmap_to_string(bitmap_or(to_bitmap(1), to_bitmap(2)));
   SELECT bitmap_to_string(bitmap_xor(to_bitmap(1), to_bitmap(2)));
   SELECT bitmap_to_string(bitmap_andnot(to_bitmap(1), to_bitmap(2)));
   SELECT bitmap_to_string(bitmap_intersect(to_bitmap(1), to_bitmap(1)));
   SELECT bitmap_contains(to_bitmap(1), 1), bitmap_contains(to_bitmap(1), 2);
   -- NULL 入参
   SELECT bitmap_or(NULL, to_bitmap(1)) IS NULL;
   ```

2. `sql-tests/function/bitmap_hll_type_restrictions.sql`
   5 个 step 分别触发 ORDER BY / GROUP BY / `=` / DISTRIBUTED BY / PRIMARY KEY
   场景，断言错误消息形态。

### 5.3 Rust 单测

`bitmap_functions.rs` 新增 5 个二元函数的小型单测：
- empty ⊕ empty
- single ⊕ single 重叠 / 不重叠
- 大量元素 → 走 SeriV2 BITMAP32_SERIV2 / BITMAP64_SERIV2 路径
- NULL 入参

## 6. 验收

1. `cargo build` / `cargo clippy` / `cargo test bitmap_` 通过。
2. 两个解锁 case 至少 `analytic_test_bitmap_union_window` 全 5 步 verify 通过。
3. 新增的 2 个 sql-tests 通过。
4. `CREATE TABLE t(bm BITMAP, hv HLL)` 不再报 `unsupported data type`。
5. `CREATE TABLE t(k INT, bm BITMAP BITMAP_UNION, hv HLL HLL_UNION) AGGREGATE KEY(k)`
   解析通过（语义闭环在 INT-3）。
6. 5 类不合法 BITMAP/HLL 用法都明确 fail fast。

## 7. 风险与已知限制

- **序列化兼容**：现有 SeriV2 代码已默认与 StarRocks 兼容；如果未来发现交叉
  引擎不一致，是 `bitmap_common.rs` 的修复点，本 PR 不调整。
- **INSERT VALUES 函数调用依赖**：`analytic_test_window_hll_bitmap` 的 INSERT
  VALUES 含 `to_bitmap` / `hll_hash` 调用，是否能在当前 INSERT VALUES 路径中
  跑通取决于 INSERT VALUES routing 是否已支持非字面量。若不通，本 PR 只解锁
  第一个 case，第二个挂到 INT-3。
- **AGGREGATE KEY 端到端**：状态列 read-time merge 已经 reader 端有 `AggOp::BitmapUnion`，
  但完整 write/read/select 闭环在 INT-3 验证；本 PR 只验证 parse + schema 写入。
- **错误消息漂移**：StarRocks 不同版本错误措辞可能略有差异；本 PR 按"语义一致即可"
  的标准对齐，不追求逐字符相同。

## 8. 后续工作

- INT-3：AGGREGATE KEY 表 BITMAP_UNION / HLL_UNION 状态列端到端；INSERT VALUES
  含函数调用路由（共享 INT-1）。
- 若用例需要：实现 `bitmap_intersect` 的 aggregate 形态、`bitmap_union_int` 与
  `bitmap_agg` 在 SELECT 中作为聚合调用的 codegen 校验。
