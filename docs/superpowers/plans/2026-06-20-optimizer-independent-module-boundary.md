# Optimizer Independent Module Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 `src/sql/optimizer` 成为只依赖 optimizer-native IR 和 `sql::common` 中立词汇的独立模块，所有 `TypedExpr`、`LogicalPlanNode`、planner wrapper 和 codegen materialization 桥接都移出 optimizer。

**Architecture:** 先建立真实归属的 `sql::common` 类型层，并让 analyzer/planner/engine 保持兼容 re-export；再把 scalar、plan、property、codegen 桥接集中迁到 `planner::optimizer_bridge` 和 codegen adapter；最后用生产代码审计禁止 optimizer 直接引用 `sql::analysis`、`sql::planner`、`sql::codegen`、`engine` 结构。Optimizer 保留 `OptExpr`、`Operator`、`ScalarArena`、`Memo`、property、stats、rules、physical plan。

**Tech Stack:** Rust, Cargo, Python audit scripts, `rg`, NovaRocks SQL test runner.

---

## Target Boundary

完成后允许的生产依赖：

- `src/sql/optimizer/**` 可以依赖 `crate::sql::common`, `crate::sql::column_id`, `crate::sql::catalog` 中扫描所需的 table identity, Arrow `DataType`, optimizer 自身模块和 Rust 标准库。
- `src/sql/optimizer/**` 不能直接依赖 `crate::sql::analysis`, `crate::sql::planner`, `crate::sql::codegen`, `crate::engine`。
- `src/sql/optimizer/**` 不能出现生产引用：`TypedExpr`, `ExprKind`, `LogicalPlanNode`, `PlanNodeKind`, `ProjectItem`, `SortItem`, `AggregateCall`, `WindowExpr`, `intern_typed`, `materialize`。
- `#[cfg(test)]` 测试可以通过显式 test helper 使用 planner/analyzer fixtures；生产审计必须跳过测试代码。

## File Structure

New files:

- `src/sql/common/mod.rs`: 中立 SQL 词汇模块入口。
- `src/sql/common/expr.rs`: `JoinKind`, `LiteralValue`, `BinOp`, `UnOp`, `LambdaParam`, `WindowFrame`, `WindowFrameType`, `WindowBound`。
- `src/sql/common/schema.rs`: `OutputColumn`, `CteId`。
- `src/sql/common/plan_hints.rs`: `ApplyKind`, `DecodeMapping`, `ScanDictionaryColumn`, `ScanVariantColumn`。
- `src/sql/common/dictionary.rs`: dictionary snapshot/value/watermark payload，供 planner、optimizer rewrite、codegen 共享。
- `src/sql/common/imv.rs`: `ImvVersionRole`, `ImvVersionRef`。
- `src/sql/planner/optimizer_bridge/mod.rs`: planner-owned optimizer bridge module.
- `src/sql/planner/optimizer_bridge/scalar.rs`: `TypedExpr`/planner wrappers 与 `ScalarId` 的双向转换。
- `src/sql/planner/optimizer_bridge/plan.rs`: `LogicalPlanNode` 与 `OptExpr` 的双向转换。
- `src/sql/planner/optimizer_bridge/property.rs`: planner sort/window metadata 到 optimizer property 的转换。
- `src/sql/codegen/scalar_materialize.rs`: codegen-facing scalar materialization adapter。
- `src/sql/optimizer/memo_copy.rs`: optimizer-native `OptExpr -> Memo` copy-in。
- `tools/dev/audit_optimizer_independence.py`: import-level and symbol-level boundary audit。

Modified files:

- `src/sql/mod.rs`: export `common` before analyzer/planner/optimizer.
- `src/sql/analysis/mod.rs`, `src/sql/analysis/cte.rs`: move common-owned definitions out, re-export them for existing callers.
- `src/sql/planner/mod.rs`, `src/sql/planner/plan.rs`, `src/sql/planner/imv_rewrite/marker.rs`, `src/sql/planner/imv_rewrite/scan_binding.rs`: expose bridge module and re-export moved common types.
- `src/engine/dictionary/model.rs`: re-export dictionary payloads from `sql::common::dictionary`.
- `src/sql/optimizer/mod.rs`, `operator.rs`, `physical_plan.rs`, `memo.rs`, `property.rs`, `scalar/mod.rs`, `extract.rs`, `cte_rewrite.rs`, `logical_props.rs`, `stats.rs`, `derive/**`, `estimate/**`, `cascades_rules/**`, `rewrite/**`: replace analysis/planner bridge imports with common or optimizer-native APIs.
- `src/sql/codegen/**`, `src/sql/planner/distributed_build.rs`, `src/engine/mod.rs`, `src/engine/mv_rewrite_prep.rs`, `src/sql/planner/imv_rewrite/**`: update bridge call sites.
- `tools/dev/audit_optimizer_typedexpr.py`: shrink legacy allowlist or delegate to the new audit.

---

### Task 1: Add Optimizer Boundary Audit

**Files:**
- Create: `tools/dev/audit_optimizer_independence.py`
- Modify: `tools/dev/audit_optimizer_typedexpr.py`

- [ ] **Step 1: Copy the existing Rust production scanner**

Run:

```bash
cp tools/dev/audit_optimizer_typedexpr.py tools/dev/audit_optimizer_independence.py
```

Expected: `tools/dev/audit_optimizer_independence.py` exists and still exits 0 with current allowlist.

- [ ] **Step 2: Replace the audit header in the new script**

In `tools/dev/audit_optimizer_independence.py`, replace `DEFAULT_ALLOW` and `PATTERN` with:

```python
DEFAULT_SYMBOL_ALLOW = {
    "src/sql/optimizer/scalar/mod.rs",
    "src/sql/optimizer/scalar_bridge.rs",
    "src/sql/optimizer/convert.rs",
    "src/sql/optimizer/property.rs",
}

DEFAULT_IMPORT_ALLOW = {
    "src/sql/optimizer/scalar/mod.rs",
    "src/sql/optimizer/scalar_bridge.rs",
    "src/sql/optimizer/convert.rs",
    "src/sql/optimizer/property.rs",
    "src/sql/optimizer/operator.rs",
    "src/sql/optimizer/physical_plan.rs",
    "src/sql/optimizer/memo.rs",
}

FORBIDDEN_SYMBOL_PATTERN = re.compile(
    r"\b("
    r"TypedExpr|ExprKind|LogicalPlanNode|PlanNodeKind|"
    r"ProjectItem|SortItem|AggregateCall|WindowExpr|"
    r"intern_typed|materialize"
    r")\b"
)

FORBIDDEN_IMPORT_PATTERN = re.compile(
    r"\b(crate::sql::analysis|crate::sql::planner|"
    r"crate::sql::codegen|crate::engine)\b"
)
```

- [ ] **Step 3: Track symbol and import hits separately**

Replace the single `if PATTERN.search(code_line)` branch in `production_hits()` with:

```python
        symbol_hit = FORBIDDEN_SYMBOL_PATTERN.search(code_line)
        import_hit = FORBIDDEN_IMPORT_PATTERN.search(code_line)
        if symbol_hit or import_hit:
            kinds = []
            if symbol_hit:
                kinds.append("symbol")
            if import_hit:
                kinds.append("import")
            yield lineno, ",".join(kinds), line.rstrip()
```

Update the caller loop to apply separate allowlists:

```python
        blocked_hits = []
        for lineno, kinds, line in hits:
            kind_set = set(kinds.split(","))
            symbol_allowed = rel in allowed_symbols
            import_allowed = rel in allowed_imports
            if ("symbol" in kind_set and not symbol_allowed) or (
                "import" in kind_set and not import_allowed
            ):
                blocked_hits.append((lineno, kinds, line))
        if not blocked_hits:
            continue
        failed = True
        print(rel)
        for lineno, kinds, line in blocked_hits:
            print(f"  {lineno} [{kinds}]: {line}")
```

Update CLI parsing:

```python
    parser.add_argument("--allow-symbol", action="append", default=[])
    parser.add_argument("--allow-import", action="append", default=[])

    allowed_symbols = set(DEFAULT_SYMBOL_ALLOW)
    allowed_symbols.update(args.allow_symbol)
    allowed_imports = set(DEFAULT_IMPORT_ALLOW)
    allowed_imports.update(args.allow_import)
```

- [ ] **Step 4: Verify baseline audit behavior**

Run:

```bash
python3 tools/dev/audit_optimizer_typedexpr.py --strict
python3 tools/dev/audit_optimizer_independence.py --strict
```

Expected: both commands exit 0 on the current baseline because bridge files are allowlisted.

- [ ] **Step 5: Commit audit baseline**

```bash
git add tools/dev/audit_optimizer_independence.py tools/dev/audit_optimizer_typedexpr.py
git commit -m "test: add optimizer independence audit baseline"
```

---

### Task 2: Create `sql::common` And Move Shared Type Ownership

**Files:**
- Create: `src/sql/common/mod.rs`
- Create: `src/sql/common/expr.rs`
- Create: `src/sql/common/schema.rs`
- Create: `src/sql/common/plan_hints.rs`
- Create: `src/sql/common/dictionary.rs`
- Create: `src/sql/common/imv.rs`
- Modify: `src/sql/mod.rs`
- Modify: `src/sql/analysis/mod.rs`
- Modify: `src/sql/analysis/cte.rs`
- Modify: `src/sql/planner/plan.rs`
- Modify: `src/sql/planner/imv_rewrite/marker.rs`
- Modify: `src/sql/planner/imv_rewrite/scan_binding.rs`
- Modify: `src/engine/dictionary/model.rs`

- [ ] **Step 1: Add the common module entry**

Add to `src/sql/mod.rs` before `analysis`:

```rust
pub(crate) mod common;
```

Create `src/sql/common/mod.rs`:

```rust
pub(crate) mod dictionary;
pub(crate) mod expr;
pub(crate) mod imv;
pub(crate) mod plan_hints;
pub(crate) mod schema;

pub(crate) use dictionary::{
    DictionaryOwner, DictionarySnapshot, DictionaryState, DictionaryValue, DictionaryWatermark,
    QueryDictionarySelection, StarRocksTabletWatermark,
};
pub(crate) use expr::{
    BinOp, JoinKind, LambdaParam, LiteralValue, UnOp, WindowBound, WindowFrame, WindowFrameType,
};
pub(crate) use imv::{ImvVersionRef, ImvVersionRole};
pub(crate) use plan_hints::{ApplyKind, DecodeMapping, ScanDictionaryColumn, ScanVariantColumn};
pub(crate) use schema::{CteId, OutputColumn};
```

- [ ] **Step 2: Move schema vocabulary**

Create `src/sql/common/schema.rs`:

```rust
use arrow::datatypes::DataType;

use crate::sql::column_id::ColumnId;

pub(crate) type CteId = u32;

#[derive(Clone, Debug)]
pub(crate) struct OutputColumn {
    pub column_id: ColumnId,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub is_internal: bool,
}
```

Remove the `OutputColumn` struct from `src/sql/analysis/mod.rs` and add:

```rust
pub(crate) use crate::sql::common::OutputColumn;
```

In `src/sql/analysis/cte.rs`, replace the local `type CteId = u32` with:

```rust
pub(crate) use crate::sql::common::CteId;
```

- [ ] **Step 3: Move scalar and relational primitive vocabulary**

Create `src/sql/common/expr.rs` by moving these definitions from `src/sql/analysis/mod.rs` without changing variant names or derive attributes:

```rust
use arrow::datatypes::DataType;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JoinKind {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Cross,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
    NullAwareLeftAnti,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct LambdaParam {
    pub name: String,
    pub slot_id: i32,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct WindowFrame {
    pub frame_type: WindowFrameType,
    pub start: WindowBound,
    pub end: WindowBound,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum WindowFrameType {
    Rows,
    Range,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum WindowBound {
    UnboundedPreceding,
    Preceding(i64),
    CurrentRow,
    Following(i64),
    UnboundedFollowing,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum LiteralValue {
    Null,
    Bool(bool),
    Int(i64),
    LargeInt(i128),
    Float(f64),
    Decimal(String),
    String(String),
    Binary(Vec<u8>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    EqForNull,
    And,
    Or,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum UnOp {
    Not,
    Negate,
    BitwiseNot,
}
```

In `src/sql/analysis/mod.rs`, re-export the moved names:

```rust
pub(crate) use crate::sql::common::{
    BinOp, JoinKind, LambdaParam, LiteralValue, UnOp, WindowBound, WindowFrame, WindowFrameType,
};
```

- [ ] **Step 4: Move plan hint vocabulary out of planner**

Create `src/sql/common/plan_hints.rs`:

```rust
use std::sync::Arc;

use arrow::datatypes::DataType;

use crate::sql::column_id::ColumnId;
use crate::sql::common::dictionary::DictionarySnapshot;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DecodeMapping {
    pub source_column_id: ColumnId,
    pub output_column_id: ColumnId,
    pub dict_column: String,
    pub string_column: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ApplyKind {
    Scalar,
    Exists { negated: bool },
    In { negated: bool },
}

#[derive(Clone, Debug)]
pub(crate) struct ScanDictionaryColumn {
    pub source_column: String,
    pub dict_column: String,
    pub dictionary: Arc<DictionarySnapshot>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ScanVariantColumn {
    pub source_column_id: ColumnId,
    pub source_column: String,
    pub synthetic_column_id: ColumnId,
    pub synthetic_column: String,
    pub canonical_path: String,
    pub requested_type: DataType,
    pub strict: bool,
}
```

In `src/sql/planner/plan.rs`, remove local definitions for those four types and add:

```rust
pub(crate) use crate::sql::common::{
    ApplyKind, DecodeMapping, ScanDictionaryColumn, ScanVariantColumn,
};
```

- [ ] **Step 5: Move dictionary payloads out of engine**

Create `src/sql/common/dictionary.rs` by moving the definitions currently in `src/engine/dictionary/model.rs`:

```rust
use std::collections::BTreeMap;

use arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum DictionaryOwner {
    StarRocksTable {
        database: String,
        table: String,
        db_id: i64,
        table_id: i64,
    },
    IcebergTable {
        catalog: String,
        namespace: String,
        table: String,
        table_uuid: Option<String>,
    },
}

impl DictionaryOwner {
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            DictionaryOwner::StarRocksTable { .. } => "starrocks_table",
            DictionaryOwner::IcebergTable { .. } => "iceberg_table",
        }
    }

    pub(crate) fn stable_key(&self) -> String {
        match self {
            DictionaryOwner::StarRocksTable {
                database,
                table,
                db_id,
                table_id,
            } => format!("db={database};table={table};db_id={db_id};table_id={table_id}"),
            DictionaryOwner::IcebergTable {
                catalog,
                namespace,
                table,
                table_uuid,
            } => format!(
                "catalog={catalog};namespace={namespace};table={table};uuid={}",
                table_uuid.as_deref().unwrap_or("")
            ),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum DictionaryWatermark {
    StarRocks {
        schema_id: i64,
        tablets: Vec<StarRocksTabletWatermark>,
    },
    Iceberg {
        snapshot_id: Option<i64>,
        schema_id: i32,
    },
}

impl DictionaryWatermark {
    pub(crate) fn stable_json(&self) -> String {
        serde_json::to_string(self).expect("dictionary watermark serializes")
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StarRocksTabletWatermark {
    pub(crate) tablet_id: i64,
    pub(crate) partition_id: i64,
    pub(crate) visible_version: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DictionaryState {
    Active,
    Stale,
    Dropped,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DictionaryValue {
    pub(crate) id: i32,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
pub(crate) struct DictionarySnapshot {
    pub(crate) dictionary_id: i64,
    pub(crate) owner: DictionaryOwner,
    pub(crate) column_id: Option<i64>,
    pub(crate) column_name: String,
    pub(crate) data_type: DataType,
    pub(crate) version: i64,
    pub(crate) watermark: DictionaryWatermark,
    pub(crate) values: Vec<DictionaryValue>,
    pub(crate) null_id: i32,
    pub(crate) state: DictionaryState,
    pub(crate) order_preserving: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct QueryDictionarySelection {
    pub(crate) base_dictionaries: BTreeMap<String, DictionarySnapshot>,
}
```

Replace `src/engine/dictionary/model.rs` with re-exports:

```rust
pub(crate) use crate::sql::common::dictionary::{
    DictionaryOwner, DictionarySnapshot, DictionaryState, DictionaryValue, DictionaryWatermark,
    QueryDictionarySelection, StarRocksTabletWatermark,
};
```

- [ ] **Step 6: Move IMV version marker vocabulary**

Create `src/sql/common/imv.rs`:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ImvVersionRole {
    From,
    To,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ImvVersionRef {
    pub(crate) role: ImvVersionRole,
}

impl ImvVersionRef {
    pub(crate) fn from_snapshot() -> Self {
        Self {
            role: ImvVersionRole::From,
        }
    }

    pub(crate) fn to_snapshot() -> Self {
        Self {
            role: ImvVersionRole::To,
        }
    }
}

impl Default for ImvVersionRef {
    fn default() -> Self {
        Self::to_snapshot()
    }
}
```

In `src/sql/planner/imv_rewrite/scan_binding.rs`, remove the local `ImvVersionRole` enum and add:

```rust
pub(crate) use crate::sql::common::ImvVersionRole;
```

In `src/sql/planner/imv_rewrite/marker.rs`, remove the local `ImvVersionRef` struct and impl block and add:

```rust
pub(crate) use crate::sql::common::ImvVersionRef;
```

- [ ] **Step 7: Verify common type move**

Run:

```bash
cargo fmt --check
cargo check --lib
```

Expected: both commands exit 0.

- [ ] **Step 8: Commit common ownership**

```bash
git add src/sql/common src/sql/mod.rs src/sql/analysis/mod.rs src/sql/analysis/cte.rs src/sql/planner/plan.rs src/sql/planner/imv_rewrite/marker.rs src/sql/planner/imv_rewrite/scan_binding.rs src/engine/dictionary/model.rs
git commit -m "refactor(sql): move shared optimizer vocabulary to common"
```

---

### Task 3: Point Optimizer Production Imports At `sql::common`

**Files:**
- Modify: `src/sql/optimizer/operator.rs`
- Modify: `src/sql/optimizer/physical_plan.rs`
- Modify: `src/sql/optimizer/memo.rs`
- Modify: `src/sql/optimizer/cte_rewrite.rs`
- Modify: `src/sql/optimizer/logical_props.rs`
- Modify: `src/sql/optimizer/stats.rs`
- Modify: `src/sql/optimizer/scalar_expr.rs`
- Modify: `src/sql/optimizer/estimate/**`
- Modify: `src/sql/optimizer/derive/**`
- Modify: `src/sql/optimizer/cascades_rules/**`
- Modify: `src/sql/optimizer/rewrite/**`

- [ ] **Step 1: Replace common primitive imports**

Use this command to identify production imports that should become `sql::common` imports:

```bash
rg -n "use crate::sql::analysis::(cte::CteId|\\{[^}]*\\b(BinOp|JoinKind|LiteralValue|OutputColumn|UnOp|WindowFrame)\\b|BinOp|JoinKind|LiteralValue|OutputColumn|UnOp|WindowFrame)" src/sql/optimizer -g '*.rs'
```

For production code, replace with imports from `crate::sql::common`. Example for `src/sql/optimizer/operator.rs`:

```rust
use crate::sql::common::{
    ApplyKind, CteId, DecodeMapping, ImvVersionRef, JoinKind, OutputColumn, ScanDictionaryColumn,
    ScanVariantColumn, WindowFrame,
};
```

Do not change imports inside `#[cfg(test)]` blocks in this step.

- [ ] **Step 2: Remove planner type imports from optimizer operators**

In `src/sql/optimizer/operator.rs`, remove:

```rust
use crate::sql::analysis::cte::CteId;
use crate::sql::analysis::{JoinKind, OutputColumn, WindowFrame};
use crate::sql::planner::imv_rewrite::marker::ImvVersionRef;
use crate::sql::planner::plan::{ApplyKind, DecodeMapping};
pub(crate) use crate::sql::planner::plan::{ScanDictionaryColumn, ScanVariantColumn};
```

Add:

```rust
use crate::sql::common::{
    ApplyKind, CteId, DecodeMapping, ImvVersionRef, JoinKind, OutputColumn, ScanDictionaryColumn,
    ScanVariantColumn, WindowFrame,
};
```

- [ ] **Step 3: Update production references using fully qualified analysis paths**

Find remaining production references:

```bash
rg -n "crate::sql::analysis::(BinOp|JoinKind|LiteralValue|OutputColumn|UnOp|WindowFrame|cte::CteId)" src/sql/optimizer -g '*.rs'
```

Replace them with the exact `crate::sql::common` path for the moved type. Examples:

```rust
crate::sql::common::JoinKind::Inner
crate::sql::common::BinOp::And
crate::sql::common::LiteralValue::Bool(true)
```

- [ ] **Step 4: Verify the common-only primitive state**

Run:

```bash
cargo fmt --check
cargo check --lib
python3 tools/dev/audit_optimizer_independence.py --strict
```

Expected: Rust checks exit 0. The independence audit still exits 0 because the remaining bridge files are allowlisted.

- [ ] **Step 5: Commit optimizer common imports**

```bash
git add src/sql/optimizer
git commit -m "refactor(optimizer): use common sql vocabulary"
```

---

### Task 4: Move Scalar Typed Bridges To Planner

**Files:**
- Create: `src/sql/planner/optimizer_bridge/mod.rs`
- Create: `src/sql/planner/optimizer_bridge/scalar.rs`
- Modify: `src/sql/planner/mod.rs`
- Modify: `src/sql/optimizer/mod.rs`
- Modify: `src/sql/optimizer/scalar/mod.rs`
- Delete: `src/sql/optimizer/scalar_bridge.rs`
- Modify: `src/sql/optimizer/**`
- Modify: `src/sql/planner/**`
- Modify: `src/sql/codegen/**`

- [ ] **Step 1: Register the planner bridge module**

Add to `src/sql/planner/mod.rs`:

```rust
pub(crate) mod optimizer_bridge;
```

Create `src/sql/planner/optimizer_bridge/mod.rs`:

```rust
pub(crate) mod property;
pub(crate) mod scalar;
```

- [ ] **Step 2: Move scalar bridge wrapper functions**

Move `src/sql/optimizer/scalar_bridge.rs` to `src/sql/planner/optimizer_bridge/scalar.rs` and update its imports to:

```rust
use arrow::datatypes::DataType;

use crate::sql::analysis::{ExprKind, OutputColumn, ProjectItem, SortItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::{ScalarAggregateSpec, ScalarProjectItem, ScalarWindowSpec};
use crate::sql::optimizer::property;
use crate::sql::optimizer::scalar::{ColumnDisplay, ScalarArena, ScalarId, SortKey};
use crate::sql::planner::plan::{AggregateCall, WindowExpr};
```

Remove this line from `src/sql/optimizer/mod.rs`:

```rust
pub(crate) mod scalar_bridge;
```

- [ ] **Step 3: Move `intern_typed` and `materialize` out of optimizer scalar**

In `src/sql/optimizer/scalar/mod.rs`, move these bridge-only functions into `src/sql/planner/optimizer_bridge/scalar.rs`:

- `intern_sort_key`
- `materialize_sort_key`
- `intern_typed`
- `materialize`
- `ColumnDisplay::from_expr`

Keep `ColumnDisplay` in optimizer and replace `from_expr` with:

```rust
impl ColumnDisplay {
    pub(crate) fn new(qualifier: Option<String>, column: String) -> Self {
        Self { qualifier, column }
    }

    fn is_fallback_for(&self, column_id: ColumnId) -> bool {
        self.qualifier.is_none() && self.column == format!("col{}", column_id.0)
    }
}
```

Add this private helper to `src/sql/planner/optimizer_bridge/scalar.rs`:

```rust
fn column_display_from_expr(expr: &TypedExpr) -> Option<ColumnDisplay> {
    match &expr.kind {
        ExprKind::ColumnRef {
            qualifier, column, ..
        } => Some(ColumnDisplay::new(qualifier.clone(), column.clone())),
        _ => None,
    }
}
```

Replace all bridge-local `ColumnDisplay::from_expr(&item.expr)` calls with `column_display_from_expr(&item.expr)`.

- [ ] **Step 4: Update production bridge call sites outside optimizer**

Run:

```bash
rg -n "optimizer::scalar_bridge|optimizer::scalar::\\{[^}]*\\b(intern_typed|materialize)\\b|optimizer::scalar::(intern_typed|materialize)|scalar::(intern_typed|materialize)" src/sql src/engine -g '*.rs'
```

For non-optimizer production callers, use:

```rust
use crate::sql::planner::optimizer_bridge::scalar::{
    intern_typed, materialize, materialize_exprs, materialize_project_items, materialize_sort_keys,
};
```

For optimizer production callers, replace bridge calls with native `ScalarArena::intern` and `ScalarNode` construction. Example:

```rust
let predicate = arena.intern(
    ScalarNode::BinaryOp {
        left,
        op: BinOp::And,
        right,
    },
    DataType::Boolean,
    arena.nullable(left) || arena.nullable(right),
);
```

- [ ] **Step 5: Update optimizer tests that still use typed fixtures**

For `#[cfg(test)]` modules that still build `TypedExpr`, import bridge helpers from planner:

```rust
use crate::sql::planner::optimizer_bridge::scalar::{intern_typed, materialize};
```

Do not add production imports from `planner::optimizer_bridge` inside optimizer files.

- [ ] **Step 6: Verify scalar bridge move**

Run:

```bash
cargo fmt --check
cargo check --lib
cargo test --lib sql::optimizer::scalar
python3 tools/dev/audit_optimizer_independence.py --strict
```

Expected: Rust checks and focused scalar tests exit 0. The audit still passes with `src/sql/optimizer/convert.rs` and `src/sql/optimizer/property.rs` allowlisted, and no longer needs `src/sql/optimizer/scalar/mod.rs` or `src/sql/optimizer/scalar_bridge.rs` as symbol allowlist entries.

- [ ] **Step 7: Shrink scalar audit allowlist and commit**

Remove these entries from `DEFAULT_SYMBOL_ALLOW` and `DEFAULT_IMPORT_ALLOW` in `tools/dev/audit_optimizer_independence.py`:

```python
"src/sql/optimizer/scalar/mod.rs",
"src/sql/optimizer/scalar_bridge.rs",
```

Run:

```bash
python3 tools/dev/audit_optimizer_independence.py --strict
git add src/sql/planner/optimizer_bridge src/sql/planner/mod.rs src/sql/optimizer src/sql/codegen src/engine tools/dev/audit_optimizer_independence.py
git commit -m "refactor(planner): own optimizer scalar bridges"
```

---

### Task 5: Move Plan Conversion To Planner And Keep Memo Copy Native

**Files:**
- Create: `src/sql/optimizer/memo_copy.rs`
- Create: `src/sql/planner/optimizer_bridge/plan.rs`
- Modify: `src/sql/optimizer/mod.rs`
- Modify: `src/sql/planner/optimizer_bridge/mod.rs`
- Delete: `src/sql/optimizer/convert.rs`
- Modify: `src/sql/planner/mod.rs`
- Modify: `src/engine/mod.rs`
- Modify: `src/engine/mv_rewrite_prep.rs`
- Modify: `src/sql/planner/imv_rewrite/**`
- Modify: `src/sql/codegen/fragment_builder.rs`
- Modify: `src/sql/optimizer/cte_rewrite.rs`
- Modify: optimizer tests that imported `optimizer::convert`

- [ ] **Step 1: Create optimizer-native memo copy module**

Create `src/sql/optimizer/memo_copy.rs`:

```rust
use crate::sql::optimizer::memo::{GroupId, MExpr, Memo};
use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;

pub(crate) fn opt_expr_to_memo(expr: &OptExpr, memo: &mut Memo) -> GroupId {
    let children: Vec<GroupId> = expr
        .children
        .iter()
        .map(|child| opt_expr_to_memo(child, memo))
        .collect();
    let mexpr = MExpr {
        id: memo.next_expr_id(),
        op: expr.op.clone(),
        children,
    };
    let group_id = memo.new_group(mexpr);
    if let Operator::LogicalCTEProduce(op) = &expr.op {
        memo.cte_produce_groups.insert(op.cte_id, group_id);
    }
    group_id
}
```

Add to `src/sql/optimizer/mod.rs`:

```rust
pub(crate) mod memo_copy;
```

- [ ] **Step 2: Move logical plan conversion into planner bridge**

Move `src/sql/optimizer/convert.rs` to `src/sql/planner/optimizer_bridge/plan.rs`.

Remove `opt_expr_to_memo` from the moved file and import it from optimizer only where needed:

```rust
use crate::sql::optimizer::memo_copy::opt_expr_to_memo;
```

Update the moved file imports so planner structures are local to planner bridge:

```rust
use crate::sql::analysis::SortItem;
use crate::sql::optimizer::operator::{
    AggregateStateMergeOp, ApplyOp, AssertOneRowOp, CTEAnchorOp, CTEConsumeOp, CTEProduceOp,
    DecodeOp, ExceptOp, FilterOp, GenerateSeriesOp, ImvDeltaOp, ImvVersionOp, IntersectOp,
    LimitOp, LogicalAggregateOp, LogicalJoinOp, Operator, ProjectOp, RepeatOp, ScanOp, SortOp,
    TableFunctionOp, UnionOp, ValuesOp, WindowOp,
};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::scalar::ScalarArena;
use crate::sql::planner::optimizer_bridge::scalar::{
    intern_aggregate_calls, intern_exprs, intern_project_items, intern_sort_items, intern_typed,
    intern_window_exprs, materialize_aggregate_calls, materialize_exprs,
    materialize_project_items, materialize_sort_keys, materialize_window_exprs,
};
use crate::sql::planner::plan::{
    LogicalAggregateNode, LogicalAggregateStateMergeNode, LogicalApplyNode,
    LogicalAssertOneRowNode, LogicalCTEAnchorNode, LogicalCTEConsumeNode, LogicalCTEProduceNode,
    LogicalDecodeNode, LogicalExceptNode, LogicalFilterNode, LogicalGenerateSeriesNode,
    LogicalImvDeltaNode, LogicalImvVersionNode, LogicalIntersectNode, LogicalJoinNode,
    LogicalLimitNode, LogicalPlanNode, LogicalProjectNode, LogicalRepeatNode, LogicalScanNode,
    LogicalSortNode, LogicalTableFunctionNode, LogicalUnionNode, LogicalValuesNode,
    LogicalWindowNode, PlanNodeKind, validate_logical_plan_stage,
};
```

Add to `src/sql/planner/optimizer_bridge/mod.rs`:

```rust
pub(crate) mod plan;
```

Remove this line from `src/sql/optimizer/mod.rs`:

```rust
pub(crate) mod convert;
```

- [ ] **Step 3: Update all plan bridge call sites**

Run:

```bash
rg -n "optimizer::convert|crate::sql::optimizer::convert" src/sql src/engine -g '*.rs'
```

Replace:

```rust
crate::sql::optimizer::convert::try_logical_plan_to_opt_expr
crate::sql::optimizer::convert::logical_plan_to_opt_expr
crate::sql::optimizer::convert::opt_expr_to_logical_plan
```

with:

```rust
crate::sql::planner::optimizer_bridge::plan::try_logical_plan_to_opt_expr
crate::sql::planner::optimizer_bridge::plan::logical_plan_to_opt_expr
crate::sql::planner::optimizer_bridge::plan::opt_expr_to_logical_plan
```

Replace `optimizer::convert::opt_expr_to_memo` with:

```rust
crate::sql::optimizer::memo_copy::opt_expr_to_memo
```

- [ ] **Step 4: Move conversion tests to planner bridge or mark them test-only**

For tests in `src/sql/optimizer/mod.rs`, `stats.rs`, `rewrite/**`, and `cascades_rules/**` that import `LogicalPlanNode` only to exercise conversion, update imports to:

```rust
use crate::sql::planner::optimizer_bridge::plan::{
    logical_plan_to_opt_expr, opt_expr_to_logical_plan, try_logical_plan_to_opt_expr,
};
```

If a test is entirely about conversion, move it into `src/sql/planner/optimizer_bridge/plan.rs` under `#[cfg(test)]`.

- [ ] **Step 5: Verify plan bridge move**

Run:

```bash
cargo fmt --check
cargo check --lib
cargo test --lib optimizer::memo_copy
cargo test --lib planner::optimizer_bridge::plan
python3 tools/dev/audit_optimizer_independence.py --strict
```

Expected: all commands exit 0. The audit no longer needs `src/sql/optimizer/convert.rs`.

- [ ] **Step 6: Shrink convert audit allowlist and commit**

Remove this entry from both allowlists in `tools/dev/audit_optimizer_independence.py`:

```python
"src/sql/optimizer/convert.rs",
```

Run:

```bash
python3 tools/dev/audit_optimizer_independence.py --strict
git add src/sql/optimizer src/sql/planner src/sql/codegen src/engine tools/dev/audit_optimizer_independence.py
git commit -m "refactor(planner): own logical optimizer plan bridge"
```

---

### Task 6: Move Planner Property Bridges Out Of Optimizer

**Files:**
- Modify: `src/sql/optimizer/property.rs`
- Create: `src/sql/planner/optimizer_bridge/property.rs`
- Modify: `src/sql/optimizer/derive/**`
- Modify: `src/sql/planner/**`

- [ ] **Step 1: Add optimizer-native ordering constructor**

In `src/sql/optimizer/property.rs`, keep `SortKey` and add:

```rust
impl OrderingSpec {
    pub(crate) fn from_sort_keys<I>(items: I) -> Self
    where
        I: IntoIterator<Item = SortKey>,
    {
        let keys: Vec<SortKey> = items.into_iter().collect();
        if keys.is_empty() {
            OrderingSpec::Any
        } else {
            OrderingSpec::Required(keys)
        }
    }
}
```

- [ ] **Step 2: Move planner sort conversions**

Move these functions from `src/sql/optimizer/property.rs` to `src/sql/planner/optimizer_bridge/property.rs`:

- `typed_expr_to_column_id`
- `window_sort_items`
- `window_ordering_spec`
- `OrderingSpec::from_sort_items`

Implement them in planner bridge as free functions:

```rust
use crate::sql::analysis::{ExprKind, SortItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::property::{OrderingSpec, SortKey};

pub(crate) fn typed_expr_to_column_id(expr: &TypedExpr) -> Option<ColumnId> {
    match &expr.kind {
        ExprKind::ColumnRef { column_id, .. } if *column_id != ColumnId::UNSET => Some(*column_id),
        _ => None,
    }
}

pub(crate) fn ordering_spec_from_sort_items(items: &[SortItem]) -> OrderingSpec {
    let mut keys = Vec::with_capacity(items.len());
    for item in items {
        let Some(column) = typed_expr_to_column_id(&item.expr) else {
            return OrderingSpec::Any;
        };
        keys.push(SortKey {
            column,
            asc: item.asc,
            nulls_first: item.nulls_first,
        });
    }
    OrderingSpec::from_sort_keys(keys)
}

pub(crate) fn window_sort_items(partition_by: &[TypedExpr], order_by: &[SortItem]) -> Vec<SortItem> {
    let mut items = Vec::with_capacity(partition_by.len() + order_by.len());
    for expr in partition_by {
        items.push(SortItem {
            expr: expr.clone(),
            asc: true,
            nulls_first: true,
        });
    }
    items.extend(order_by.iter().cloned());
    items
}

pub(crate) fn window_ordering_spec(
    partition_by: &[TypedExpr],
    order_by: &[SortItem],
) -> OrderingSpec {
    ordering_spec_from_sort_items(&window_sort_items(partition_by, order_by))
}
```

- [ ] **Step 3: Update optimizer property tests**

Tests that build `SortItem` should move to `src/sql/planner/optimizer_bridge/property.rs`. Optimizer `property.rs` tests should only construct native `SortKey` values:

```rust
let required = OrderingSpec::from_sort_keys([SortKey {
    column: ColumnId(1),
    asc: true,
    nulls_first: true,
}]);
```

- [ ] **Step 4: Verify property split**

Run:

```bash
cargo fmt --check
cargo check --lib
cargo test --lib optimizer::property
cargo test --lib planner::optimizer_bridge::property
python3 tools/dev/audit_optimizer_independence.py --strict
```

Expected: all commands exit 0. The audit no longer needs `src/sql/optimizer/property.rs`.

- [ ] **Step 5: Shrink property audit allowlist and commit**

Remove this entry from both allowlists in `tools/dev/audit_optimizer_independence.py`:

```python
"src/sql/optimizer/property.rs",
```

Run:

```bash
python3 tools/dev/audit_optimizer_independence.py --strict
git add src/sql/optimizer/property.rs src/sql/planner/optimizer_bridge/property.rs tools/dev/audit_optimizer_independence.py
git commit -m "refactor(planner): own optimizer property bridges"
```

---

### Task 7: Add Codegen Scalar Materialization Adapter

**Files:**
- Create: `src/sql/codegen/scalar_materialize.rs`
- Modify: `src/sql/codegen/mod.rs`
- Modify: `src/sql/codegen/id_binding_verifier.rs`
- Modify: `src/sql/codegen/ir/explain.rs`
- Modify: `src/sql/codegen/ir/lowering.rs`
- Modify: `src/sql/codegen/ir/equiv.rs`
- Modify: `src/sql/codegen/fragment_builder.rs`
- Modify: `src/sql/planner/distributed_build.rs`

- [ ] **Step 1: Create codegen adapter module**

Create `src/sql/codegen/scalar_materialize.rs`:

```rust
pub(crate) use crate::sql::planner::optimizer_bridge::scalar::{
    materialize, materialize_aggregate_call, materialize_aggregate_calls, materialize_exprs,
    materialize_project_item, materialize_project_items, materialize_sort_key,
    materialize_sort_keys, materialize_window_expr, materialize_window_exprs,
};
```

Add to `src/sql/codegen/mod.rs`:

```rust
pub(crate) mod scalar_materialize;
```

- [ ] **Step 2: Route codegen materialization imports through adapter**

Run:

```bash
rg -n "optimizer::scalar::\\{[^}]*materialize|optimizer::scalar::materialize|planner::optimizer_bridge::scalar::materialize|optimizer::scalar_bridge" src/sql/codegen src/sql/planner/distributed_build.rs -g '*.rs'
```

Replace materialization imports with:

```rust
use crate::sql::codegen::scalar_materialize::{
    materialize, materialize_exprs, materialize_project_items, materialize_sort_keys,
};
```

If a file also needs `intern_typed` for tests, import it inside the `#[cfg(test)]` module from:

```rust
use crate::sql::planner::optimizer_bridge::scalar::intern_typed;
```

- [ ] **Step 3: Verify codegen adapter**

Run:

```bash
cargo fmt --check
cargo check --lib
cargo test --lib sql::codegen
python3 tools/dev/audit_optimizer_independence.py --strict
```

Expected: all commands exit 0.

- [ ] **Step 4: Commit codegen adapter**

```bash
git add src/sql/codegen src/sql/planner/distributed_build.rs
git commit -m "refactor(codegen): route scalar materialization through adapter"
```

---

### Task 8: Clean Production Rewrite Dependencies

**Files:**
- Modify: `src/sql/optimizer/rewrite/rules/subquery/**`
- Modify: `src/sql/optimizer/rewrite/rules/low_cardinality_dict/**`
- Modify: `src/sql/optimizer/rewrite/rules/variant_path_pushdown/**`
- Modify: `src/sql/optimizer/rewrite/rules/column_pruning/**`
- Modify: `src/sql/optimizer/rewrite/rules/utils.rs`
- Modify: `src/sql/optimizer/rewrite/required_columns.rs`
- Modify: `src/sql/optimizer/cascades_rules/implement.rs`

- [ ] **Step 1: Identify remaining production planner/analyzer imports**

Run:

```bash
python3 tools/dev/audit_optimizer_independence.py --strict \
  --allow-symbol src/sql/optimizer/property.rs \
  --allow-import src/sql/optimizer/property.rs
```

Expected: after Tasks 4 to 7, any printed file is a production rewrite dependency that still crosses the boundary.

- [ ] **Step 2: Convert primitive imports to common**

For production imports like:

```rust
use crate::sql::analysis::{BinOp, JoinKind, LiteralValue, OutputColumn};
use crate::sql::planner::plan::{ApplyKind, DecodeMapping, ScanDictionaryColumn};
```

replace with:

```rust
use crate::sql::common::{
    ApplyKind, BinOp, DecodeMapping, JoinKind, LiteralValue, OutputColumn, ScanDictionaryColumn,
};
```

- [ ] **Step 3: Keep typed/logical plan helpers test-only**

For files where `LogicalPlanNode`, `ProjectItem`, `SortItem`, `TypedExpr`, or `WindowExpr` are used only in tests, move imports under the existing `#[cfg(test)] mod tests` block.

Example:

```rust
#[cfg(test)]
mod tests {
    use crate::sql::analysis::{ExprKind, ProjectItem, SortItem, TypedExpr};
    use crate::sql::planner::optimizer_bridge::plan::{
        logical_plan_to_opt_expr, opt_expr_to_logical_plan,
    };
    use crate::sql::planner::plan::{LogicalPlanNode, PlanNodeKind};
}
```

- [ ] **Step 4: Replace production scalar bridge calls with native scalar builders**

For production code that constructs simple literals or predicates through `TypedExpr`, rewrite to `ScalarArena::intern`. Example replacement:

```rust
let literal = arena.intern(
    ScalarNode::Literal(HashableLiteral(LiteralValue::Bool(true))),
    DataType::Boolean,
    false,
);
let predicate = arena.intern(
    ScalarNode::BinaryOp {
        left,
        op: BinOp::And,
        right: literal,
    },
    DataType::Boolean,
    arena.nullable(left),
);
```

- [ ] **Step 5: Verify production rewrite cleanup**

Run:

```bash
cargo fmt --check
cargo check --lib
cargo test --lib sql::optimizer::rewrite
cargo test --lib sql::optimizer::cascades_rules
python3 tools/dev/audit_optimizer_independence.py --strict
```

Expected: all commands exit 0 and the audit prints no files.

- [ ] **Step 6: Commit rewrite cleanup**

```bash
git add src/sql/optimizer src/sql/planner src/sql/codegen tools/dev/audit_optimizer_independence.py
git commit -m "refactor(optimizer): remove production planner rewrite dependencies"
```

---

### Task 9: Tighten Legacy TypedExpr Audit To Zero

**Files:**
- Modify: `tools/dev/audit_optimizer_typedexpr.py`
- Modify: `tools/dev/audit_optimizer_independence.py`

- [ ] **Step 1: Empty the legacy typed allowlist**

In `tools/dev/audit_optimizer_typedexpr.py`, set:

```python
DEFAULT_ALLOW = set()
```

- [ ] **Step 2: Empty the independence audit allowlists**

In `tools/dev/audit_optimizer_independence.py`, set:

```python
DEFAULT_SYMBOL_ALLOW = set()
DEFAULT_IMPORT_ALLOW = set()
```

- [ ] **Step 3: Verify both audits are strict**

Run:

```bash
python3 tools/dev/audit_optimizer_typedexpr.py --strict
python3 tools/dev/audit_optimizer_independence.py --strict
```

Expected: both commands exit 0 and print no files.

- [ ] **Step 4: Commit audit hardening**

```bash
git add tools/dev/audit_optimizer_typedexpr.py tools/dev/audit_optimizer_independence.py
git commit -m "test: enforce optimizer independence boundary"
```

---

### Task 10: Full Rust Verification

**Files:**
- No source changes expected unless verification finds a real defect.

- [ ] **Step 1: Format and diff checks**

Run:

```bash
cargo fmt --check
git diff --check
```

Expected: both commands exit 0.

- [ ] **Step 2: Library build**

Run:

```bash
cargo check --lib
```

Expected: exits 0.

- [ ] **Step 3: Focused tests**

Run each command separately:

```bash
cargo test --lib sql::optimizer
cargo test --lib sql::planner
cargo test --lib sql::codegen
```

Expected: each command exits 0.

- [ ] **Step 4: Full library tests**

Run:

```bash
cargo test --lib
```

Expected: exits 0.

- [ ] **Step 5: Boundary audits**

Run:

```bash
python3 tools/dev/audit_optimizer_typedexpr.py --strict
python3 tools/dev/audit_optimizer_independence.py --strict
```

Expected: both commands exit 0 and print no files.

- [ ] **Step 6: Commit any verification fixes**

If fixes were required:

```bash
git add src tools/dev
git commit -m "fix: stabilize optimizer boundary verification"
```

If no fixes were required, do not create an empty commit.

---

### Task 11: SQL Plan-Shape Verification

**Files:**
- No source changes expected unless verification finds a real defect.

- [ ] **Step 1: Prepare shared Docker-backed runtime**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
```

Expected: Docker services are running or reused; `$NOVAROCKS_SQL_TEST_CONFIG` is set.

- [ ] **Step 2: Run optimizer SQL suite**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite optimizer --mode verify
```

Expected: suite exits 0.

- [ ] **Step 3: Run TPC-DS smoke cases that previously exercised optimizer/codegen bridges**

Run:

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite tpc-ds --only q10,q35,q69 --mode verify
```

Expected: command exits 0.

- [ ] **Step 4: Commit SQL verification fixes**

If fixes were required:

```bash
git add src sql-tests tests tools
git commit -m "fix: preserve optimizer bridge plan shapes"
```

If no fixes were required, do not create an empty commit.

---

## Final Review Checklist

- [ ] `src/sql/optimizer/mod.rs` no longer exports `convert` or `scalar_bridge`.
- [ ] `src/sql/optimizer/scalar/mod.rs` no longer imports or mentions `TypedExpr`, `ExprKind`, `SortItem`, `ProjectItem`, `intern_typed`, or `materialize` in production code.
- [ ] `src/sql/optimizer/property.rs` no longer imports or mentions `TypedExpr`, `ExprKind`, or `SortItem` in production code.
- [ ] `src/sql/optimizer/**` production code has no direct imports from `crate::sql::analysis`, `crate::sql::planner`, `crate::sql::codegen`, or `crate::engine`.
- [ ] Planner bridge owns `TypedExpr <-> ScalarId`, `LogicalPlanNode <-> OptExpr`, and planner sort/window property conversion.
- [ ] Codegen uses `src/sql/codegen/scalar_materialize.rs` as its materialization surface.
- [ ] `python3 tools/dev/audit_optimizer_typedexpr.py --strict` exits 0.
- [ ] `python3 tools/dev/audit_optimizer_independence.py --strict` exits 0.
- [ ] `cargo test --lib` exits 0.
- [ ] SQL optimizer suite and TPC-DS smoke cases exit 0.

## Self-Review Result

- Spec coverage: Phase 1 through Phase 6 are covered by Tasks 2 through 9. The additional planner-owned operator payloads found in `operator.rs` are covered by Task 2 and Task 3.
- Placeholder scan: no delayed implementation language is required to execute this plan; each code-moving task names the exact source and target files, commands, and verification.
- Type consistency: bridge names are consistently under `planner::optimizer_bridge::{scalar, plan, property}`; optimizer-native memo copy is consistently `optimizer::memo_copy::opt_expr_to_memo`; codegen materialization is consistently `codegen::scalar_materialize`.
