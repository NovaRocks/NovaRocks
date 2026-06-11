# IVM-A11 · MV schema / field-id 依赖契约

## 状态

- 优先级：⭐⭐⭐（A9 合入后的下一步主线）
- 当前范围：Iceberg-backed projection/filter MV、单 base table、base 必须是 Iceberg v3 + row-lineage（与 A9 一致）
- 非目标范围：join、aggregate、多 base table、managed lake MV、非 row-lineage Iceberg、A2 历史 snapshot range schema 读取
- 依赖：IVM-A9 Iceberg target 行身份与增量 apply 协议（hidden apply-key 已落地）
- 后续依赖：IVM-A2（snapshot range 规划时需要历史 schema）、IVM-A12（MV 元数据 trait 抽象）
- 关联：IVM-B2 MV dependency、IVM-A9

## 问题

A9 已经把 base `_row_id` 写入 MV target 的 hidden apply-key column，并把 target delete/update
apply 建在这个列的 field id 和语义稳定性上。当前实现只覆盖了 A9 所需的局部 guard：apply-key
列是否存在、类型是否为 BIGINT、field id 是否匹配、base table UUID 是否一致。

下一步如果继续扩展 aggregate / join，不能再只依赖 SELECT SQL 重新解析和局部列检查。需要先把
MV 创建时的 base field id、输出列、target visible/hidden field id、表达式依赖持久化成可验证
contract。否则 rename、reorder、drop+add 同名列、target schema 被外部改写等情况会让
incremental refresh 有机会返回旧数据或删除错 target row。

A11 的目标是把 A9 的局部 guard 升级为完整 schema contract，覆盖：

- base table 创建 MV 时的 schema id / field id 映射；
- MV 输出列到 base field id / 表达式的依赖关系；
- target Iceberg schema 的 field id 分配与用户可见列顺序；
- base schema evolution 后的安全 / 不安全分类。

证据：

- `src/meta/repository/mv.rs`：`StoredMvDefinition` 当前只持久化
  `select_sql`、`base_table_refs`、`target_apply_key`（A9 新增）等局部字段。
- `src/engine/mv/iceberg_target_apply.rs`：`ensure_target_apply_key_contract` 是
  A9 的局部 guard，未覆盖 base referenced field 的 schema evolution。
- `src/engine/mv/iceberg_refresh.rs`：refresh 路径中 guard 调用点散落，没有
  统一 contract 校验入口。
- `src/sql/analyzer/`：`OutputColumn` 仅保存 `name / data_type / nullable`，无任何
  base column lineage。

## 当前阶段约束

1. 与 A9 一致：base 必须是 Iceberg format-version=3、`write.row-lineage=true`，能读
   `_row_id`；MV 必须是单 base projection/filter。
2. A11 不引入新的 MV 形态。仍只支持 projection + filter。
3. 不覆盖 managed lake MV 路径。managed lake 仍然走 `mv_ddl::create_mv` 而非 contract。
4. 不引入 backward compat。NovaRocks 还未上线，旧 MV 元数据记录若不再可解析直接 fail；
   `MV_DEFINITION_SCHEMA_VERSION` 顺手 +1。
5. 不引入 IVM-A12 的 trait 抽象。A11 直接在现有 `MvMetaRepository` 类型上扩展字段。

## 行身份模型与 A9 的衔接

A9 当前的 `MvTargetApplyKey { column_name, field_id, source }` 作为 A11 contract
的子结构存在，逻辑等价于 contract 中的 `target.hidden_apply_key`。

A11 在数据结构层面**不再保留** `StoredMvDefinition.target_apply_key` 顶层字段；
contract 引入后，所有"创建时固化的 schema 约束"集中在 `schema_contract` 字段。

## Schema Contract 数据模型

```text
MvSchemaContract {
  contract_version: u16,                 // 当前固定 = 1
  base: BaseContract,
  output: OutputContract,
  target: TargetContract,
}

BaseContract {
  table_fqn: String,                     // 与 base_table_refs[0] 保持一致
  table_uuid: String,
  schema_id_at_create: i32,              // base Iceberg current_schema_id
  schema_at_create: BaseSchemaSnapshot,
}

BaseSchemaSnapshot {
  fields: Vec<BaseFieldRecord>,          // 仅包含被引用的 base field
}

BaseFieldRecord {
  field_id: i32,
  name_at_create: String,                // 仅用于诊断；不参与 guard 判定
  type_signature: String,                // 见下文规范
  required: bool,
}

OutputContract {
  columns: Vec<OutputColumnLineage>,     // 与 target.visible_columns 按下标对齐
  filter: Option<FilterLineage>,         // WHERE 的 lineage；None 表示无 filter
}

OutputColumnLineage {
  // 不重复 output_name / output_type / output_nullable；
  // 这些都在同下标的 target.visible_columns 上有真理。
  expression: ExpressionLineage,
}

ExpressionLineage {
  kind: ExpressionKind,                  // Column | Cast | Func | Literal | Mixed
  referenced_base_field_ids: Vec<i32>,   // 排序去重
}

FilterLineage {
  referenced_base_field_ids: Vec<i32>,
}

TargetContract {
  table_fqn: String,
  table_uuid: String,
  schema_id_at_create: i32,
  visible_columns: Vec<TargetVisibleColumn>,
  hidden_apply_key: HiddenApplyKeyContract,
}

TargetVisibleColumn {
  output_name: String,
  target_field_id: i32,
  type_signature: String,
  nullable: bool,
}

HiddenApplyKeyContract {
  column_name: String,                   // 固定 = "__nova_base_row_id"
  target_field_id: i32,
  source: ApplyKeySource,                // 当前阶段只有 BaseRowId
}
```

### type_signature 规范

使用 Iceberg type spec 风格的字符串表示：

- 标量：`int`, `long`, `float`, `double`, `boolean`, `string`, `binary`, `uuid`, `date`,
  `time`, `timestamp`, `timestamptz`, `decimal(P,S)`, `fixed(L)`。
- 容器：`list<E>`, `map<K,V>`, `struct<a:T1,b:T2>`。

刻意与 NovaRocks SQL 层 type display 解耦，避免 SQL display 演化影响 contract 签名。

### referenced_base_field_ids 计算

按输出列单独收集（不跨列去重），同一表达式内部排序去重。filter 单独维护。

## CREATE MV 时的 contract 生成

唯一入口：`src/engine/mv/iceberg_refresh.rs::create_iceberg_mv()`。Managed lake MV
路径不受影响。

流程：

```text
analyze_mv_select(select_sql)
   │   返回 outputs: ProjectItem[] 和 resolved base ref
   ▼
build_projection_filter_lineage(analysis, base_iceberg_schema)  ◄── 新模块
   │   src/sql/analyzer/mv_lineage.rs
   │   遍历每个 ProjectItem.expr 与 WHERE expr：
   │     - 收集 referenced_base_field_ids
   │     - 推导 ExpressionKind
   ▼
load_base_schema_snapshot(base_iceberg_schema, referenced_ids)
   │   仅截取被引用 field 的 BaseFieldRecord
   ▼
create_target_iceberg_table()       ◄── 已有逻辑 (含 hidden apply-key 列)
   ▼
build_target_contract(target_iceberg_schema, outputs)
   │   按 output_name → target_field_id 建立映射
   │   固化 hidden_apply_key.target_field_id
   ▼
MvSchemaContract::new(base, output, target)
   ▼
ensure_contract_self_consistent(&contract, target_iceberg_schema)
   │   失败 → rollback 已创建 target，删除元数据，向上抛错
   ▼
MvMetaRepository::create_definition_with_id(definition_with_contract)
```

### analyzer 增强

新模块 `src/sql/analyzer/mv_lineage.rs`，对外暴露：

```text
fn build_projection_filter_lineage(
    analysis: &MvAnalysis,
    base_iceberg_schema: &iceberg::spec::Schema,
) -> Result<(Vec<OutputColumnLineage>, Option<FilterLineage>)>
```

实现要点：

- Analyzer 内部已经把 SELECT 绑定到了具体 base column；在那一步**额外查询 Iceberg
  `Schema` 拿到 field id** 并写入 lineage。
- 不直接 import `iceberg` crate；按 trait / closure 把 field id 查询能力作为参数传入。
- 不在此处再判定 MV 形态；A9 已经在 `analyze_mv_select` 阶段拒绝 join / aggregate /
  union 等。`build_projection_filter_lineage` 入口加 `debug_assert!`。

### CREATE 时的自检

`ensure_contract_self_consistent` 在持久化前必须通过：

1. `output.columns.len() == target.visible_columns.len()`（按下标对齐）。
2. `target.hidden_apply_key.column_name == "__nova_base_row_id"`，对应列在 target schema
   中存在，type=BIGINT，required=true。
3. `output.columns[i].expression.referenced_base_field_ids` 中每个 id 都能在
   `base.schema_at_create.fields` 中找到。
4. `base.table_uuid` 非空，`base.schema_id_at_create >= 0`。
5. 同一 referenced field id 在 base snapshot 中类型签名稳定。
6. `target.visible_columns[i].output_name` 与 analyzer 给出的输出列名一致；
   `target.visible_columns[i].type_signature` 与 target Iceberg schema 中该 field 的类型一致。

自检失败要 rollback 已创建的 target table；走 A9 已有的 create rollback 路径。

## REFRESH guard

新增 `src/engine/mv/schema_contract.rs`，对外暴露：

```text
fn validate_schema_contract(
    contract: &MvSchemaContract,
    current_base_table: &iceberg::Table,
    current_target_table: &iceberg::Table,
    analyzer_ctx: &AnalyzerCtx,
) -> Result<ContractDecision>

enum ContractDecision {
    CompatibleSafe,
    CompatibleSafeWithRebind {
        rebound_columns: Vec<(i32 /*field_id*/, String /*old*/, String /*new*/)>,
    },
    Incompatible(SchemaEvolutionError),
}
```

### 三段式检查

**段 1：identity guard（便宜先做）**

- `current_base_table.uuid == contract.base.table_uuid` → 否则
  `SchemaEvolutionError::BaseTableIdentityChanged`。
- base format-version = 3 且 `write.row-lineage=true` → 否则 `BaseRowLineageContractBroken`。
- target format-version = 3 且 `write.row-lineage=true` → 否则 `TargetRowLineageContractBroken`。
- target uuid 与 contract 一致 → 否则 `TargetTableIdentityChanged`。

**段 2：schema-id 快路 + base referenced field 精确比对**

- 快路：`current_base.current_schema_id == contract.base.schema_id_at_create` **且**
  `current_target.current_schema_id == contract.target.schema_id_at_create` →
  直接 `CompatibleSafe`，跳过段 3。
- 否则对 `contract.base.schema_at_create.fields` 中每个 `BaseFieldRecord`：
  - 按 field id 在当前 base schema 查找。
  - 找不到 → `BaseFieldDropped { field_id, name_at_create }`。
  - 找到但 type_signature 不匹配 → `BaseFieldTypeChanged { field_id, from, to }`。
  - 找到、type 匹配、name 改变 → 加入 `rebound_columns`，继续。
- **不检查未被引用的 base 列**。新增 / 删除 / rename 未被引用列自动通过。

**段 3：target schema 精确比对**

- 对 `contract.target.visible_columns` 中每个 `TargetVisibleColumn` 按 `target_field_id`
  查 target 当前 schema：
  - 找不到 → `TargetVisibleFieldDropped`。
  - type_signature 不匹配 → `TargetVisibleFieldTypeChanged`。
  - `output_name` 不匹配 → `TargetVisibleFieldRenamed`。
- 对 `contract.target.hidden_apply_key` 按 field id 查：
  - column_name 必须仍是 `__nova_base_row_id`、type=BIGINT required → 否则
    `HiddenApplyKeyContractBroken`。

段 3 不重新分析 SELECT SQL：在 SQL 文本不可变、被引用 base 列类型已被段 2 接住的前提下，
表达式语义不会自漂移；表达式重分析在 refresh 后续 plan 阶段自然发生，guard 不重复做。

### Analyzer rebind hint

`AnalyzerCtx` 暴露 `mv_rebind_hint: HashMap<String /*name_at_create*/, i32 /*field_id*/>`。
SQL binder 在按名查找 base column 失败时，按 hint 取 field id 在当前 base schema 中找现行
name 重试。两者都失败 → 直接报 `BaseFieldDropped`。

`mv_rebind_hint` 只在 MV refresh 上下文里启用；普通查询路径不应使用，避免污染 SQL 语义。

### 调用点

`src/engine/mv/iceberg_refresh.rs::refresh_iceberg_mv` 在做任何 incremental 工作之前：

```text
refresh_iceberg_mv(...)
   │
   ├─ load base table & target table
   │
   ├─ decision = validate_schema_contract(...)
   │     ├─ Incompatible(err)            → return Err(SchemaEvolutionUnsupported(err))
   │     ├─ CompatibleSafe               → continue
   │     └─ CompatibleSafeWithRebind {.} → log INFO 列出 rebind，继续
   │
   ├─ plan_changes(...)  使用 rebound 后的当前 base 列名
   │
   └─ 现有 apply pipeline 不变
```

A9 留下的 `ensure_target_apply_key_contract` 被 contract 段 1+3 包含；不再单独调用。

### Incremental refresh 不修改 contract

即使发生 safe rename，contract 中的 `name_at_create` 不更新。Contract 只在 CREATE 或
full rebuild 这两个时机生成，永远反映"上次完整决策时刻的快照"。

## Full rebuild contract regeneration

### 触发

`REFRESH MATERIALIZED VIEW ... FULL`（显式）或未来 `CREATE OR REPLACE MV`。

### 流程

```text
refresh_full_iceberg_mv()
   │
   ├─ load current base table
   │
   ├─ ensure_base_row_lineage_contract(base)
   │     base 仍必须满足 v3 + row-lineage；full rebuild 不豁免
   │
   ├─ 跳过 validate_schema_contract — full rebuild 是修复手段
   │
   ├─ 重新跑 analyzer + build_projection_filter_lineage
   │     得到 new_base、new_output
   │
   ├─ drop existing target table
   │     uuid 会变化，contract 中 target.table_uuid 同步更新
   │
   ├─ create new target table (复用 create_iceberg_mv 的 target 创建逻辑)
   │
   ├─ build new_target_contract
   │
   ├─ new_contract = MvSchemaContract::new(new_base, new_output, new_target)
   │
   ├─ ensure_contract_self_consistent(&new_contract)
   │
   ├─ 写数据 + commit (复用现有 append-only refresh 路径)
   │
   └─ atomic 更新 MvMetaRepository:
         schema_contract = new_contract
         base_table_uuids 同步
         base / target snapshot 标记同步
```

Atomicity 沿用 A10 的事务框架：commit 成功才更新 contract；commit 失败 contract 保持旧值，
下次 refresh 仍能正确报错。

### Drop & recreate target table

Full rebuild 选择 drop + create，而不是 reuse + overwrite：

- 语义最干净：full rebuild = MV 重新创建一遍。
- 能修复"target schema 被外部改坏"这类 contract 内部错误。
- NovaRocks 目前没有按 target uuid 引用 MV 的下游 system，uuid 变化无害。

## Schema evolution 决策矩阵

### Base 表演化

| 变化 | 检测点 | 决策 |
|---|---|---|
| base UUID 改变 | 段 1 | **Fail fast** `BaseTableIdentityChanged` |
| format-version → 2 | 段 1 | **Fail fast** `BaseRowLineageContractBroken` |
| `write.row-lineage=false` | 段 1 | **Fail fast** `BaseRowLineageContractBroken` |
| Rename 被引用列 | 段 2（name 不匹配，field id 匹配） | `CompatibleSafeWithRebind` |
| Rename 未被引用列 | 不检查 | `CompatibleSafe` |
| Drop 被引用列 | 段 2 | **Fail fast** `BaseFieldDropped` |
| Drop 未被引用列 | 不检查 | `CompatibleSafe` |
| Drop+add 同名（被引用） | 段 2，field id 不存在 | **Fail fast** `BaseFieldDropped` |
| Drop+add 同名（未被引用） | 不检查 | `CompatibleSafe` |
| 被引用列类型变化（任何类型变化，含 promotion-safe） | 段 2 | **Fail fast** `BaseFieldTypeChanged` |
| 新增 nullable 列 | 不检查 | `CompatibleSafe` |
| 新增 required 列 | 不检查 | `CompatibleSafe`（外部写 base 自身会失败，不在 MV 范围内） |
| Reorder 列 | field id 不变 | `CompatibleSafe` |
| 仅 schema_id 变 | 快路 miss，段 2 精确比对全过 | `CompatibleSafe` |

### Target 表演化（target 被外部改写）

| 变化 | 检测点 | 决策 |
|---|---|---|
| Target drop & recreate | 段 1（uuid 变） | **Fail fast** `TargetTableIdentityChanged` |
| format-version / row-lineage 被改 | 段 1 | **Fail fast** `TargetRowLineageContractBroken` |
| Hidden apply-key 列被 drop / rename / type 改 | 段 3 hidden | **Fail fast** `HiddenApplyKeyContractBroken` |
| Visible 输出列被 drop | 段 3 visible | **Fail fast** `TargetVisibleFieldDropped` |
| Visible 输出列被 rename | 段 3 visible | **Fail fast** `TargetVisibleFieldRenamed` |
| Visible 输出列类型被改 | 段 3 visible | **Fail fast** `TargetVisibleFieldTypeChanged` |
| Target 加新列（与 MV 无关） | 不检查 | `CompatibleSafe` |

## Error Handling

`SchemaEvolutionError` 是显式 enum，定义在 `src/engine/mv/schema_contract.rs`：

```text
enum SchemaEvolutionError {
    BaseTableIdentityChanged { expected: String, actual: String },
    BaseRowLineageContractBroken { reason: String },
    BaseFieldDropped { field_id: i32, name_at_create: String },
    BaseFieldTypeChanged { field_id: i32, name_at_create: String, from: String, to: String },
    TargetTableIdentityChanged { expected: String, actual: String },
    TargetRowLineageContractBroken { reason: String },
    TargetVisibleFieldDropped { output_name: String, target_field_id: i32 },
    TargetVisibleFieldRenamed { target_field_id: i32, expected: String, actual: String },
    TargetVisibleFieldTypeChanged { target_field_id: i32, from: String, to: String },
    HiddenApplyKeyContractBroken { reason: String },
}
```

错误信息要求 action-oriented，全部以英文输出：

```text
iceberg MV refresh blocked: base table identity changed (uuid expected=<u1>, actual=<u2>); run REFRESH FULL or recreate the MV
iceberg MV refresh blocked: base column "<name_at_create>" (field id <id>) was dropped from base table; run REFRESH FULL or recreate the MV
iceberg MV refresh blocked: base column "<name_at_create>" (field id <id>) changed type from <t1> to <t2>; run REFRESH FULL or recreate the MV
iceberg MV refresh blocked: target visible column "<name>" (field id <id>) renamed externally; recreate the MV
iceberg MV refresh blocked: target hidden apply-key column missing or altered; recreate the MV
```

不允许通过 session flag / config 绕过这些错误。`REFRESH FULL` 是唯一恢复路径。
任何 catch 后 fallback rebuild 都是 bug。

## 组件边界

### 1. MvSchemaContract（数据结构）
位置：`src/meta/repository/mv.rs` 旁的子模块或同一文件内独立 mod。仅做序列化 / 反序列化 /
不变量自检。不依赖 iceberg / analyzer crate。

### 2. mv_lineage（lineage builder）
位置：`src/sql/analyzer/mv_lineage.rs`。负责把 analyzer 输出 + base Iceberg schema
转成 `OutputColumnLineage` 与 `FilterLineage`。通过 trait/closure 间接拿 Iceberg field id。

### 3. schema_contract（refresh guard）
位置：`src/engine/mv/schema_contract.rs`。`validate_schema_contract` 单一入口。
依赖 iceberg crate 读 schema。

### 4. iceberg_refresh（CREATE / REFRESH / REFRESH FULL 触发）
位置：`src/engine/mv/iceberg_refresh.rs`。
- `create_iceberg_mv`：调用 mv_lineage 与 contract 自检，写元数据。
- `refresh_iceberg_mv`：调用 `validate_schema_contract`，按 decision 推进。
- `refresh_full_iceberg_mv`：drop + recreate target，重生成 contract，写元数据。

### 5. iceberg_target_apply（A9 残留）
位置：`src/engine/mv/iceberg_target_apply.rs`。
- A9 顶层 `MvTargetApplyKey` struct 删除。
- `ensure_target_apply_key_contract` 删除（语义被 contract 段 1+3 包含）。
- `apply_key_table_column` 等 hidden 列构造工具保留。

## 测试

### Unit

`src/engine/mv/schema_contract.rs` 与 `src/sql/analyzer/mv_lineage.rs` 自带 `#[cfg(test)]`：

1. **MvSchemaContract serialization roundtrip**：JSON 字段顺序 / 完整性。
2. **build_projection_filter_lineage**：
   - 简单 column ref → kind=Column。
   - Cast → kind=Cast。
   - Function / 运算符 → kind=Func。
   - 多列引用 → referenced 排序去重。
   - Filter 单独 lineage。
3. **validate_schema_contract**：
   - 全 match → `CompatibleSafe`。
   - 被引用列 rename → `CompatibleSafeWithRebind`。
   - 被引用列 drop → `Incompatible(BaseFieldDropped)`。
   - 被引用列 type 变 → `Incompatible(BaseFieldTypeChanged)`。
   - 未被引用列 add/drop/rename → `CompatibleSafe`。
   - Base UUID 变 → `Incompatible(BaseTableIdentityChanged)`。
   - Target visible rename → `Incompatible(TargetVisibleFieldRenamed)`。
   - Hidden apply-key 丢失 → `Incompatible(HiddenApplyKeyContractBroken)`。
   - Reorder（field id 不变）→ `CompatibleSafe`。
   - Schema id 快路命中 → `CompatibleSafe`（不进段 2 / 3）。
### SQL / Integration

加在 `tests/sql-tests/suites/iceberg-ivm/`：

- `iceberg_ivm_a11_base_rename_referenced` — rename 后 incremental refresh 成功。
- `iceberg_ivm_a11_base_rename_unreferenced` — 通过。
- `iceberg_ivm_a11_base_drop_referenced` — fail with `BaseFieldDropped`。
- `iceberg_ivm_a11_base_drop_unreferenced` — 通过。
- `iceberg_ivm_a11_base_drop_add_same_name` — fail with `BaseFieldDropped`。
- `iceberg_ivm_a11_base_type_change_referenced` — fail with `BaseFieldTypeChanged`。
- `iceberg_ivm_a11_base_add_unrelated_column` — 通过。
- `iceberg_ivm_a11_base_reorder_columns` — 通过。
- `iceberg_ivm_a11_base_uuid_changed` — fail with `BaseTableIdentityChanged`。
- `iceberg_ivm_a11_target_field_id_mismatch` — 显式破坏 target，fail。
- `iceberg_ivm_a11_full_rebuild_after_evolution` — base drop 被引用列 → `REFRESH FULL`
  成功，新 contract 反映新 schema，后续 incremental refresh 通过。

### Regression

- A9 已有 `iceberg_ivm_base_delete_row_lineage` 继续通过（contract 取代 A9 局部 guard，
  语义等价）。
- `SELECT * FROM mv` 仍不显示 `__nova_base_row_id`。
- A9 顶层 `MvTargetApplyKey` 字段移除后，旧测试 fixture 更新为 contract 形态。

## 验收

- Iceberg base 列 rename：如果 SELECT 依赖 field id 未变，refresh 仍正确；
  按名称歧义场景明确报错。
- Drop referenced 列后 refresh fail fast，不返回旧数据。
- Add unrelated 列后 append-only MV incremental refresh 继续通过。
- Target schema field id 在重启、refresh、full rebuild 后保持稳定。
- Full rebuild 后 contract 重新固化。

## 不建议的绕过

- 不要只保存 SELECT SQL 然后每次按名字 rebind。Contract 必须固化 field id。
- 不要把 schema evolution 一律 full refresh。Full refresh 仅作为修复路径之一。
- 不要让 target hidden identity columns 与用户输出列共享临时 field id 规则。
- 不要把 `mv_rebind_hint` 暴露给普通查询路径，仅在 MV refresh 内部使用。
- 不要在 incremental refresh 后修改 contract。Contract 只在 CREATE / full rebuild 时生成。
- 不要 catch `SchemaEvolutionError` 后 fallback rebuild；显式 fail fast 是 contract 的核心价值。

## 与后续任务的衔接

- **IVM-A2 snapshot range**：A11 当前只固化 CREATE 时刻的 base schema；A2 完整 snapshot
  range 需要"读历史 schema" 时，扩展 `BaseContract` 为 `Vec<BaseSchemaSnapshot>` 或追加
  `base_schema_history` 字段，contract_version +1。
- **IVM-A12 MvMetaRepository trait**：A11 仍直接使用具体 repository 类型；A12 启动时把
  contract 的 CRUD 一并放到 trait 上。
- **Aggregate / Join 扩展**：`ExpressionLineage` 与 `BaseContract` 已留下结构空间。新形态
  需要新的 lineage builder（output 不再是 row-preserving），但 contract 数据模型可以增量
  扩充（例如增加 `group_by_lineage`、`join_condition_lineage`），不破坏当前形态。
