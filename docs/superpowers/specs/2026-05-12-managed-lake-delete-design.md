# Managed-Lake DELETE 与 StarRocks 对齐设计

- **日期**:2026-05-12(原设计);**2026-05-13 实施前现状核验更新见 §10**
- **任务**:INT-2 managed-lake DELETE 支持
- **作者**:Claude (Opus 4.7) + Harbor Liu
- **状态**:设计完成 + 现状核验完成,待实施

> **重要**:写 plan 时做了一次代码核验,发现 NovaRocks 已经实现的基础设施远多于设计期预估。**§4 / §5 / §8 涉及"新建文件"和"代码量/时间估计"的部分,以 §10 为准**。前面章节保留设计意图与决策记录,§10 列出与现状对照的具体修正。

---

## 1. 总体目标与产品行为

把 managed-lake 上的 `DELETE FROM t WHERE ...` 实现替换为**与 StarRocks 共享数据(lake)模式语义对齐**的版本。

### 1.1 产品行为(对外可见)

| 表类型 | 接受的 WHERE | 出错信息(对齐 StarRocks)|
|---|---|---|
| **DUPLICATE KEY** | 任意列;`= != < <= > >= IN NOT IN IS NULL IS NOT NULL`;只支持 AND(无 OR);浮点列上禁用 `=`;无函数/子查询/JOIN | 复用 StarRocks `DeleteAnalyzer` 报错措辞 |
| **UNIQUE / AGGREGATE KEY** | **只允许 key 列**,其它与 DUP 相同 | `Only key column can be used in conditions...` |
| **PRIMARY KEY** | 任意 WHERE(含函数、子查询、CTE);只要能 plan 成 SELECT 即可 | — |
| **物化视图** | 拒绝 | `The data of '<mv>' cannot be deleted because it is a materialized view...` |
| **Iceberg / 其它 connector** | 不变(继续走 `delete_flow.rs` 现有 Iceberg 路径)| — |

### 1.2 移除 / 替换

- 删除 `delete_flow.rs:354-395` 的 CoW `execute_managed_delete_statement` 整段。
- 删除其相关单测 `src/engine/mod.rs:4765` `managed_delete_rewrites_remaining_rows_for_primary_key_table`(替换为新路径集成测试)。

### 1.3 不在范围

- `TRUNCATE TABLE`、`DROP TABLE`(`truncate_managed_table` helper 继续给 TRUNCATE 用)
- `INSERT` / `UPDATE` / `MERGE` 语义
- Sink 侧 `__op` 列协议的对外暴露(stream load 协议)— 本 spec 让内部 sink 路径**为它就绪**,但不实现协议本身

---

## 2. 项 D — 测试 runner SQL splitter 修复

### 2.1 问题

`tests/sql-test-runner/src/session.rs:262-303` 的 `split_sql_statements` 不识别 SQL 注释。`datetime_microsecond_precision_delete.sql` 第 45 行 `-- '00:00:00.0' is same as '00:00:00'; rows already gone` 注释里的 `;` 被当作分隔符,产生伪 SQL `rows already gone\nDELETE FROM ...`,server 返回 `unsupported sql in standalone server v1`。

### 2.2 修复

`split_sql_statements` 状态机扩展两种新状态:

- `LineComment` — `--` 开头,需要后跟空白或行尾(MySQL 规则:`a--b` 不算注释),到行尾结束
- `BlockComment` — `/* ... */`,**不支持嵌套**(对齐 MySQL)

注释状态下 `;` `'` `"` `` ` `` 全部被忽略,只看状态退出条件。

### 2.3 测试(L1)

`tests/sql-test-runner/src/session.rs` 单测覆盖:

- `--` 注释里的 `;` 不被切
- `/* */` 块注释里的 `;` 不被切
- 引号字符串里的 `--` `/*` 不被当成注释
- `a--b`(无空白)不被当成注释开头
- 嵌套块注释视为不嵌套

### 2.4 代码量

~60 行(splitter 改动 + ~5 个单测)。

### 2.5 风险

可能让某些原本"凑巧通过"的用例改变拆分结果。实施时跑一遍全部 sql-tests 看回归。

---

## 3. 项 C — 字面量与列类型的隐式 cast(C2 范围)

### 3.1 问题

`WHERE c2 = '2020-01-01 00:00:00.012'` 当 `c2` 是 `DATETIME(6)` 时,字面量保持为 STRING,比较被按 STRING 做(或按降级精度做),导致 `'2020-01-01 00:00:00.012' != '2020-01-01 00:00:00.012000'`,匹配不到行。

### 3.2 对齐目标

对齐 StarRocks `LiteralExprFactory.create(value, columnType)` — 在 analyzer 阶段把 `<column> <op> <literal>` 中的字面量按列类型重建为 typed literal,并保留列声明的精度(DATETIME 默认 microsecond)。

### 3.3 范围(C2,宽)

**覆盖语句**:`SELECT`、`DELETE`、`UPDATE`、`MERGE` 的 WHERE / ON / HAVING 谓词,以及 INSERT VALUES。所有走通用 analyzer 类型协调的表达式入口。

**覆盖比较**:`= != < <= > >=`、`IN (lit, ...)`、`NOT IN (...)`、`BETWEEN lit AND lit`。`IS NULL / IS NOT NULL` 无字面量,跳过。

**覆盖类型对**(列类型 ← 字面量类型):
- DATETIME ← STRING / DATE
- DATE ← STRING
- DECIMAL(p,s) ← STRING / INT / FLOAT
- INT 系列 ← STRING(全数字)
- 已经类型匹配的不动

### 3.4 实现位置

新增 `src/sql/analyzer/literal_coercion.rs`,导出:

```rust
pub fn coerce_predicate_literals(expr: &mut Expr, schema: &Schema, options: CoerceOptions);
```

集成入口:
1. SELECT analyzer 的 WHERE / HAVING / JOIN ON 处理处
2. `delete_flow::execute_managed_delete_statement`(§4 的输入)
3. UPDATE / MERGE 的 condition 处理处

### 3.5 STRING → DATETIME 保精度算法

输入 `s`,目标列 scale 默认 6:

1. 尝试按完整 `YYYY-MM-DD HH:MM:SS[.ffffff]` parse
2. 小数部分:
   - 长度 ≤ 6:补 0 到 6 位 → microsecond
   - 长度 > 6:**报错**("Datetime literal '...' is invalid")
3. 不带小数 → microsecond = 0

复用 `src/exec/expr/cast.rs` 的 datetime cast 实现,不重新实现解析。

### 3.6 错误行为

- 字面量 parse 失败:报错,**不 fallback** 到字符串比较。措辞:`cannot cast literal 'foo' to DATETIME for column 'c2'`
- 字面量精度溢出:报错(对齐 StarRocks)
- DECIMAL 精度损失行为:**实施时按 StarRocks 实测行为定**(报错 / 截断)
- 复杂表达式(`expr op expr`、含子查询):**不动**,留运行时计算

### 3.7 测试(L1)

`literal_coercion.rs` 单测:
- DATETIME(6) ← `'2020-01-01 00:00:00.012'` → microsecond=12000
- DATETIME(6) ← `'2020-01-01 00:00:00'` → microsecond=0
- DATETIME(6) ← `'...0.1234567'`(7 位)→ 报错
- DATE ← `'2020-13-01'` → 报错
- DECIMAL(10,2) ← `'12.345'` → 按 StarRocks 实测定
- INT ← `'42'` → 42
- `IS NULL` / `BETWEEN` / `IN (...)` 列表每个字面量都 cast

### 3.8 代码量

~450 行(模块 ~250 + 集成 ~50 + 单测 ~150)。

### 3.9 风险

- **类型协调副作用**:某些用例可能依赖现状的 "string-string 比较"。对齐 StarRocks 后行为改变 — 但这正是用户要求方向
- **DECIMAL 精度损失行为**:实施时 double-check StarRocks 实测
- 现有 SELECT 用例可能从"漏匹配"修复为"正确匹配" — 全套回归必须跑

---

## 4. 项 A — DUP/UNIQUE/AGG via DeletePredicate

> **现状核验(2026-05-13)**:`DeletePredicatePb` 写端、FFI 入口、tablet txn_log 写入、publish 识别均**已实现**。FE-compatible 模式(StarRocks FE → Shim → FFI)走 `delete_data` RPC 端到端贯通。Standalone(SQL)模式只缺 FE 端的 WHERE → `DeletePredicatePb` 翻译,以及调用入口。具体清单见 §10.2。

### 4.1 路径概览

```
delete_flow.rs::execute_managed_delete_statement
  ↓
  ├─ §4.2 路由:取 keys_type
  │   ├─ PRIMARY_KEYS → 走 §5
  │   ├─ MV → 报错
  │   └─ DUP/UNIQUE/AGG → 本节
  │
  ├─ §4.3 WHERE 翻译 + 校验
  │   ├─ 调 §3 coerce_predicate_literals
  │   ├─ 校验:仅 conjunctive(AND 扁平展开)
  │   ├─ 校验:每个原子是 col-op-literal / col IN (...) / col IS (NOT) NULL
  │   ├─ 校验:UNIQUE/AGG 仅 key 列;DUP 任意列
  │   ├─ 校验:浮点列禁用 =
  │   └─ 翻译为 DeletePredicateTerms
  │
  ├─ §4.4 构造 DeletePredicatePB
  ├─ §4.5 构造 TxnLog { OpWrite { rowset: empty + delete_predicate } } 并 publish
  └─ §4.6 缓存失效,返回 Ok
```

### 4.2 路由

```rust
let keys_type = resolved.keys_type();  // "DUP_KEYS" | "UNIQUE_KEYS" | "AGG_KEYS" | "PRIMARY_KEYS"
match keys_type {
    "PRIMARY_KEYS" => execute_managed_pk_delete(...),    // §5
    _ if is_materialized_view => Err(...),
    _ => execute_managed_predicate_delete(state, target, stmt, keys_type),
}
```

`resolved.keys_type()` 来自 `src/connector/starrocks/managed/store.rs:74` 的 `StoredManagedTable.keys_type`。

### 4.3 WHERE 翻译与校验

新增 `src/engine/delete_predicate_translate.rs`:

```rust
pub fn translate_to_delete_predicate(
    where_expr: &sqlast::Expr,
    schema: &TableSchema,
    keys_type: &str,
) -> Result<DeletePredicateTerms, String>;

pub struct DeletePredicateTerms {
    pub binary: Vec<BinaryTerm>,
    pub in_list: Vec<InTerm>,
    pub is_null: Vec<IsNullTerm>,
}
```

**算法**:

1. 展平 AND;`OR` 出现报错:`DELETE on <keys_type> table: OR is not supported, only AND of comparisons / IN / IS NULL`
2. 对每个原子:
   - `col op lit`(op ∈ `= != < <= > >=`)→ `BinaryTerm`
   - `col IN (...)` / `col NOT IN (...)` → `InTerm`
   - `col IS NULL` / `col IS NOT NULL` → `IsNullTerm`
   - 其它 → 报错
3. 列校验:
   - 列不存在 → 报错
   - UNIQUE/AGG 非 key 列 → `Where clause only supports key column 'col_name'`
   - 浮点列 `=` / `!=` / `IN` → `Don't support float column 'col_name' in delete condition`
4. 字面量已经在 §3 cast 为 typed value;此处序列化成 StarRocks `BinaryPredicatePB.value` 字符串格式:
   - INT → `"42"`
   - DATETIME(microsecond) → `"2020-01-01 00:00:00.012000"`(6 位定长)
   - DATE → `"2020-01-01"`
   - DECIMAL(p,s) → 按列 scale 输出固定小数位(例如 DECIMAL(10,2) 的值 `12.3` 序列化为 `"12.30"`,而非 `"12.3"`);**最终格式以 StarRocks BE 实测输出为准,实施时 lock**
   - STRING → 字符串本身
   - BOOL → `"0"` / `"1"`

序列化规则集中在 `delete_predicate_translate.rs::starrocks_lit_to_string`,与 §5 共用。

### 4.4 DeletePredicatePB 构造

新增 `src/connector/starrocks/lake/delete_predicate_proto.rs`:

```rust
pub fn build_delete_predicate_pb(
    terms: &DeletePredicateTerms,
    version: i32,
) -> DeletePredicatePB;
```

直接填现有 proto(`idl/proto/lake_types.proto`)的 `DeletePredicatePB { version, in_predicates, binary_predicates, is_null_predicates }`。`sub_predicates`(旧 hybrid 兼容)**不填**。

### 4.5 TxnLog 构造与 publish

新增:

```rust
pub fn append_lake_txn_log_delete_predicate(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    tablet_ids: &[i64],
    delete_predicate: DeletePredicatePB,
) -> Result<i64 /* txn_id */, String>;
```

每个 tablet 写一份 `TxnLog { txn_id, tablet_id, op_write: OpWrite { rowset: empty_rowset_with_delete_predicate } }`。

**empty_rowset_with_delete_predicate**:
- `rowset_seg_id`:新分配
- `num_rows = 0`
- `num_segments = 0`
- `delete_predicate`:填入构造好的 `DeletePredicatePB`
- `version`:由 publish 阶段填

**新增 publish 子路径** `apply_dup_predicate_write_log_to_metadata`:
- 把 rowset(空 segments + delete_predicate)挂到分区元数据
- 推进版本号
- 记 SQLite metadata

### 4.6 读端

**零工作**。`src/formats/starrocks/reader/record_batch.rs:1164-1429` 已支持 `DeletePredicatePB` scan-time 应用,新写入的 rowset 会被 `src/formats/starrocks/metadata.rs:757-859` 的 `collect_delete_predicates` 一并 collect。

### 4.7 测试

**L1 单测**:见 §6.2 "项 A"。

**L3 集成用例**(`sql-tests/write-path/`):

- `managed_dup_delete_non_key_col` — DUP 表非 key 列 DELETE
- `managed_unique_delete_keyonly` — UNIQUE 表 key 列 DELETE
- `managed_unique_delete_nonkey_rejected` — UNIQUE 表非 key 列拒绝
- `managed_agg_delete_keyonly` — AGG 表
- `managed_dup_delete_or_rejected` — `OR` 拒绝
- `managed_dup_delete_in_list` — `IN`
- `managed_dup_delete_is_null` — `IS NULL`
- `datetime_microsecond_precision_delete`(已有)— §3 + 本节落地后应当全绿

### 4.8 代码量(更正后,见 §10.2)

| 模块 | 估计 |
|---|---|
| `delete_predicate_translate.rs`(WHERE → terms + literal 序列化) | ~400 |
| `delete_predicate_proto.rs`(terms → `DeletePredicatePb`)| ~80(比原估的 150 少,因为只是 proto 字段填充,不涉及 publish 路径)|
| `delete_flow.rs` DUP/UNIQUE/AGG 分支 + dispatch 到现有 `transactions::delete_data` | ~150 |
| 单元测试 | ~450 |
| 集成测试 | 7 用例 |
| **合计** | **~1080 行 + 7 用例** |

**省去**:`txn_log.rs` 新 entry + publish 子路径 — **完全不需要新建**,`transactions::delete_data` + `applier.rs` 现有 publish 已经识别 `delete_predicate.is_some()`(applier.rs:101)。

### 4.9 风险

- **`OpWrite { rowset: empty + delete_predicate }` 是 publish 路径新形态**。实施时第一刀跑通,否则后面活全卡(G2 gate)
- **`BinaryPredicatePB.value` 字符串编码细节**(DATETIME 补 0、DECIMAL scale 补 0):集中在 `starrocks_lit_to_string` + 完整单测
- **跨 tablet 原子性**:复用现有 publish stage/activate 二段提交

---

## 5. 项 B — PRIMARY KEY via `__op` + sink

> **现状核验(2026-05-13)**:`__op` 列识别 / 按值拆 chunk / `.del` 文件编码 / `OpWrite { rowset, dels[] }` 构造 / PK encoder / publish 路径 — **全部已经实现**。NovaRocks 已经能从 stream load 视角处理混合 upsert+delete batch。Standalone SQL DELETE 缺的只是 FE 端把 `DELETE FROM pk_t WHERE cond` 改写为带 `__op = 1` 的 chunk 并发送给现有 sink。`__op` 列在 SQL analyzer/optimizer 路径上**目前完全没碰过**,需要新加最小保护机制(name-based helper + 防御性单测)。具体清单见 §10.3。

### 5.1 设计意图

NovaRocks managed-lake 是 StarRocks PK 表的 cloud-native 形态。**未来必然要接 stream load 协议**(Flink CDC、Debezium 入口);stream load 在字节流上必须用 `__op` 列编码每行 op,这是对外协议硬约束。

既然 `__op` 在协议层必须保留,内部就**完全沿用 StarRocks PK 表的 `__op` + sink + split 设计**,不在内部和协议之间做翻译层。代价(`__op` 在 plan 内部作为特殊列存在)通过 §5.3 集中处理收敛,不让它污染整个优化器。

**设计意图旁注**(精简版):

> StarRocks PK 表用 `__op` 不是因为行号不可见(类型化 sink 也不要求 FE 知道行号),而是因为 **stream load 协议要求把 op 编码到字节流的行上**;SQL 路径只是搭了顺风车复用协议。NovaRocks 未来必然要做 stream load,因此 `__op` 必须保留;既然协议层必有 op 列,索性内部也走 op 列以避免两套表达,跟 StarRocks PK 表实现完全同构。这个选择的代价(类型污染)通过 §5.3 集中处理收敛。

### 5.2 FE 改写

`delete_flow::execute_managed_pk_delete` 把 `DELETE FROM pk_t WHERE cond` 改写为:

```sql
SELECT pk_col1, pk_col2, ..., 1 AS __op
FROM t WHERE cond
```

走现有 managed-lake sink,sink 内部按 `__op` 拆分(§5.4)。

`__op` 是 TINYINT 字面量,由 analyzer 当作 const projection 加入,**不在用户 SQL 里出现**。

任意 WHERE 都接受(子查询、JOIN、CTE…只要能 plan 成 SELECT),对齐 StarRocks PK DELETE。

### 5.3 `__op` 列在优化器中的集中处理

新增 `src/sql/analyzer/load_op_column.rs`:

```rust
pub const LOAD_OP_COLUMN: &str = "__op";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoadOp {
    Upsert = 0,
    Delete = 1,
    // Update = 2,  // reserved, sink rejects with "unsupported __op value"
    // Insert = 3,  // reserved
}

pub fn is_load_op_column(name: &str) -> bool { name == LOAD_OP_COLUMN }
```

**`ColumnKind` 扩展**:

```rust
pub enum ColumnKind {
    Normal,
    HiddenMeta,     // _file, _pos
    LoadOp,         // __op
}
```

**优化器各 pass 短路**:

| Pass | 行为 |
|---|---|
| 列裁剪 | `__op` 标 sink-required,不可裁 |
| 谓词下推 | 不参与(用户写 `WHERE __op = 1` 报 "unknown column") |
| CBO selectivity | 直接跳过 |
| 字面量协调(§3) | 跳过 TINYINT const 列 |
| EXPLAIN | 标 `LOAD_OP` 便于审计 |

**集中点**:所有 pass 通过 `is_load_op_column` / `ColumnKind::LoadOp` 一处判断,**没有任何 pass 手写 `"__op"` 字面量**。clippy lint 检测非 `load_op_column.rs` 模块出现 `"__op"` 字面量时报警。

**`__op` 值域当前 2 值**(`0=upsert`、`1=delete`),enum 类型预留 4 个槽位(`Update=2`、`Insert=3` 标 reserved),sink 收到未实现值时报 `unsupported __op value`。未来 MERGE/UPDATE 扩展不改 wire format。

### 5.4 Sink 内部按 `__op` 拆 chunk

位置:`src/connector/starrocks/sink/operator.rs::process_chunk`。

```rust
fn process_chunk(&mut self, chunk: RecordBatch) -> Result<(), Error> {
    let op_idx = chunk.schema().index_of(LOAD_OP_COLUMN);
    if op_idx.is_err() {
        // fast path: pure INSERT, no __op column
        return self.write_upsert_segment(chunk);
    }
    let op_col = chunk.column(op_idx?).as_any().downcast_ref::<Int8Array>()?;
    let (upsert_mask, delete_mask) = split_by_op(op_col)?;
    let chunk_no_op = remove_column(chunk, op_idx?);

    if !upsert_mask.is_empty() {
        let upsert_chunk = filter_rows(&chunk_no_op, &upsert_mask)?;
        self.write_upsert_segment(upsert_chunk)?;
    }
    if !delete_mask.is_empty() {
        let delete_chunk = filter_rows(&chunk_no_op, &delete_mask)?;
        self.write_delete_file(delete_chunk)?;
    }
    Ok(())
}
```

特性:

- **`__op` 列在拆分时被剥离**,不进 segment 不进 `.del`(对齐 StarRocks `MemTable::_split_upserts_deletes`)
- **混合 chunk 自然支持**:同一 sink 调用里 upsert + delete → 一个 TxnLog 同时填 `rowset` 和 `dels` → 为 stream load 混合事务铺路
- **DELETE 入口当前不产生混合 chunk**(都是 `__op=1`),但 sink 路径支持它

### 5.5 `.del` 文件写入

新增 `src/connector/starrocks/lake/del_file_writer.rs`:

```rust
pub fn write_del_file(
    file_path: &str,
    fs_factory: &Arc<dyn FileSystemFactory>,
    pk_column: &dyn Array,
    pk_schema: &[ColumnSchema],
) -> Result<DelFileMeta, String>;

pub struct DelFileMeta {
    pub file_name: String,
    pub file_size: u64,
    pub encryption_meta: Vec<u8>,
}
```

- 路径:`<tablet_root>/data/<txn_id>_<seg>.del`
- 内容格式:**字节兼容 StarRocks BE**(`ColumnArraySerde` 序列化的 PK-only encoded column)
- **实施前置(G1 gate)**:用 NovaRocks 已知 PK 编码样本与 StarRocks BE 输出做字节级 round-trip

**PK encoder**:`src/connector/starrocks/lake/pk_encoder.rs`(实施时确认是否复用现有,否则新建)。StarRocks `PrimaryKeyEncoder::encode_selective()` 规则:
- 数值列:big-endian,有符号需要 sign bit 翻转
- STRING:UTF-8 + `\0` 终结符
- DATE / DATETIME:i64 数值编码
- 多列 PK:按 PK 列顺序拼接

### 5.6 TxnLog 形态

Sink 在 finalize 时构造:

```rust
TxnLog {
    txn_id,
    tablet_id,
    op_write: OpWrite {
        rowset: RowsetMetadataPB {
            num_rows: upsert_count,
            num_segments: <count>,
            segments: vec![<segment_paths>],
        },
        dels: vec!["<txn_id>_<seg>.del", ...],
    },
}
```

- **纯 DELETE chunk**:`rowset.num_segments = 0`、`dels` 非空
- **纯 UPSERT chunk**(现有 INSERT):`rowset.num_segments > 0`、`dels` 空
- **混合 chunk**:两者都填

与 StarRocks `TxnLogPB.OpWrite` 提交形态完全一致。

### 5.7 Publish

走 `src/connector/starrocks/lake/pk_applier.rs:68` 现有 `apply_primary_key_write_log_to_metadata`,零改动。**基础设施已就绪**。

### 5.8 删除 CoW

- 删除 `delete_flow.rs:354-395` `execute_managed_delete_statement` 整段
- 删除 `src/engine/mod.rs:4765` `managed_delete_rewrites_remaining_rows_for_primary_key_table` 单测
- `truncate_managed_table` 保留(`TRUNCATE TABLE` 仍要用)

**约束**:CoW 删除**只能在 §5 完成、新 PK 路径与新 DUP/UNIQUE/AGG 路径都已 merge 的子 PR 内**进行,不可预删。

### 5.9 已知 limitation

- **PK index 每次 publish 全量重建**(`pk_applier` 当前实现):大表 DELETE 慢。本 spec **不优化**,纳入未来工作("PK persistent index" 单独立项)
- **`__op` 值域当前 2 值**:未来 MERGE/UPDATE 时扩 sink 分支,不改 wire format

### 5.10 代码量(更正后,见 §10.3)

| 模块 | 估计 |
|---|---|
| `load_op_column.rs`(name helper + LoadOp enum) | ~80 |
| `__op` 列 analyzer/optimizer 防御性单测 + minimal 集成 | ~80 |
| `delete_flow::execute_managed_pk_delete` + plan 改写(走 standalone 现有 SELECT pipeline → sink)| ~300 |
| 删 CoW + 删旧单测 | ~30 |
| 单元测试 | ~400 |
| 集成测试 | 5 用例 |
| **合计** | **~890 行 + 5 用例** |

**省去**:`del_file_writer.rs`(已存在 [src/connector/starrocks/lake/delete_payload_codec.rs](src/connector/starrocks/lake/delete_payload_codec.rs))、`pk_encoder.rs`(已存在 [src/connector/starrocks/lake/pk_applier.rs:573-595](src/connector/starrocks/lake/pk_applier.rs#L573))、`sink/operator.rs::process_chunk` 拆分逻辑(已存在 [txn_log.rs:1394 parse_op_batch](src/connector/starrocks/lake/txn_log.rs#L1394) + 集成到 [sink/operator.rs:1071](src/connector/starrocks/sink/operator.rs#L1071) → [txn_log.rs:75 append_lake_txn_log_with_chunk_rowset](src/connector/starrocks/lake/txn_log.rs#L75))。

### 5.11 风险

1. **`__op` 列污染**:某个新 pass 漏判断 → 数据被错位写。clippy lint + 每 pass 单测控制
2. **Sink 现有 INSERT 回归**:`process_chunk` 增加 split → 影响所有 PK 表 INSERT。纯 INSERT fast path + 全 INSERT 回归
3. **`.del` 字节兼容 StarRocks**:G1 gate 验证
4. **混合 chunk 当前不产生但 sink 必须支持**:构造混合 chunk 单测保证路径稳定

---

## 6. 测试策略

### 6.1 测试金字塔

| 层级 | 数量 | 跑在哪 | 解决什么 |
|---|---|---|---|
| **L1 Rust 单测** | ~25 个 | `cargo test` | 算法纯函数级正确性 |
| **L2 Rust 集成测**(`src/engine/mod.rs::tests`)| ~6 个 | `cargo test` | 单进程内 DELETE 端到端 |
| **L3 SQL 回归测**(`sql-tests/write-path/`)| 已有 3 + 新增 ~10 | sql-tests runner + docker | 跨进程真实 DELETE 行为 |
| **L4 跨引擎字节兼容**(可选)| ~2 个 | 手工 + Spark/StarRocks | 外部引擎读 NovaRocks 写的文件 |

### 6.2 L1 单测覆盖矩阵

**项 D — splitter**:
- `--` 注释里 `;` 不被切
- `/* */` 块注释里 `;` 不被切
- 引号字符串里 `--` `/*` 不被当注释
- `a--b` 不被当注释开头
- 嵌套块注释视为不嵌套

**项 C — literal coercion**:
- DATETIME(6) ← `'2020-01-01 00:00:00.012'` → microsecond=12000
- DATETIME(6) ← `'2020-01-01 00:00:00'` → microsecond=0
- DATETIME(6) ← 7 位小数 → 报错
- DATE ← 非法日期 → 报错
- DECIMAL(10,2) ← `'12.345'` → 按 StarRocks 实测
- INT ← `'42'` → 42
- IS NULL / BETWEEN / IN 列表中每个字面量都 cast

**项 A — delete predicate translate**:
- 合法 WHERE → 正确 terms(覆盖所有支持算子)
- `OR` 报错(完整错误措辞)
- 函数表达式报错
- 子查询报错
- `col1 = col2` 报错
- UNIQUE/AGG 非 key 列报错(措辞与 StarRocks 一致)
- DUP 非 key 列通过
- 浮点列 `=` 报错
- DATETIME(6) 字面量序列化保 6 位精度
- `IN (1,2,3)` → InTerm

**项 A — delete predicate proto round-trip**:
- terms → DeletePredicatePB → terms 语义一致
- `sub_predicates` 必为空(对齐 lake mode)

**项 B — `__op` 列优化器短路**:
- 列裁剪不裁 `__op`
- `WHERE __op = 1` 报 "unknown column"
- 字面量协调跳过
- ColumnKind metadata 正确传播

**项 B — sink chunk split**:
- 纯 `__op=0` → segment writer,`dels` 空
- 纯 `__op=1` → `.del` writer,`rowset.segments` 空
- 混合 chunk → 两者都填
- `__op` 列从输出剥离
- `__op=2/3` 报 `unsupported __op value`

**项 B — `.del` 文件 round-trip**:
- 单列 PK 写 → 读,字节一致
- 多列 PK 编码顺序与 hash routing 一致
- 空 chunk 不产生文件

**项 B — PK encoder**:
- 各类型编码字节
- 多列 PK 拼接顺序
- 与 StarRocks 已知 PK 样本字节对比

### 6.3 L2 Rust 集成测

借用 `src/engine/mod.rs` 现有 `maybe_managed_lake_config()`,6 个用例:

1. `pk_delete_simple_by_pk`
2. `pk_delete_complex_where`
3. `pk_delete_no_match`
4. `pk_delete_then_insert_same_pk`
5. `dup_delete_predicate_simple`
6. `unique_delete_nonkey_rejected`

每用例断言:行可见性、TxnLog 形态、写入文件存在性。

### 6.4 L3 SQL 回归用例

**现有必通用例**:
- `primary_key_insert_delete_select`(继续通过)
- `primary_key_upsert_delete_select`(继续通过)
- `datetime_microsecond_precision_delete`(D+C 后进 step 3;A 后全过)

**新增用例**(每个 `.sql` + `.result`):
1. `managed_dup_delete_non_key_col`
2. `managed_unique_delete_keyonly`
3. `managed_unique_delete_nonkey_rejected`(错误用例)
4. `managed_agg_delete_keyonly`
5. `managed_dup_delete_or_rejected`(错误用例)
6. `managed_dup_delete_in_list`
7. `managed_dup_delete_is_null`
8. `managed_pk_delete_complex_where`(子查询 / JOIN)
9. `managed_pk_delete_no_match`
10. `managed_mv_delete_rejected`(错误用例)

### 6.5 L4 跨引擎字节兼容(可选)

- NovaRocks 写 `.del` → StarRocks BE 读
- StarRocks BE 写 `.del` → NovaRocks 读
- DeletePredicatePB 双向 round-trip

CI 无 BE 镜像时,手工脚本一次性验证,implementation notes 记录,**不阻塞 merge**;但任何 `.del` 编码或 PK encoder 改动要重跑。

### 6.6 回归基线

| Suite | 期望 |
|---|---|
| `write-path` | 目标 3 + 新增 10 全绿 |
| `cte`、`join`、`filter`、`sort`、`function` | 不引入回归(C 项可能影响)|
| `tpc-h`、`tpc-ds`、`ssb` | 不引入回归(C 项影响 SELECT)|
| `iceberg`、`iceberg-rest`、`iceberg-compatibility` | 全不变 |
| `cargo test` | 通过 |

实施中每项落地后跑增量 `write-path`;A/B 全落地后**强制**全 suite 回归。

### 6.7 测试代码量

| 类型 | 估计 |
|---|---|
| L1 Rust 单测 | ~1500 行 |
| L2 Rust 集成测 | ~400 行 |
| L3 SQL 新增 | ~10 用例 |
| L4 手工脚本 | ~100 行(可选)|

---

## 7. 风险与回滚

### 7.1 风险矩阵

| ID | 风险 | 概率 | 影响 | 控制 / 回滚 |
|---|---|---|---|---|
| **R1** | `.del` 文件字节格式与 StarRocks 不兼容 | 中 | 高 | G1 gate:实施第一刀字节级 round-trip |
| **R2** | `__op` 列污染优化器 | 中 | 中 | clippy lint + 每 pass 单测 |
| **R3** | Sink 改造引入 INSERT 回归 | 中 | 高 | 纯 INSERT fast path + 全 INSERT 回归 |
| **R4** | `DeletePredicatePB` 空 rowset publish 路径未走过 | 中 | 中 | G2 gate:实施第一刀打通 |
| **R5** | `BinaryPredicatePB.value` 编码小细节 | 高 | 中 | 集中 `starrocks_lit_to_string` + known-sample 单测 |
| **R6** | C 项引入全局 SELECT 回归 | 中 | 高 | C 放最早(§8 M2),全套回归数据再继续 |
| **R7** | `pk_applier` 全量重建大表慢 | 高(已知)| 低 | known limitation,独立立项 |
| **R8** | 跨 tablet 部分 publish 失败 | 低 | 高 | 复用 stage/activate 二段提交 |
| **R9** | `truncate_managed_table` 被误删 | 低 | 中 | spec 明确保留 + TRUNCATE 烟测 |
| **R10** | D 项改动让某些用例拆分变化 | 中 | 低 | A/B/C/D 前后各跑完整 sql-test |

### 7.2 回滚单元

- **D 回滚**:revert splitter 单 commit
- **C 回滚**:revert `literal_coercion.rs` + analyzer 集成
- **A 回滚**:revert DUP/UNIQUE/AGG 分支;表 DELETE 退到"未实现"(报错),不退回 CoW(CoW 已删)
- **B 回滚**:revert PK 路径;PK DELETE 退到"未实现"(报错)

**关键约束**:CoW 删除不能比 A 或 B 任意一个先 merge — 否则 DELETE 在过渡窗口无路径可走。

### 7.3 不在范围(显式声明)

- PK 表 UPDATE
- MERGE
- Stream load 协议实现
- PK persistent index(SST 化)
- Iceberg DELETE 路径改造
- `DELETE ... USING` / CTE-WHERE
- DUP 表 OR、函数、子查询 WHERE(显式拒绝)

### 7.4 可观测性

实施时加日志和 metric:

- `delete_flow::execute_managed_pk_delete` 入口:tablet 数、predicate 摘要
- `del_file_writer` 写入:文件路径、行数、大小、CRC
- `pk_applier` publish 时 `changed_deletes` 统计:每 tablet 命中行数、未命中行数
- `sink/operator.rs::process_chunk` split 后:upsert / delete 行数比例
- `DeletePredicatePB` publish:谓词字段数、tablet 数

出问题时能快速定位是 "FE 没传对"、"sink 拆错"、"publish 没应用" 哪段。

---

## 8. 实施顺序

### 8.1 总体策略

按"**修测试基建 → 修通用 analyzer → 上 DeletePredicate → 上 op 列**"四段,每段独立可 merge、可回滚。CoW 删除必须晚于 A、B 任一段。

### 8.2 顺序与里程碑

| 里程碑 | 项 | 子 PR | 退出标准 |
|---|---|---|---|
| **M1** | D | 1 个 | 全 sql-test suite 无回归 |
| **M2** | C | 1 个 | 全 suite 跑通,datetime step 3-6 通过,tpc-h/tpc-ds/ssb 无回归 |
| **M3** | A | A.1 + A.2 | L1+L2+7 个 sql-test 新增用例通过;datetime 继续绿;CoW 仍保留给 PK |
| **M4** | B | B.1 + B.2 + B.3 + B.4 | 3 目标 sql-test 全绿;全 suite 回归通过 |
| **M5** | — | — | 可选 L4 跨引擎兼容 |

**M3 子 PR 拆分**:
- A.1:proto encoder + publish 子路径打通(G2)
- A.2:FE 改造 + 集成测 + sql-test 用例

**M4 子 PR 拆分**:
- B.1:`.del` + PK encoder 字节兼容 round-trip(G1),纯 infra
- B.2:`__op` 列优化器集中处理 + clippy lint
- B.3:sink `process_chunk` 拆分 + 纯 INSERT fast path + 全 INSERT 回归(G3)
- B.4:`delete_flow` PK 分支 + 删 CoW + 5 个 sql-test 用例 + 全 suite 回归(G4 / G5)

### 8.3 子 PR 原则

- 每子 PR 独立通过 CI
- 每子 PR 独立可回滚,不留半成品
- 不留死代码;单子 PR 最长存活 7 天,超过要么推进要么 revert

### 8.4 关键 gate

| Gate | 触发 | 行为 |
|---|---|---|
| **G1** `.del` 字节兼容 | M4 B.1 落地前 | round-trip + 已知样本通过才能继续 B.2-B.4 |
| **G2** 空 rowset publish | M3 A.1 落地前 | publish 写入 + 读端 collect 拿到 |
| **G3** INSERT 全回归 | M4 B.3 后 | sink 改造不影响 INSERT |
| **G4** 全 suite 回归 | M2、M3、M4 各落地后 | hard block 任何回归 |
| **G5** CoW 删除时机 | M4 B.4 内 | 不可预删 |

### 8.5 时间估计(更正后)

| 里程碑 | 工程天 |
|---|---|
| M1 D | 0.5 |
| M2 C | 2-3 |
| M3 A | 3-4(简化,见 §4.8 更正)|
| M4 B | 4-6(大幅简化,见 §5.10 更正)|
| **总计** | **9.5-13.5 工程天** |

不含 review wait time。

**简化原因**:写 plan 时核验代码,发现 `.del` 文件编码、`__op` 列拆分、`DeletePredicatePb` 写入路径、PK encoder 均已实现(详见 §10)。原估计基于"全新建"假设,过保守。

**G1 字节兼容 gate 风险大幅降低**:`encode_delete_keys_payload` 已经存在并有完整单测覆盖 ([delete_payload_codec.rs:653-779](src/connector/starrocks/lake/delete_payload_codec.rs#L653))。实施时仅需做一次"现有实现是否真的字节兼容 StarRocks BE"的回归验证,不需要从零写编码器。

### 8.6 失败模式与决策点

- **G1 失败 > 2 天**:`.del` 字节格式无法对齐 → 暂停 M4,字节格式调研单独立项
- **G3 失败**:sink 改造引入 INSERT 回归不可控 → 暂停 M4,回到 "绕过 sink" 方案,接受 stream load 接入时再补
- **G4 全 suite 回归 > 5 用例**:暂停,定位 root cause 是否需要重新设计

---

## 9. 设计决策记录

| # | 决策 | 选择 | 原因 |
|---|---|---|---|
| 1 | DUP/UNIQUE/AGG WHERE 严格度 | 严格对齐 StarRocks(无 OR、仅 col-op-literal、无函数子查询)| 谓词存元数据扫描时应用,复杂表达式语义脆弱 |
| 2 | C 项范围 | C2(SELECT/DELETE/UPDATE/MERGE 共用 analyzer)| 窄改要么做两次要么覆盖不全 |
| 3 | CoW 旧实现 | 彻底删除 | 无原子性 + O(N) 开销,留着只是 bug surface |
| 4 | `.del` 字节兼容 | 必须字节兼容 StarRocks BE | managed-lake 核心承诺 = StarRocks BE 互操作 |
| 5 | PK 表用 `__op` + sink | 是 | 未来 stream load 协议必然要 `__op`,内部同构避免两层不一致 |
| 6 | `__op` 列在优化器中处理 | 集中到 `load_op_column.rs` + `ColumnKind::LoadOp` + clippy lint | 控制类型污染回归面 |
| 7 | `__op` 值域 | 当前 2 值(`0=upsert, 1=delete`)+ enum 预留 4 槽位 | 对齐 StarRocks PK stream load 协议,wire format 不破坏 |
| 8 | Sink 拆分位置 | `sink/operator.rs::process_chunk` | 离 chunk 来源最近,内存压力低,模块内聚 |

---

## 10. 实施前现状核验 — 现有基础设施清单与修正

写 plan 阶段(2026-05-13)做了一次代码核验,目的是把 spec 假设的"新建"与现实区分清楚。本节是 §4 / §5 / §8 的事实修正,**优先于前面章节的代码量与"新建文件"描述**。

### 10.1 NovaRocks 已实现的 DELETE 相关基础设施

#### 编码 / 序列化

| 能力 | 状态 | 位置 |
|---|---|---|
| PK encoder(单列 + 多列) | ✅ 已实现 | `encode_primary_keys_from_batch`、`encode_primary_key_cell` — [src/connector/starrocks/lake/pk_applier.rs:573-595](src/connector/starrocks/lake/pk_applier.rs#L573);`encode_primary_keys_from_key_batch` — [src/connector/starrocks/lake/txn_log.rs:1916](src/connector/starrocks/lake/txn_log.rs#L1916) |
| `.del` 文件 payload 编码 | ✅ 已实现 + 已测 | `encode_delete_keys_payload` + 5 个 round-trip 单测 — [src/connector/starrocks/lake/delete_payload_codec.rs:28](src/connector/starrocks/lake/delete_payload_codec.rs#L28) |
| `.del` 文件 payload 解码 | ✅ 已实现 | `decode_delete_keys_payload` — 同文件 |

#### `__op` 列处理

| 能力 | 状态 | 位置 |
|---|---|---|
| `LOAD_OP_COLUMN = "__op"` 常量 | ✅ 已定义 | [sink/operator.rs:70](src/connector/starrocks/sink/operator.rs#L70)、[sink/factory.rs:71](src/connector/starrocks/sink/factory.rs#L71) |
| `parse_op_batch` 按 `__op` 拆 chunk(Int8/Int32,NULL 校验,Upsert/Delete/Mixed 三种)| ✅ 已实现 | [txn_log.rs:1394](src/connector/starrocks/lake/txn_log.rs#L1394) |
| `strip_last_op_control_column` 剥离 `__op` 列 | ✅ 已实现 | [txn_log.rs:1506](src/connector/starrocks/lake/txn_log.rs#L1506) |
| Sink schema 识别 `__op`(`is_load_op_column` 等价逻辑分散在 sink) | ✅ 已实现 | [operator.rs:345, 890, 2523, 2590](src/connector/starrocks/sink/operator.rs#L345) |

#### TxnLog / OpWrite / publish

| 能力 | 状态 | 位置 |
|---|---|---|
| `OpWrite { rowset, dels[] }` 完整 schema | ✅ 已实现 | proto `idl/proto/lake_types.proto`;Rust 入口 [txn_log.rs:75 append_lake_txn_log_with_chunk_rowset](src/connector/starrocks/lake/txn_log.rs#L75) |
| 自动按 `__op` 把 chunk 拆成 rowset(upsert) + dels(delete) | ✅ 已实现 | `append_lake_txn_log_with_chunk_rowset` 内部调 `parse_op_batch` |
| `DeletePredicatePb` proto 类型 + 写端 RPC handler | ✅ 已实现 | `delete_data` — [transactions.rs:926](src/connector/starrocks/lake/transactions.rs#L926);`append_delete_data_txn_log` — [transactions.rs:958](src/connector/starrocks/lake/transactions.rs#L958) |
| `DeleteDataRequest` FFI 入口(FE-compatible 路径) | ✅ 已实现 | `novarocks_rs_lake_delete_data` — [service/engine_ffi.rs:656](src/service/engine_ffi.rs#L656) |
| publish 识别 `rowset.delete_predicate.is_some()` | ✅ 已实现 | [applier.rs:101](src/connector/starrocks/lake/applier.rs#L101) |
| publish 识别 `op_write.dels[]` + PK index 查找 + delvec 生成 | ✅ 已实现 | `apply_primary_key_write_log_to_metadata` — [pk_applier.rs:68](src/connector/starrocks/lake/pk_applier.rs#L68);`persist_delvec_updates` — [pk_applier.rs:935](src/connector/starrocks/lake/pk_applier.rs#L935) |

#### 读端

| 能力 | 状态 | 位置 |
|---|---|---|
| `DeletePredicatePb` scan-time 应用 | ✅ 已实现 | `apply_delete_term_to_mask` — [formats/starrocks/reader/record_batch.rs:1164-1429](src/formats/starrocks/reader/record_batch.rs#L1164) |
| `.delvec` 文件读取 + 应用 | ✅ 已实现 | reader 已经按 delvec metadata 跳行 |
| `collect_delete_predicates` 元数据收集 | ✅ 已实现 | [formats/starrocks/metadata.rs:757-859](src/formats/starrocks/metadata.rs#L757) |

### 10.2 §4 项 A 修正(DUP/UNIQUE/AGG)

**spec 原描述**:新建 `delete_predicate_proto.rs`、`txn_log.rs` 新 entry、publish 子路径,共 ~550 行底层 + ~400 行 FE。

**实际剩余工作**:
- `delete_predicate_proto.rs`(~80 行):只是 `DeletePredicateTerms` → `DeletePredicatePb` 字段填充,**不写新 publish 路径**
- `delete_predicate_translate.rs`(~400 行,无变化):WHERE → terms,literal 序列化为 StarRocks 兼容字符串
- `delete_flow.rs` DUP/UNIQUE/AGG 分支(~150 行):构造 `DeleteDataRequest` 或直接构造 `DeletePredicatePb` 并调用 `transactions::delete_data` 等价路径
- 单测 + 集成测(7 用例,~450 行)

**对应 spec §4.5 "新增 publish 子路径 `apply_dup_predicate_write_log_to_metadata`"**:**取消**。`applier.rs:101` 现有路径 + `delete_data` 已经覆盖。`delete_flow` 直接调用 `transactions::delete_data`(可能要包一个 standalone-mode-friendly 的入口,见 plan)。

**G2 gate(空 rowset publish 路径未走过)**:**风险降低**。`transactions.rs:926 delete_data` 已经能写入并 publish,且有读端测试(`txn_log.rs:6128 partial_update_snapshot_uses_lake_rowset_visibility_and_delete_predicates`)。需要做的是**确认 standalone-mode publish 链路同 FFI 链路走的是同一段** publish 代码,不走的话补一个 standalone 入口包装。

**A.1 / A.2 子 PR 拆分调整**:
- A.1:`delete_predicate_translate.rs` + `delete_predicate_proto.rs` + 单测(纯翻译层,无 publish 路径变更)
- A.2:`delete_flow.rs` DUP/UNIQUE/AGG 分支 + 调用 `transactions::delete_data` 等价路径 + 集成测 + sql-test 用例

### 10.3 §5 项 B 修正(PRIMARY KEY)

**spec 原描述**:新建 `pk_encoder.rs` (~200)、`del_file_writer.rs` (~250)、sink `process_chunk` 拆分 (~150)、`load_op_column.rs` + `ColumnKind::LoadOp` (~200) + 各优化器 pass 短路 (~80),共 ~880 行底层 + ~250 行 FE 改写 + ~600 单测。

**实际剩余工作**:
- `load_op_column.rs`(~80 行):name helper(`is_load_op_column`)+ `LoadOp` enum(对外协议字段)。**不需要 `ColumnKind` 枚举改动** — NovaRocks 没有顶层 `ColumnKind` enum(只有 `OutputColumnKind` 在 reader 内部),sink 现状用 name 比较已经够。
- 优化器 pass 防御性单测(~80 行):写测试**验证** `__op` projection 不会被 `column_pruning_rules` ([src/sql/optimizer/rbo/rules/column_pruning.rs](src/sql/optimizer/rbo/rules/column_pruning.rs)) 误裁、不会被 `predicate_pushdown_rbo_rules` 误推。**当前 pass 应当不会误碰**(因为 `__op` 是 plan 顶层 const projection 而非 scan 列),但写测试锁定这个行为。**预计 0 行 production 代码改动**;如果测试失败再补保护。
- `delete_flow::execute_managed_pk_delete`(~300 行):
  - 改写为带 `__op = 1` 列的 chunk(plan 改写 + 执行 + 走现有 sink)
  - 复用 standalone INSERT 现有 plan 流程(`insert_flow::execute_insert_from_query_on_pipeline` 类似入口),不绕过 sink
- 删 CoW + 删旧单测(~30 行净减)
- 单测 + 集成测(5 用例,~400 行)

**对应 spec §5.4 "Sink 内部按 `__op` 拆 chunk"**:**已经实现**,无需新写。spec §5.4 的伪代码描述的是**已存在**的 [txn_log.rs:1394 parse_op_batch](src/connector/starrocks/lake/txn_log.rs#L1394) 等价行为。

**对应 spec §5.5 "`.del` 文件写入 + PK encoder"**:**全部已经实现**。`encode_delete_keys_payload` + `encode_primary_keys_from_batch` 已经构成完整路径。

**对应 spec §5.6 "TxnLog 形态"**:**完全自动**。Sink 传 chunk 给 `append_lake_txn_log_with_chunk_rowset`,内部按 `__op` 拆分填 `rowset.segments` / `dels[]`。

**G1 gate(`.del` 字节兼容 StarRocks)风险大幅降低**:
- `delete_payload_codec.rs` 有 5 个 round-trip 单测覆盖 SLICE_ESCAPE 规则
- 复用 NovaRocks 现有 `pk_applier::encode_primary_key_cell` 已经在 PK INSERT 路径上跑过
- **实施时验证**:跑一次"NovaRocks 写 `.del` → StarRocks BE 读取"的字节级回归,确认现有实现真的字节兼容(理论上是 — 它就是为了 stream load + PK INSERT 的 conflict 处理写的)

**B.1-B.4 子 PR 拆分调整**:
- B.1:**取消**(`.del` + PK encoder 已存在,作为 G1 验证一次性检查就够)
- B.2:`load_op_column.rs`(name helper + LoadOp enum)+ 防御性单测
- B.3:**取消**(sink `__op` 拆分已存在);改为"**验证现有 sink 路径在 DELETE 入口下行为正确**"的集成测试
- B.4:`delete_flow::execute_managed_pk_delete` + 删 CoW + 5 个 sql-test 用例 + 全 suite 回归

**实质上 M4 缩短为 B.2 + B.4 两个子 PR**。

### 10.4 §8 时间估计已更新

见 §8.5(已就地更新)。9.5-13.5 工程天(对比原 15-19)。

### 10.5 风险评估调整

| 原风险 ID | 状态 |
|---|---|
| R1 `.del` 字节兼容 | 降级(基础设施已通过单测验证,只需 StarRocks BE 互操作回归)|
| R3 Sink 改造引入 INSERT 回归 | **大幅降级** — 不再改 sink,只在 sink 上加 DELETE 入口 |
| R4 `DeletePredicatePb` 空 rowset publish 未走过 | 降级 — 已经走过(`transactions::delete_data` + `applier.rs:101`)|
| R2 `__op` 列污染优化器 | **降级** — 当前 NovaRocks plan optimizer **完全没碰过** `__op`,只需要写防御性单测锁定现有行为 |
| R5 / R6 / R7 / R8 / R9 / R10 | 无变化 |

### 10.6 实施时仍要做的事

虽然底层就绪,以下事项不可跳过:

1. **standalone 模式 `delete_data` 调用入口的形态**:
   `transactions::delete_data(&DeleteDataRequest)` 设计给 FFI / RPC 使用,接受 proto 类型;standalone 模式可以直接调,但要核对 `tablet_ids`、`txn_id` 分配、`schema_key` 来源 — 这些通常 FE 提供。实施时要选:
   - (a) 直接复用 `delete_data`,在 `delete_flow` 里构造 `DeleteDataRequest`,自分配 `txn_id`
   - (b) 抽出一个 `transactions::delete_data_with_predicate(tablet_ids, predicate, schema_key)` 内部入口,FFI 和 standalone 各调一个适配层
2. **PK DELETE 复用 standalone 现有 INSERT pipeline 的可行性**:[src/engine/insert_flow.rs:183 execute_insert_from_query_on_pipeline](src/engine/insert_flow.rs#L183) 是否能接受 `SELECT pk_cols, 1 AS __op` 的 plan、是否能感知"目标表是 PK 表"并把 chunk 走到 sink。实施时实测,如果不能,补一个 `execute_pk_delete_on_pipeline` 适配层。
3. **literal coercion (§3) 在 SELECT/DELETE 共用 analyzer 入口处的接入**:仍是新工作,无变化。
4. **splitter (§2)**:无变化。
5. **全 suite 回归**:无变化,所有 hard gate 仍存在。

### 10.7 总代码量更正

| 项 | 原估 | 更正后 |
|---|---|---|
| D | ~60 | ~60 |
| C | ~450 | ~450 |
| A | ~1450 + 7 用例 | ~1080 + 7 用例 |
| B | ~1730 + 5 用例 | ~890 + 5 用例 |
| **合计** | **~3690 + 12 用例** | **~2480 + 12 用例** |
