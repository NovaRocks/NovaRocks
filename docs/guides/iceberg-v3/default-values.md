# Default Value（V3）

> Iceberg v3 spec 让每一列声明两套默认值：`initial-default` 给老 data file 读取时回填，`write-default` 给新写入回填。NovaRocks 已实现 DDL 解析、读端 backfill、INSERT 显式列表写端 fill。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| CREATE TABLE 列上 `DEFAULT <literal>` | ✅ | |
| ALTER TABLE ADD COLUMN `DEFAULT <literal>` | ✅ | |
| `DEFAULT NULL` 显式 | ✅ | |
| V2 表硬拒非 NULL DEFAULT | ✅ | format-version 必须为 3 |
| 读端按 `initial-default` backfill 老文件 | ✅ | |
| INSERT 显式列表（`INSERT INTO t (a, b) ...`）按 `write-default` 自动 fill 缺省列 | ✅ | |
| INSERT 全列表（`INSERT INTO t VALUES ...`）写端 fill | 🚧 | 仅显式列表形式自动 fill；全列表需要传齐 |
| `DEFAULT <expression>`（非字面量） | ❌ | 仅字面量 |
| `DEFAULT CURRENT_TIMESTAMP` / 函数调用 | ❌ | |

实现入口：`src/sql/parser/ast/mod.rs`（`TableColumnDef.default`）+ DefaultLiteral AST + `default_literal_to_iceberg` 类型校验 + IcebergSink 的 fill 逻辑。

---

## ✅ CREATE TABLE 列默认值

```sql
CREATE TABLE orders (
  id        BIGINT,
  user_id   BIGINT,
  amount    DECIMAL(18, 2) DEFAULT 0.00,
  status    STRING         DEFAULT 'pending',
  created   TIMESTAMP      DEFAULT '2026-01-01 00:00:00',
  is_active BOOLEAN        DEFAULT true,
  raw_blob  BINARY         DEFAULT X'cafebabe'
)
TBLPROPERTIES ("format-version" = "3");
```

支持的字面量：

- bool（`TRUE` / `FALSE`）
- 整数（含 `TINYINT` / `SMALLINT` / `INT` / `BIGINT`，按列类型校验范围）
- 浮点（`FLOAT` / `DOUBLE`）
- decimal（按列 `(P, S)` 校验 scale）
- 字符串（`STRING`，含 date / datetime parsing：`'2026-01-01'` 给 DATE，`'2026-01-01 00:00:00'` 给 TIMESTAMP）
- hex binary（`X'cafebabe'` 给 `BINARY`/`FIXED`）
- 负数字面量

类型不匹配（如 `TINYINT DEFAULT 999`）在 parse 阶段直接 reject。

## ✅ ALTER TABLE ADD COLUMN 列默认值

```sql
ALTER TABLE orders ADD COLUMN region STRING DEFAULT 'CN';
ALTER TABLE orders ADD COLUMN refund_amount DECIMAL(18, 2) DEFAULT 0.00;
ALTER TABLE orders ADD COLUMN note STRING DEFAULT NULL;     -- 等价于不写 DEFAULT
```

写入 `initial-default`（用于老文件读取回填）+ `write-default`（用于新写入回填）。

## ✅ V2 表硬拒非 NULL DEFAULT

```sql
-- v2 表：DEFAULT NULL 允许，非 NULL DEFAULT 报错
CREATE TABLE t_v2 (id BIGINT, c STRING DEFAULT 'x')
TBLPROPERTIES ("format-version" = "2");
-- ERROR: non-NULL DEFAULT requires format-version >= 3
```

## ✅ 读端 backfill

老 data file 不含某列（在该 file 写出之后才 ADD COLUMN），读端按 `initial-default` 回填：

```sql
-- 老文件 → 读出来的 region 都是 'CN'
SELECT id, region FROM orders WHERE id < <某个老 snapshot 行的 id>;
```

## ✅ INSERT 显式列表写端 fill

```sql
-- amount / status / created / is_active 都按 write-default 自动 fill
INSERT INTO orders (id, user_id) VALUES (1, 1001);
```

## 🚧 INSERT 全列表写端 fill

```sql
-- 全列表形式：必须传齐所有列，否则报"column count mismatch"
INSERT INTO orders VALUES (1, 1001);   -- 不会自动 fill 默认值
```

**TODO**：让 INSERT 全列表形式也支持"行内填 NULL，let write-default 接管"的语义。当前必须用显式列表形式才能享受 default fill。

## ❌ DEFAULT 表达式（非字面量）

```sql
-- 暂未实现
ALTER TABLE orders ADD COLUMN created TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
ALTER TABLE orders ADD COLUMN id_str STRING DEFAULT CAST(id AS STRING);
```

Spec 在 v3 中定义了 `DEFAULT <expr>` 形式（含函数调用、引用其他列），NovaRocks 仅支持字面量。

**TODO**：未实现。如果要"插入时间戳"，目前需要在 SQL 写入端显式传 `CURRENT_TIMESTAMP()`。
