# Variant 类型（V3）

> Iceberg v3 引入 `variant` 类型，物理上由 metadata + value 两个 binary stream 组成（参考 Parquet variant proposal），逻辑上承载 schema-less 半结构化数据。NovaRocks 实现了读路径 + 表达式函数族，并在 PR #87 落地了 **INSERT happy path**（可以把 variant 列写出去）；其他 DML（OVERWRITE / DELETE / UPDATE / MERGE / equality-delete）以及 variant 在 partition / sort order 中的使用仍 fail-fast 拒绝。

| 能力 | 状态 | 备注 |
| --- | --- | --- |
| Variant 列读 | ✅ | `src/exec/variant.rs` |
| Variant 表达式函数（`variant_get` / `variant_extract` / `variant_typeof` / `get_json_string` 等） | ✅ | `src/exec/expr/function/variant/*` |
| `INSERT INTO ... VALUES (parse_json(...))` 写 variant 列 | ✅ | PR #87；单 partition spec、无 shredding |
| `INSERT INTO ... SELECT` 写 variant 列 | ✅ | PR #87 |
| `INSERT OVERWRITE` / `DELETE` / `UPDATE` / `MERGE INTO` / `ADD EQUALITY DELETE` 在 variant-bearing 表 | ❌ | PR #87 fail-fast，给出可执行错误信息 |
| Variant *shredding*（`typed_value` 子树） | ❌ | spec optional，本次未做 |
| Variant default value（`initial-default` / `write-default`） | ❌ | |
| Variant 在 partition spec / sort order / equality_ids | ❌ | spec 禁止；NovaRocks reject |
| Variant predicate pushdown 到 parquet | ❌ | |

---

## ✅ Variant 读路径

```sql
-- 假设 t.payload 是一个 variant 列（由 NovaRocks INSERT 写入或 cross-engine 写入）
SELECT id, variant_get(payload, '$.user.id') AS uid
  FROM t
 WHERE id < 1000;
```

支持：

- 直接 SELECT variant 列（按 binary 返回）
- 用 variant 表达式函数提取字段（`$.path.to.field`）
- variant 字段在投影裁剪 / 列裁剪 / runtime filter 中正常参与

## ✅ Variant INSERT happy path（PR #87）

```sql
CREATE TABLE t (id INT, v VARIANT) USING iceberg
  TBLPROPERTIES ("format-version" = "3");

INSERT INTO t VALUES
  (1, parse_json('{"a":1,"b":"x"}')),
  (2, parse_json('[10, 20, 30]')),
  (3, parse_json('null')),
  (4, NULL);

SELECT id, variant_typeof(v) FROM t ORDER BY id;
-- 1 Object / 2 Array / 3 Null / 4 NULL

SELECT id, get_json_string(v, '$.b') FROM t WHERE id = 1;  -- "x"
```

写路径细节：

- 文件落到 object storage 时是 spec-compliant parquet：variant group 标 `LogicalType::Variant`，下挂两个 required binary leaf（`metadata`、`value`）
- 单 partition spec、无 shredding
- 也支持 `INSERT INTO t SELECT ...` 形式（来源是另一张 variant-bearing 表）

## ❌ 其他 DML（fail-fast 拒绝）

下列 DML 在 variant 列上一律 reject，错误信息明确指向 PR #87 的非目标边界，方便用户判断为什么命令被拒：

```sql
INSERT OVERWRITE t SELECT ...;            -- ERROR: variant column not supported in INSERT OVERWRITE
DELETE FROM t WHERE id = 1;               -- ERROR: variant column not supported in DELETE
UPDATE t SET v = parse_json('{}') WHERE id = 1;  -- ERROR: variant column not supported in UPDATE
MERGE INTO t USING ... ;                  -- ERROR: variant column not supported in MERGE
ALTER TABLE t ADD EQUALITY DELETE ...;    -- ERROR: variant column not supported in equality-delete
```

替代方案：DELETE / UPDATE / MERGE 可以先把 variant 列从表 schema 中移除（DROP COLUMN）后再做，或换 cross-engine（Spark / Trino）改写后再让 NovaRocks 读。

## ❌ Variant in partition spec / sort order / equality_ids

Spec 禁止；NovaRocks 在 CREATE TABLE / ALTER PARTITION 阶段 reject，错误信息明确说明。

## ❌ Variant shredding（typed_value）

Spec optional 能力：在 parquet 物理结构里把"已知 schema 部分"提到 `typed_value` 子树以加速点查 / pushdown。NovaRocks 当前不写 shredded variant，读端遇到 cross-engine 写出的 shredded variant 退化按 metadata + value 解码。

## ❌ Variant default value

Spec：variant 列允许 declare DEFAULT。NovaRocks 当前 CREATE TABLE / ALTER ADD COLUMN 在 variant 列上拒绝 DEFAULT。

## ❌ Variant predicate pushdown

Spec：variant 字段 path 提取（`payload:user.id > 1000`）应该可以下推到 parquet 层做剪枝（依赖 parquet variant page index）。NovaRocks 当前在 BE 层 evaluate variant predicate，扫描成本与全表扫描相当。

---

## 路线图

按 [完成度清单](reference/support-matrix.md#43-v3-新类型) 的优先级，剩余 variant 工作（OVERWRITE / DELETE / UPDATE / MERGE 写、shredding、predicate pushdown、partition / sort 中使用）排在 P3（"v3 类型 tail"），优先级低于 catalog 生态、cross-engine 互通、MV 自动改写。
