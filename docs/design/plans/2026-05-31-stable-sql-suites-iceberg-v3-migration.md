# 稳定 SQL 套件迁移至 Iceberg v3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 10 个通用 CI 稳定套件（filter / limit / project / sort / join / cte / set-op / table-function / runtime-filter / low-cardinality）的表存储从 StarRocks 托管表改为 Iceberg v3，并各自 `verify -j 1` 跑绿。

**Architecture:** 复用仓库现有"套件跑 Iceberg v3"显式模式（`sql-tests/iceberg-ddl`）：每套件加 `init.sql`（建外部 iceberg catalog + `@catalog` 指令）与 `cleanup.sql`（drop catalog），逐条把 `CREATE TABLE` 改写为纯列定义 + `TBLPROPERTIES("format-version"="3")`。零引擎/runner 生产代码改动；用一个一次性 dev 辅助脚本机械化建表改写，再以 record → diff-review → verify 闭环兜底。

**Tech Stack:** Rust sql-test-runner（`tests/sql-test-runner`）、Docker Iceberg REST + MinIO 本机环境（`docker/iceberg-rest/`）、Python3（迁移辅助脚本）。

---

## 迁移配方（每个套件任务共用，务必先读）

### A. 环境与命令约定

所有命令在仓库根目录执行。先按 CLAUDE.md 发现并启动本机环境（见 Task 0），之后
每个套件任务都用下面这组命令（把 `SUITE` 替换为具体套件名，如 `sort`）：

```bash
# 已在 Task 0 source 过 env.sh；如新开 shell 需重新 source：
source docker/iceberg-rest/runtime/current/env.sh

# 重录该套件 golden
env NO_PROXY=127.0.0.1,localhost \
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite SUITE --mode record -j 1

# 验证该套件（完成门）
env NO_PROXY=127.0.0.1,localhost \
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite SUITE --mode verify -j 1

# 只跑指定用例（排查/特例处理时用）
env NO_PROXY=127.0.0.1,localhost \
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite SUITE --only case_a,case_b --mode verify -j 1
```

### B. `init.sql` / `cleanup.sql` 模板

每套件的 catalog 名 = `<suite_us>_cat_${suite_uuid0}`，其中 `<suite_us>` 把套件名
的 `-` 换成 `_`（如 `set-op` → `set_op_cat`，`table-function` → `table_function_cat`）。

`init.sql`（把 `CATALOG_NAME` 换成本套件的 catalog 名）：

```sql
-- @catalog=CATALOG_NAME
CREATE EXTERNAL CATALOG IF NOT EXISTS `CATALOG_NAME`
PROPERTIES (
    "type"="iceberg",
    "iceberg.catalog.type"="${iceberg_catalog_type}",
    "iceberg.catalog.warehouse"="${iceberg_catalog_warehouse}",
    "aws.s3.access_key"="${oss_ak}",
    "aws.s3.secret_key"="${oss_sk}",
    "aws.s3.endpoint"="${oss_endpoint}",
    "aws.s3.enable_path_style_access"="true"
);
```

`cleanup.sql`：

```sql
DROP CATALOG IF EXISTS `CATALOG_NAME`;
```

### C. `CREATE TABLE` 改写规则

对每个 `CREATE TABLE name (col_list) <tail>;`（**跳过** `CREATE TABLE ... AS SELECT`，
它无 `<tail>` 存储子句，原样保留）：

- **从 `<tail>` 删除**这些 StarRocks 原生子句（大小写不敏感）：
  - `DUPLICATE KEY(...)` / `AGGREGATE KEY(...)` / `UNIQUE KEY(...)` / 表级 `PRIMARY KEY(...)`
  - `DISTRIBUTED BY HASH(...) [BUCKETS n]` / `DISTRIBUTED BY RANDOM [BUCKETS n]`
  - `ORDER BY(...)`（存储排序子句，注意别误删 SELECT 里的 ORDER BY——只处理建表语句的 `<tail>`）
  - `PROPERTIES (...)`（如 `replication_num`）
  - 显式 `ENGINE = ...`
- **追加** `TBLPROPERTIES ("format-version" = "3")`。若该建表已带 `TBLPROPERTIES(...)`，
  把 `"format-version" = "3"` 合并进去（去重保留已有其它键）。
- **保留**：列名、列类型、`NOT NULL`、列级 `DEFAULT`。

### D. 类型映射参考（建表不报错，重点关注语义变化的两类）

`src/connector/iceberg/catalog/registry.rs::iceberg_type_for_sql_type`：
TINYINT/SMALLINT/INT→Int，BIGINT→Long，**LARGEINT→Decimal(38,0)**，
FLOAT/DOUBLE/DECIMAL 对应，**JSON→String**，STRING/VARCHAR/CHAR→String，
DATETIME→Timestamp，DATE→Date，BOOLEAN→Boolean，ARRAY/MAP/STRUCT→嵌套。

### E. 每套件迁移闭环（每个 Task 2–11 都按此执行）

1. 写 `init.sql` + `cleanup.sql`（模板 B）。
2. 用 Task 1 的脚本批量改写该套件全部 `CREATE TABLE`（规则 C），或对小套件直接 Edit。
3. `git diff sql-tests/SUITE/sql/` 逐文件 review 改写结果，修正脚本未覆盖的边角。
4. 先 `--mode verify`（不 record）跑一遍，观察哪些用例因存储/类型变化而 diff。
5. 判定每处 diff：能用"存储/类型映射"解释 → 正常；疑似回归 → 用 `systematic-debugging` 排查。
6. `--mode record` 重录，`git diff sql-tests/SUITE/result/` 复核新 golden 合理。
7. `--mode verify -j 1` 跑绿。
8. 遇到无法在 iceberg v3 保意图的用例（见各任务"特例"）：**停下并上报用户**，不静默改语义/删断言。
9. `git add sql-tests/SUITE/ && git commit`。

---

## Task 0: 启动本机 Iceberg 测试环境 + 建立基线

**Files:** 无（仅环境与基线验证）

- [ ] **Step 1: 发现并准备 worktree 环境**

```bash
docker/iceberg-rest/up.sh --prepare-only
source docker/iceberg-rest/runtime/current/env.sh
echo "MYSQL=$NOVA_ENV_MYSQL_PORT REST=$NOVAROCKS_ICEBERG_REST_URI"
echo "SQL_TEST_CONFIG=$NOVAROCKS_SQL_TEST_CONFIG"
echo "STANDALONE_CONFIG=$NOVAROCKS_STANDALONE_CONFIG"
```

Expected: 三个变量均非空。

- [ ] **Step 2: 拉起共享 Docker 服务**

Run: `docker/iceberg-rest/up.sh && docker/iceberg-rest/status.sh`
Expected: MinIO / Iceberg REST 服务可用（status 全绿）。

- [ ] **Step 3: 构建并后台启动 standalone-server（就绪门）**

```bash
cargo build
LOG=/tmp/novarocks-server.log
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  grep -q '^NOVAROCKS_READY ' "$LOG" && break
  kill -0 "$SRV_PID" 2>/dev/null || { echo "server died"; tail -20 "$LOG"; exit 1; }
  sleep 1
done
grep -q '^NOVAROCKS_READY ' "$LOG" || { echo "timeout"; kill -9 "$SRV_PID"; exit 1; }
echo "server pid=$SRV_PID"
```

Expected: 日志出现 `NOVAROCKS_READY mysql_port=... pid=...`。**该 server 在后续所有任务中保持运行。**

- [ ] **Step 4: 基线 sanity——确认改造前 `sort` 套件在原生存储下通过**

Run:
```bash
env NO_PROXY=127.0.0.1,localhost \
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite sort --mode verify -j 1
```
Expected: PASS（确认环境/runner 工作正常，作为迁移前参照）。若失败，先解决环境问题再继续。

- [ ] **Step 5: 确认 iceberg-ddl 模式可运行（参照样板）**

Run:
```bash
env NO_PROXY=127.0.0.1,localhost \
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-ddl --only default_numeric --mode verify -j 1
```
Expected: PASS（确认 `@catalog` + `${case_db}` + `format-version=3` 这条路在本环境通）。

---

## Task 1: 迁移辅助脚本

**Files:**
- Create: `tools/dev/migrate_suite_iceberg_v3.py`

- [ ] **Step 1: 写脚本**

```python
#!/usr/bin/env python3
"""One-off migration helper: rewrite StarRocks `CREATE TABLE` statements in a
sql-tests suite into Iceberg-v3 form.

For each `CREATE TABLE name (col_list) <tail>;` it strips native storage
clauses from <tail> and appends/merges TBLPROPERTIES("format-version"="3").
`CREATE TABLE ... AS SELECT` is left untouched. Every change is printed for
human diff review; the record/verify gate is the real safety net.

Usage:  python3 tools/dev/migrate_suite_iceberg_v3.py sql-tests/<suite>/sql
"""
import re
import sys
from pathlib import Path

# Native clauses to remove from the options tail (case-insensitive).
NATIVE_CLAUSES = [
    r"\bDUPLICATE\s+KEY\s*\([^)]*\)",
    r"\bAGGREGATE\s+KEY\s*\([^)]*\)",
    r"\bUNIQUE\s+KEY\s*\([^)]*\)",
    r"\bPRIMARY\s+KEY\s*\([^)]*\)",
    r"\bDISTRIBUTED\s+BY\s+HASH\s*\([^)]*\)(\s+BUCKETS\s+\d+)?",
    r"\bDISTRIBUTED\s+BY\s+RANDOM(\s+BUCKETS\s+\d+)?",
    r"\bORDER\s+BY\s*\([^)]*\)",
    r"\bPROPERTIES\s*\([^)]*\)",
    r"\bENGINE\s*=\s*\w+",
]
FV = '"format-version" = "3"'


def find_col_list_end(s, open_idx):
    """Return index just past the matching ')' for the '(' at open_idx."""
    depth = 0
    for i in range(open_idx, len(s)):
        if s[i] == "(":
            depth += 1
        elif s[i] == ")":
            depth -= 1
            if depth == 0:
                return i + 1
    return -1


def rewrite_statement(stmt):
    """Rewrite one CREATE TABLE statement (without trailing ';'). Returns
    (new_stmt, changed: bool)."""
    m = re.search(r"create\s+table\s+(if\s+not\s+exists\s+)?", stmt, re.I)
    if not m:
        return stmt, False
    # Find first '(' after the table name; if 'AS SELECT' comes first -> CTAS.
    paren = stmt.find("(", m.end())
    as_sel = re.search(r"\bAS\b", stmt[m.end():], re.I)
    if paren == -1 or (as_sel and m.end() + as_sel.start() < paren):
        return stmt, False  # CTAS or no column list
    end = find_col_list_end(stmt, paren)
    if end == -1:
        return stmt, False
    head = stmt[:end]            # CREATE TABLE name (cols)
    tail = stmt[end:]            # storage clauses

    existing_tblprops = re.search(r"\bTBLPROPERTIES\s*\(([^)]*)\)", tail, re.I)
    for pat in NATIVE_CLAUSES:
        tail = re.sub(pat, "", tail, flags=re.I)
    tail = re.sub(r"\bTBLPROPERTIES\s*\([^)]*\)", "", tail, flags=re.I)
    tail = tail.strip()

    if existing_tblprops:
        inner = existing_tblprops.group(1).strip()
        if re.search(r'format-version', inner, re.I):
            merged = re.sub(r'"format-version"\s*=\s*"\d+"', FV, inner, flags=re.I)
        else:
            merged = (inner + ", " if inner else "") + FV
        props = f'TBLPROPERTIES ({merged})'
    else:
        props = f'TBLPROPERTIES ({FV})'

    new_stmt = f"{head}\n{props}"
    return new_stmt, True


def process_file(path):
    text = path.read_text()
    out = []
    changed = False
    # Split on ';' but keep statements; naive split is fine for these suites.
    parts = re.split(r";", text)
    for idx, part in enumerate(parts):
        if re.search(r"create\s+table", part, re.I):
            new, ch = rewrite_statement(part)
            if ch:
                changed = True
                print(f"  [{path.name}] rewrote a CREATE TABLE")
            out.append(new)
        else:
            out.append(part)
    if changed:
        path.write_text(";".join(out))
    return changed


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(2)
    sql_dir = Path(sys.argv[1])
    n = 0
    for f in sorted(sql_dir.glob("*.sql")):
        if process_file(f):
            n += 1
    print(f"rewrote CREATE TABLE in {n} file(s) under {sql_dir}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: 自测脚本（不依赖套件，验证核心改写逻辑）**

```bash
python3 - <<'PY'
import importlib.util, sys
spec = importlib.util.spec_from_file_location("m", "tools/dev/migrate_suite_iceberg_v3.py")
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)

s1 = "CREATE TABLE t (k INT, s STRING, v INT) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num' = '1')"
out1, ch1 = m.rewrite_statement(s1)
assert ch1 and 'format-version' in out1 and 'DUPLICATE' not in out1 and 'DISTRIBUTED' not in out1 and 'replication_num' not in out1, out1

s2 = "CREATE TABLE t (id INT) TBLPROPERTIES (\"a\"=\"b\")"
out2, ch2 = m.rewrite_statement(s2)
assert ch2 and 'format-version' in out2 and '"a"="b"' in out2.replace(' ',''), out2

s3 = "CREATE TABLE t AS SELECT * FROM x"
out3, ch3 = m.rewrite_statement(s3)
assert not ch3, "CTAS must be untouched"

s4 = "CREATE TABLE t (a INT)"
out4, ch4 = m.rewrite_statement(s4)
assert ch4 and 'format-version' in out4, out4
print("helper self-test OK")
PY
```

Expected: 打印 `helper self-test OK`。

- [ ] **Step 3: Commit**

```bash
git add tools/dev/migrate_suite_iceberg_v3.py
git commit -m "tools: add one-off helper to migrate sql-test suites to Iceberg v3"
```

---

## Task 2: 迁移 `sort` 套件（干净·0 原生 DDL）

**Files:**
- Create: `sql-tests/sort/init.sql`, `sql-tests/sort/cleanup.sql`
- Modify: `sql-tests/sort/sql/*.sql`（13 文件，约 10 条 CREATE TABLE）
- Modify: `sql-tests/sort/result/*.result`（record 后）

**特例:** 无（0 原生 DDL，纯计算）。

- [ ] **Step 1: 建 init.sql / cleanup.sql**（模板 B，catalog 名 `sort_cat_${suite_uuid0}`）
- [ ] **Step 2: 改写建表** — `python3 tools/dev/migrate_suite_iceberg_v3.py sql-tests/sort/sql`
- [ ] **Step 3: review 改写** — `git diff sql-tests/sort/sql/`，确认每条 CREATE TABLE 仅去原生子句 + 加 v3
- [ ] **Step 4: 先 verify 观察 diff**（命令见配方 A，SUITE=sort，`--mode verify`）
- [ ] **Step 5: record 重录** — 配方 A，`--mode record`；`git diff sql-tests/sort/result/` 复核
- [ ] **Step 6: verify -j1 跑绿** — 配方 A，`--mode verify -j 1`，Expected: PASS
- [ ] **Step 7: Commit** — `git add sql-tests/sort/ && git commit -m "test(sort): migrate suite to Iceberg v3 storage"`

---

## Task 3: 迁移 `set-op` 套件（干净·0 原生 DDL）

**Files:**
- Create: `sql-tests/set-op/init.sql`, `sql-tests/set-op/cleanup.sql`（catalog 名 `set_op_cat_${suite_uuid0}`）
- Modify: `sql-tests/set-op/sql/*.sql`（18 文件，约 2 条 CREATE TABLE）
- Modify: `sql-tests/set-op/result/*.result`

**特例:** 无。

- [ ] **Step 1–7:** 按配方 E 执行（SUITE=set-op，catalog=`set_op_cat_${suite_uuid0}`）。提交信息 `test(set-op): migrate suite to Iceberg v3 storage`。

---

## Task 4: 迁移 `table-function` 套件（干净·0 CREATE TABLE）

**Files:**
- Create: `sql-tests/table-function/init.sql`, `sql-tests/table-function/cleanup.sql`（catalog 名 `table_function_cat_${suite_uuid0}`）
- Modify: `sql-tests/table-function/result/*.result`（如有变化）

**特例:** 该套件 0 条 CREATE TABLE（多用 `generate_series` 等 TVF）。仍加 init/cleanup 以保持
catalog 一致；多数用例不建表，理论上结果不变。

- [ ] **Step 1: 建 init.sql / cleanup.sql**（catalog `table_function_cat_${suite_uuid0}`）
- [ ] **Step 2: verify 观察** — SUITE=table-function，`--mode verify`。若全过，跳到 Step 4。
- [ ] **Step 3: 如有 diff，record + 复核** — `--mode record`，`git diff sql-tests/table-function/result/`
- [ ] **Step 4: verify -j1 跑绿**
- [ ] **Step 5: Commit** — `test(table-function): add Iceberg v3 catalog init`

---

## Task 5: 迁移 `project` 套件（轻原生 DDL + JSON/largeint 特例）

**Files:**
- Create: `sql-tests/project/init.sql`, `sql-tests/project/cleanup.sql`（catalog `project_cat_${suite_uuid0}`）
- Modify: `sql-tests/project/sql/*.sql`（27 文件，约 6 条 CREATE TABLE）
- Modify: `sql-tests/project/result/*.result`

**特例（重点 review，按配方 E Step 8 处理无法保意图者）:**
- largeint：`project_abs_largeint_boundary.sql`、`project_md5sum_numeric_largeint_semantics.sql`、
  `project_cast_string_sign_to_int.sql` —— LARGEINT→Decimal(38,0)，注意 128 位边界/溢出/abs 是否变化。
- json：`project_cast_json_to_map.sql`、`project_cast_json_to_struct.sql` —— JSON→String，
  JSON→复杂类型 cast 语义是否仍成立。

- [ ] **Step 1: 建 init.sql / cleanup.sql**
- [ ] **Step 2: 改写建表** — `python3 tools/dev/migrate_suite_iceberg_v3.py sql-tests/project/sql`
- [ ] **Step 3: review 改写** — `git diff sql-tests/project/sql/`
- [ ] **Step 4: 先 verify，重点看上述 5 个特例文件**
- [ ] **Step 5: 逐特例判定** — 能解释→record；无法保意图（如真·128 位边界、JSON 专属 cast 断言失效）→**停下上报用户**
- [ ] **Step 6: record + 复核 result diff**
- [ ] **Step 7: verify -j1 跑绿**
- [ ] **Step 8: Commit** — `test(project): migrate suite to Iceberg v3 storage`

---

## Task 6: 迁移 `filter` 套件（轻原生 DDL + JSON/PK 特例 + 2 处 explain 断言）

**Files:**
- Create: `sql-tests/filter/init.sql`, `sql-tests/filter/cleanup.sql`（catalog `filter_cat_${suite_uuid0}`）
- Modify: `sql-tests/filter/sql/*.sql`（15 文件，约 15 条 CREATE TABLE）
- Modify: `sql-tests/filter/result/*.result`

**特例:**
- `filter/sql/filter_basic_comparison.sql` 含 `-- @explain_contains=stats={rows=` 和
  `-- @explain_contains=SCAN`。迁移后 iceberg 扫描节点名/stats 可能变化：跑该用例的 EXPLAIN，
  确认计划文本仍含 `SCAN` 与 `stats={rows=`；若 iceberg 扫描节点不含子串 `SCAN`，把指令字符串
  改成实际节点名（**不放宽断言强度**，只对齐真实节点名）。
- `filter/sql/test_scan_predicate_expr_reuse.sql` 同时含 **JSON 列 + PRIMARY KEY**：
  脚本会去掉 PRIMARY KEY 子句、JSON→String。检查去 PK 后是否引入重复行影响结果；JSON 谓词是否仍成立。
  无法保意图→**停下上报**。

- [ ] **Step 1: 建 init.sql / cleanup.sql**
- [ ] **Step 2: 改写建表** — `python3 tools/dev/migrate_suite_iceberg_v3.py sql-tests/filter/sql`
- [ ] **Step 3: review 改写**（特别检查 `test_scan_predicate_expr_reuse.sql` 的 PK 去除是否合理）
- [ ] **Step 4: 先 verify，重点看 `filter_basic_comparison`（explain 断言）与 `test_scan_predicate_expr_reuse`**
- [ ] **Step 5: 对齐 explain 断言节点名（如需）；逐特例判定，无法保意图→停下上报**
- [ ] **Step 6: record + 复核 result diff**
- [ ] **Step 7: verify -j1 跑绿**
- [ ] **Step 8: Commit** — `test(filter): migrate suite to Iceberg v3 storage`

---

## Task 7: 迁移 `limit` 套件

**Files:**
- Create: `sql-tests/limit/init.sql`, `sql-tests/limit/cleanup.sql`（catalog `limit_cat_${suite_uuid0}`）
- Modify: `sql-tests/limit/sql/*.sql`（1 文件，1 条 CREATE TABLE）
- Modify: `sql-tests/limit/result/*.result`

**特例:** 无（1 条建表，含原生 DDL，纯改写）。

- [ ] **Step 1–7:** 按配方 E（SUITE=limit）。提交 `test(limit): migrate suite to Iceberg v3 storage`。

---

## Task 8: 迁移 `cte` 套件

**Files:**
- Create: `sql-tests/cte/init.sql`, `sql-tests/cte/cleanup.sql`（catalog `cte_cat_${suite_uuid0}`）
- Modify: `sql-tests/cte/sql/*.sql`（3 文件，约 4 条 CREATE TABLE）
- Modify: `sql-tests/cte/result/*.result`

**特例:** 无（含原生 DDL，纯改写）。

- [ ] **Step 1–7:** 按配方 E（SUITE=cte）。提交 `test(cte): migrate suite to Iceberg v3 storage`。

---

## Task 9: 迁移 `runtime-filter` 套件（重原生 DDL；验证 RF 作用于 iceberg 扫描）

**Files:**
- Create: `sql-tests/runtime-filter/init.sql`, `sql-tests/runtime-filter/cleanup.sql`（catalog `runtime_filter_cat_${suite_uuid0}`）
- Modify: `sql-tests/runtime-filter/sql/*.sql`（22 文件，约 51 条 CREATE TABLE）
- Modify: `sql-tests/runtime-filter/result/*.result`

**特例:** runtime filter 是计算特性，应作用于 iceberg 扫描。若某用例用 `@explain_contains`/EXPLAIN
断言 RF 下推到扫描节点，核对 iceberg 扫描节点上 RF 仍出现；不出现→**停下上报**（引擎缺口）。

- [ ] **Step 1: 建 init.sql / cleanup.sql**
- [ ] **Step 2: 改写建表** — `python3 tools/dev/migrate_suite_iceberg_v3.py sql-tests/runtime-filter/sql`
- [ ] **Step 3: review 改写** — `git diff sql-tests/runtime-filter/sql/`（51 条，逐文件扫一遍）
- [ ] **Step 4: 先 verify 观察 diff**
- [ ] **Step 5: 判定 diff（含 RF 计划断言）；无法保意图→停下上报**
- [ ] **Step 6: record + 复核 result diff**
- [ ] **Step 7: verify -j1 跑绿**
- [ ] **Step 8: Commit** — `test(runtime-filter): migrate suite to Iceberg v3 storage`

---

## Task 10: 迁移 `join` 套件（最大体量：60 文件 / ~155 建表 + largeint/json 特例）

**Files:**
- Create: `sql-tests/join/init.sql`, `sql-tests/join/cleanup.sql`（catalog `join_cat_${suite_uuid0}`）
- Modify: `sql-tests/join/sql/*.sql`（60 文件，约 155 条 CREATE TABLE，约 30 文件含原生 DDL）
- Modify: `sql-tests/join/result/*.result`

**特例（重点 review）:**
- largeint：`join_large_in_predicate.sql`、`join_skew.sql`、`join_skew_v2.sql`、`join_one_key.sql`
  —— LARGEINT→Decimal(38,0)，join key 为 largeint 时确认等值/分布语义不变。
- json/复杂类型：`join_struct_type.sql`、`join_map_type.sql` —— 确认 STRUCT/MAP/JSON 在 iceberg 上 join 成立。

- [ ] **Step 1: 建 init.sql / cleanup.sql**
- [ ] **Step 2: 改写建表** — `python3 tools/dev/migrate_suite_iceberg_v3.py sql-tests/join/sql`
- [ ] **Step 3: review 改写** — `git diff sql-tests/join/sql/`（体量大，分批 review；重点 30 个原生 DDL 文件 + 6 个特例文件）
- [ ] **Step 4: 先 verify 观察 diff（全套件）**
- [ ] **Step 5: 逐特例判定（largeint join key / struct-map join）；无法保意图→停下上报**
- [ ] **Step 6: record + 复核 result diff**
- [ ] **Step 7: verify -j1 跑绿**
- [ ] **Step 8: Commit** — `test(join): migrate suite to Iceberg v3 storage`

---

## Task 11: 迁移 `low-cardinality` 套件（最高风险：字典重写 + largeint + char + 原生专属意图）

**Files:**
- Create: `sql-tests/low-cardinality/init.sql`, `sql-tests/low-cardinality/cleanup.sql`（catalog `low_cardinality_cat_${suite_uuid0}`）
- Modify: `sql-tests/low-cardinality/sql/*.sql`（5 文件，约 7 条 CREATE TABLE）
- Modify: `sql-tests/low-cardinality/result/*.result`

**特例（逐个 review，风险最高）:**
- `rewrite.sql`：`@result_contains=DECODE` 断言字典重写出现 DECODE 节点。`ANALYZE FULL` 在 iceberg
  表上构建字典已被 `src/engine/dictionary/maintenance.rs` 支持，但优化器 `LowCardinalityDictionaryRewrite`
  是否在 iceberg 扫描上端到端触发需实跑确认。若 DECODE **不出现**→**停下上报**（引擎缺口，由用户决定
  是否补引擎支持 / 该用例留原生 / 调整断言），不静默删断言。
- `disabled.sql`：验证 `disable_optimizer_rules='LowCardinalityDictionaryRewrite'` 后不走字典；逻辑应与存储无关，确认通过。
- `compressed_key.sql` / `compressed_key2.sql`：含 **largeint + char(100) + PRIMARY KEY**（compressed_key）。
  LARGEINT→Decimal(38,0)、CHAR→String、去 PK。多为 `@skip_result_check=true` 的聚合冒烟；确认建表/插入/聚合在 iceberg 上成立。
- `stale.sql`：字典 stale 失效路径；确认 iceberg 表的 stale 标记/重建在 ANALYZE 后成立。

- [ ] **Step 1: 建 init.sql / cleanup.sql**
- [ ] **Step 2: 改写建表** — `python3 tools/dev/migrate_suite_iceberg_v3.py sql-tests/low-cardinality/sql`
- [ ] **Step 3: review 改写**（确认 largeint/char/PK 改写合理）
- [ ] **Step 4: 单独先跑 `rewrite`（最关键）** — `--only rewrite --mode verify`；确认计划仍含 DECODE
- [ ] **Step 5: 若 DECODE 缺失 → 停下上报用户**（这是设计中预判的最高风险点）
- [ ] **Step 6: 跑全套件 verify 观察 diff，逐特例判定**
- [ ] **Step 7: record + 复核 result diff**
- [ ] **Step 8: verify -j1 跑绿**
- [ ] **Step 9: Commit** — `test(low-cardinality): migrate suite to Iceberg v3 storage`

---

## Task 12: 全量复验 + 收尾

**Files:** 无（或更新 memory 索引）

- [ ] **Step 1: 逐个复验全部 10 套件 verify -j1**

```bash
source docker/iceberg-rest/runtime/current/env.sh
for s in sort set-op table-function project filter limit cte runtime-filter join low-cardinality; do
  echo "=== verify $s ==="
  env NO_PROXY=127.0.0.1,localhost \
    cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
    --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite "$s" --mode verify -j 1 || echo "FAILED: $s"
done
```

Expected: 10 套件全 PASS（被上报、用户决定保留原生/调整的用例除外，需在收尾说明里列出）。

- [ ] **Step 2: 确认未改动范围** — `git diff --stat origin/main -- tools/ci/suites/stable-sql-suites.txt`
  应为空（CI 清单不变）；`src/` 无生产代码改动（除非某特例上报后用户要求补引擎支持）。

- [ ] **Step 3: 用 `superpowers:requesting-code-review` 发起自审**（review 全部套件 diff）

- [ ] **Step 4: 收尾说明** — 汇总：10 套件迁移结果、被上报/特殊处置的用例清单、是否触及引擎缺口。

---

## Self-Review（计划对照 spec）

- **范围覆盖**：spec §范围的 10 套件 → Task 2–11 一一对应；6 iceberg 套件与 CI 清单不动 → Task 12 Step 2 校验。✓
- **配方**：spec §1 init/cleanup + CREATE TABLE 改写 → 配方 B/C + Task 1 脚本。✓
- **类型映射**：spec §2 → 配方 D + Task 5/10/11 的 largeint/json 特例。✓
- **特例（不静默漂移）**：spec §3 → 各任务"特例"块 + 配方 E Step 8「停下上报」。✓
  - PK：filter / low-cardinality(compressed_key) → Task 6 / Task 11 点名。✓
  - largeint：project(3) / join(4) / low-cardinality(2) → Task 5 / 10 / 11 点名。✓
  - json：filter(1) / project(2) / join(2) → Task 6 / 5 / 10 点名。✓
  - 字典重写 DECODE：Task 11 Step 4–5。✓
  - 2 处 explain 断言：Task 6 特例。✓
- **验证流程**：spec §4 record→diff-review→verify -j1 → 配方 E + 各任务 Step。✓
- **顺序**：spec §5 → Task 2–11 顺序一致。✓
- **占位符扫描**：init/cleanup 给出模板与每套件 catalog 名；命令给全；脚本完整含自测；无 TBD。✓
