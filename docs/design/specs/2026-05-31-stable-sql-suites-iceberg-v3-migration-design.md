# 稳定 SQL 套件迁移至 Iceberg v3 设计

## 背景

NovaRocks 当前以 **Iceberg v3** 作为主存储方向，StarRocks 原生类型表不再是
重点。但 CI 的稳定测试集里，大部分通用 SQL 套件仍建在 `default_catalog`
（standalone 模式下即 managed-lake / StarRocks 托管表）之上，没有验证
Iceberg v3 读写/计算路径。

CI 稳定套件清单固定在 `tools/ci/suites/stable-sql-suites.txt`，由
`tools/ci/local-full-ci.sh` 在默认 daily 模式下逐套件
`sql-tests --mode verify -j 1` 执行。清单共 16 个套件：

- **6 个 iceberg 专用套件**：`iceberg`、`iceberg-compatibility`、
  `iceberg-ddl`、`iceberg-dml`、`iceberg-ivm`、`iceberg-rest`。已经跑在
  Iceberg 上，且**故意混用 v1/v2/v3** 做格式覆盖（例如
  `iceberg-ddl` 同时有 `format-version=2` 和 `=3` 的用例）。
- **10 个通用套件**：`filter`、`limit`、`project`、`sort`、`join`、`cte`、
  `set-op`、`table-function`、`runtime-filter`、`low-cardinality`。目前用裸
  `CREATE TABLE`（无存储子句），建在 `default_catalog`，即 StarRocks 托管表。

仓库已有成熟的"套件跑 Iceberg v3"模式（见 `sql-tests/iceberg-ddl/init.sql`
与 `sql-tests/iceberg-dml/init.sql`）：

- `init.sql` 用 `-- @catalog=<name>` 指令激活一个外部 iceberg catalog，并
  `CREATE EXTERNAL CATALOG IF NOT EXISTS` 建它；`cleanup.sql` `DROP CATALOG`。
- 每条建表显式写 `TBLPROPERTIES ("format-version" = "3")`。
- 用例正文用 `${case_db}` 占位库名，由 runner 在该 catalog 下逐用例建库/清库。

Iceberg 建表时若不指定 `format-version`，默认 **V2**
（`src/connector/iceberg/catalog/registry.rs` 的
`extract_table_format_version_property`，默认 `FormatVersion::V2`），并且
`iceberg-ddl` 里有用例专门断言"未指定时默认 v2"。因此**不能修改全局默认值**。

## 目标

把上述 **10 个通用稳定套件** 的表存储结构从 StarRocks 托管表改为
**Iceberg v3**，使 CI 稳定集整体以 Iceberg v3 为表存储；保持套件名、用例
文件名、`@` 指令和测试意图不变；最终每个被改造套件
`sql-tests --mode verify -j 1` 跑绿。

### 非目标

- 不改 6 个 iceberg-* 套件（已在 iceberg 上，且故意保留 v1/v2 覆盖）。
- 不改 CI 清单 `tools/ci/suites/stable-sql-suites.txt`（套件名不变，只是
  其底层存储变了）。
- 不改引擎/runner 生产代码（采用"逐套件显式迁移"方案，零引擎改动）。
- 不引入 session 级默认 format-version 或 runner 自动 DDL 重写机制。

## 实现方案：逐套件显式迁移（方案 A）

复用仓库现有 `iceberg-ddl` / `iceberg-dml` 的显式模式。决策依据：与现状完全
一致、零引擎/runner 风险、每个 `.sql` 自描述存储；机械改动量大但可脚本化、
可逐套件独立验证。

### 1. 每套件迁移配方

**(a) 新增 `init.sql`**（照搬 `iceberg-ddl/init.sql` 模板，仅改 catalog 名）：

```sql
-- @catalog=<suite>_cat_${suite_uuid0}
CREATE EXTERNAL CATALOG IF NOT EXISTS `<suite>_cat_${suite_uuid0}`
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

**(b) 新增 `cleanup.sql`**：`DROP CATALOG IF EXISTS \`<suite>_cat_${suite_uuid0}\`;`

`${iceberg_catalog_type}`（hadoop）、`${iceberg_catalog_warehouse}`、
`${oss_*}` 均已在生成的 `$NOVAROCKS_SQL_TEST_CONFIG` 的 `[env]` 中提供，套件
无需额外配置即可在本机 MinIO 环境跑通。

**(c) 逐条改写 `CREATE TABLE`**：

- **剥离**这些 StarRocks 原生子句：
  - `DUPLICATE KEY(...)` / `AGGREGATE KEY(...)` / 表级 `PRIMARY KEY(...)` /
    `UNIQUE KEY(...)`
  - `DISTRIBUTED BY HASH(...) [BUCKETS n]` / `DISTRIBUTED BY RANDOM [BUCKETS n]`
  - `ORDER BY(...)` 存储排序子句
  - `PROPERTIES('replication_num' = ...)` 等表属性
  - 显式 `ENGINE=...`
- **追加** `TBLPROPERTIES ("format-version" = "3")`；若该建表已有
  `TBLPROPERTIES`，则把 `format-version=3` 合并进去。
- **保留**：列名、列类型（依赖引擎类型映射，见下）、`NOT NULL`、列 `DEFAULT`。

`${case_db}` 机制不变——`iceberg-ddl/dml` 已证明 runner 能在 `@catalog`
指定的 iceberg catalog 下逐用例建库/清库。

### 2. 类型映射（建表不会报错，但部分语义会变）

`src/connector/iceberg/catalog/registry.rs::iceberg_type_for_sql_type` 对所有
StarRocks 标量类型都有映射，**CREATE TABLE 不会因类型失败**：

| StarRocks 类型 | Iceberg 类型 | 语义影响 |
|---|---|---|
| TINYINT / SMALLINT / INT | Int (32 位) | 值等价 |
| BIGINT | Long | 值等价 |
| **LARGEINT** | **Decimal(38,0)** | **128 位范围被截到 decimal(38,0)；边界/溢出/abs 用例可能差异或报错** |
| FLOAT / DOUBLE / DECIMAL | Float / Double / Decimal | 值等价 |
| **JSON** | **String** | **JSON 函数语义可能变化** |
| STRING / VARCHAR / CHAR | String | 预期值等价（定长/变长丢失，但显示一致） |
| BOOLEAN | Boolean | 值等价 |
| DATE | Date | 值等价 |
| DATETIME | Timestamp（无 tz） | 预期格式一致，需验证 |
| TIME | Time | 值等价 |
| BINARY / BITMAP / HLL | Binary | 取决于用例 |
| VARIANT | Variant | v3 原生支持 |
| ARRAY / MAP / STRUCT | List / Map / Struct | 嵌套递归映射 |

### 3. 特例处理（不做静默语义漂移，遵守 CLAUDE.md 规则 1/2）

- **PK / AGGREGATE KEY 表**（通用套件里约 2 个 PRIMARY KEY + 1 个 AGGREGATE
  KEY）：iceberg 无去重/按键聚合语义。逐个检查：用例不依赖去重/聚合时纯列化
  即可；依赖时重录并确认意图存活，无法存活则**上报**。
- **`ANALYZE FULL TABLE`**：iceberg 表支持（`src/engine/dictionary/
  maintenance.rs` 明确支持 `DictionaryOwner::IcebergTable`，注释指出
  `ANALYZE FULL` 是 iceberg 表构建字典的路径）。保留不动。
- **low-cardinality 字典重写**（`rewrite.sql` 用 `@result_contains=DECODE`
  断言计划里出现 DECODE 节点）：字典引擎层支持已确认，但优化器
  `LowCardinalityDictionaryRewrite` 是否在 iceberg 扫描上端到端触发需**实跑
  验证**。该套件**放最后迁**；若 DECODE 不出现，作为引擎缺口**上报**，不静默
  跳过或删断言。
- **LargeInt → Decimal(38,0)**：约 9 个文件（`project_abs_largeint_boundary`、
  `project_md5sum_numeric_largeint_semantics`、`project_cast_string_sign_to_int`、
  `join_large_in_predicate`、`join_skew`/`join_skew_v2`、`join_one_key`、
  `low-cardinality/compressed_key`/`compressed_key2`）。逐个重录+review；专测
  真·128 位范围、意图无法在 decimal(38,0) 存活的用例**上报**，由用户决定
  （移出套件 / 该用例留原生 / 接受新语义）。
- **Json → String**：约 38 处 json 引用。JSON 函数用例逐个重录+review；
  JSON 专属意图破坏的用例**上报**。
- **2 处 `@explain_contains`**（`filter_basic_comparison.sql`：断言 `SCAN`、
  `stats={rows=`）：核对 iceberg 扫描节点名是否仍含该子串，必要时调整指令
  字符串；不放宽断言强度。

### 4. Golden 与验证流程（每套件独立闭环）

1. 改写该套件的 `init.sql`/`cleanup.sql` 与全部 `CREATE TABLE`。
2. `sql-tests --mode record` 重录该套件。
3. `git diff sql-tests/<suite>/result/` 审查变化：确认每处变化都能由"存储/类型
   映射"解释，**不是回归**；无法解释的变化按特例处理或上报。
4. `sql-tests --mode verify -j 1` 跑绿。

多数 `.result` 不会变（确定性 INSERT+SELECT 的结果与存储无关），变化集中在
largeint/json 用例与计划断言。

**环境**（严格按 CLAUDE.md）：
`source docker/iceberg-rest/runtime/current/env.sh` → `docker/iceberg-rest/up.sh`
→ 以 `NOVAROCKS_READY` marker 作为就绪门后台启动 standalone-server（用
`$NOVAROCKS_STANDALONE_CONFIG`）→ 用 `$NOVAROCKS_SQL_TEST_CONFIG` 跑
sql-tests。不硬编码端口。

### 5. 迁移顺序（逐步去风险，每套件是独立可验证增量）

1. **干净·存储无关**（先验证整套模式跑通）：`sort` → `set-op` →
   `table-function`（这三套件 0 原生 DDL）。
2. **轻原生 DDL**：`project` → `filter` → `limit` → `cte`。
3. **重原生 DDL**：`runtime-filter` → `join`（join 60 用例中约 30 含原生
   DDL、155 条建表）。
4. **最高风险**（字典重写 + largeint + 原生专属意图）：`low-cardinality`。

### 6. 风险与回报

- **风险**：少数 largeint / json / PK 用例在 iceberg 上无法保持原意图。处理
  原则——逐个浮现并给出建议，由用户拍板，**绝不静默改语义或删断言**。
- **回报**：10 个通用稳定套件从此实打实走 Iceberg v3 的读写/计算路径，CI 稳定
  集整体以 Iceberg v3 为表存储，符合 NovaRocks 主存储方向。

## 验收标准

- 10 个通用套件各自新增 `init.sql`/`cleanup.sql`，全部 `CREATE TABLE` 改写为
  iceberg v3（纯列定义 + `format-version=3`，无原生存储子句）。
- 每个被改造套件 `sql-tests --config "$NOVAROCKS_SQL_TEST_CONFIG"
  --suite <suite> --mode verify -j 1` 跑绿。
- 所有 golden 变化都有"存储/类型映射"层面的解释，无未解释回归。
- 任何无法在 iceberg v3 上保持意图的用例都已显式上报并有处置结论，未被静默
  跳过或弱化。
- 6 个 iceberg-* 套件与 `stable-sql-suites.txt` 保持不变。

## 实施结果与偏差（2026-05-31 完成）

### 最终结果
全部 16 个稳定套件 `verify -j 1` 全绿：

- **9 个通用套件已迁移至 Iceberg v3**：filter(15)、limit(1)、project(27)、sort(13)、
  join(60)、cte(3)、set-op(18)、table-function(6)、runtime-filter(22)。
- **low-cardinality(5) 保留在 StarRocks 原生存储**（见下"偏差一"）。
- 6 个 iceberg-* 套件未改动，复验无回归（iceberg 24、iceberg-ddl 47、iceberg-dml 37、
  iceberg-ivm 62、iceberg-rest 9、iceberg-compatibility 12）。
- `tools/ci/suites/stable-sql-suites.txt` 未改动；`cargo test --lib` 仅 5 个**预存**失败
  （`mv_shape`/`pipeline::builder`，源自 main 的 commit `45f6e676`，与本次无关），本次新增 0 失败。

### 偏差一：low-cardinality 保留原生存储（用户决定 Option B）
迁移中发现低基数字典重写在 iceberg 扫描上会分配 `dict_columns`，但 iceberg/HDFS 扫描
**执行层没有字典 encode 管道**（`HdfsScanConfig` 无 dict 支持；守卫在
`src/sql/codegen/fragment_builder.rs`）。让其在 iceberg 上"既触发又能执行 DECODE"需
实现 iceberg 扫描字典执行（Option A，中大规模、风险中高）。用户选择 Option B：在
`src/engine/dictionary/mod.rs::owner_for` 对 `IcebergDataFiles` 返回 `None`，把字典重写
排除出 iceberg；low-cardinality 套件保留原生 DDL 不迁移（字典重写本质是原生存储优化，
原生是其自然测试宿主）。Option A 留作未来专项；机制可逆。

### 偏差二：原 spec 范围外的引擎修复（用户授权"全修"）
迁移暴露出多个 iceberg 写入/解析路径与原生路径的能力差距，用户授权修复（原 spec 把 src/
改动列为非目标，因特例上报后纳入）。共 7 个引擎 commit：
- **标量 INSERT 隐式转换族**（3 commit）：`reannotate_array` 加 Null→任意、numeric↔numeric/
  decimal/float、scalar/bool/temporal→Utf8，复用 `cast_with_special_rules`（与原生写入路径
  同款 cast，是对齐非分叉），嵌套类型保留 fail-fast。
- **#1 ANALYZE 解析 iceberg 外部表**：新增 `register_external_table_by_name`，ANALYZE 前物化
  iceberg 表。
- **#2 嵌套字面量转换**：`build_literal_array` 对 ARRAY 元素 / MAP 键字面量按声明类型转换。
- **#3 MAP NULL 键**：panic→优雅报错（iceberg 规范禁止 null map 键）。
- **#4 JOIN ON 子查询表名解析**：`query_refs` 遍历 `join.join_operator` 的 ON 约束。
- **#5 字典重写 gate**：见偏差一。

### 偏差三：测试数据调整（用户授权）
- `join_map_type`/`join_struct_type`：含 iceberg 无法存储的 **null map 键**行（iceberg 规范禁止）。
  按用户决定移除 null-键数据行/条目并重录 golden，保留其余 map/struct 联接覆盖。
- `runtime_filter_push_down_grf_broadcast`：fingerprint 含 `数字+字符串列` 隐式转换，绝对值随
  存储变化但 **GRF on==off 不变量在 iceberg 上 12 对全部成立**（已逐对核验），按 intent-preserved
  重录。

### 一次性辅助产物
`tools/dev/migrate_suite_iceberg_v3.py`：机械化 CREATE TABLE 改写（剥原生子句 + 加
`format-version=3`，幂等，CTAS 不动），供各套件迁移使用。

### 已知遗留（非阻塞）
- iceberg 扫描字典执行（Option A）未实现；low-cardinality 留原生。
- `query_refs` 的 ASOF `match_condition` 子查询未遍历（ASOF 为 Snowflake 专属，套件未用）。
