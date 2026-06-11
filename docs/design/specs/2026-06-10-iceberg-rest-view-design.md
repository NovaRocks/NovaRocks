# Iceberg REST Catalog View 支持(+ 删除 `{version}-{uuid}` 元数据回退读取)

- 日期:2026-06-10
- 来源:Obsidian roadmap `IV3-10-catalog-glue-hive-rest-view.md`(IV3-10 · Catalog 扩展)的 rest-view 子项
- 状态:设计已确认,待实施
- 范围外:Glue / Hive catalog、ALTER VIEW、view 缓存、优化器级 view 改写、hadoop/memory catalog 的 view 支持

本 spec 包含两个独立工作项:

- **工作项 A**(前置小改动,独立 commit 先落地):删除 `{version}-{uuid}.metadata.json` 回退读取兼容逻辑。
- **工作项 B**(主体):Iceberg REST catalog 的 view 元数据 CRUD + SELECT 查询支持。

两者代码不重叠;A 先行合入。

---

## 工作项 A:删除 `{version}-{uuid}.metadata.json` 回退读取

### 背景

NovaRocks 早期 Hadoop catalog 写表用 iceberg-rust 内部命名 `{version}-{uuid}.metadata.json`,
后改为 Hadoop 约定 `v{N}.metadata.json` + `version-hint.text`(StarRocks FE / Spark / Trino
依赖该命名做发现)。读侧 `choose_latest_metadata_filename` 保留了对旧命名的回退,注释标注
"for pre-migration tables"。

按项目原则"NovaRocks 没有历史用户,不写兼容性代码",这些存量表不存在,回退是死代码。
已验证:`Memory` 与 `Hadoop` kind 均经 `HadoopFileSystemCatalog` 写 `vN` 命名
(`registry.rs::build_iceberg_catalog`),REST kind 不走目录列举路径;无任何单测引用回退分支。

### 改动

`src/connector/iceberg/catalog/registry.rs`:

1. 删除 `parse_internal_metadata_version`(约 :1747)。
2. `choose_latest_metadata_filename`(约 :1771)删除内部格式 fallback 分支,只认
   `v{N}.metadata.json`;更新函数与调用点注释(移除 "pre-migration" 表述)。
3. 行为变化:目录中只有内部格式文件的表报 `unknown table`(fail-fast)。
4. 新增单测:混合命名目录只挑 `vN` 最大版本;纯内部格式目录返回错误。

不改 vendored crate(`MetadataLocation::from_str` 是 REST/memory catalog 自身约定,与本路径无关)。

---

## 工作项 B:Iceberg REST Catalog View 支持

### 1. 目标与语句表面

在 standalone 模式下,对 **REST 类型** iceberg catalog 支持:

| 语句 | 语义 |
| --- | --- |
| `CREATE VIEW [IF NOT EXISTS] v AS SELECT ...` | REST create-view endpoint 持久化 view 元数据 |
| `CREATE OR REPLACE VIEW v AS SELECT ...` | 已存在时走 REST commit(保留版本历史),否则 create |
| `DROP VIEW [IF EXISTS] v` | REST drop-view endpoint |
| `SELECT ... FROM <catalog>.<db>.<view>` | load-view 取 SQL,内联展开后正常执行 |
| `SHOW CREATE VIEW v` | load-view 拼 DDL 文本 |
| `SHOW VIEWS [FROM db]` | REST list-views endpoint |

非 REST 的 iceberg catalog(hadoop / memory)上执行 view DDL → 明确报错
`view operations require a REST catalog`。本地/默认 catalog 上的会话级内存 view 行为不变。

### 2. 分层架构

```text
SQL 语句
  └─ src/engine/statement.rs 路由(按目标 catalog 类型分发)
       ├─ 本地/默认 catalog → 现有会话级内存 view(不变)
       └─ iceberg REST catalog
            └─ src/connector/backend.rs CatalogBackend 新增 view 方法
                 └─ src/connector/iceberg/catalog/registry.rs view 包装函数
                      └─ vendored iceberg Catalog trait 新增 view 方法(默认 FeatureUnsupported)
                           └─ vendored iceberg-catalog-rest 实现 REST view endpoint
```

REST endpoint(`{prefix}` 来自 `/v1/config` 握手,由现有 `HttpClient` 处理):

| 操作 | HTTP |
| --- | --- |
| create | `POST /v1/{prefix}/namespaces/{ns}/views` |
| load | `GET /v1/{prefix}/namespaces/{ns}/views/{view}` |
| commit(OR REPLACE) | `POST /v1/{prefix}/namespaces/{ns}/views/{view}` |
| drop | `DELETE /v1/{prefix}/namespaces/{ns}/views/{view}` |
| exists | `HEAD /v1/{prefix}/namespaces/{ns}/views/{view}` |
| list | `GET /v1/{prefix}/namespaces/{ns}/views` |

### 3. vendored crate 改动

**`vendor/iceberg-0.9.0`**(已 vendor,有本地 patch 先例):

- `Catalog` trait 新增方法,默认实现返回 `ErrorKind::FeatureUnsupported`:
  `create_view(namespace, ViewCreation)`、`load_view(&TableIdent)`、
  `update_view(view-commit)`、`drop_view(&TableIdent)`、`view_exists(&TableIdent)`、
  `list_views(&NamespaceIdent)`。
- spec 结构(`ViewMetadata` / `ViewMetadataBuilder` / `ViewVersion` / `ViewCreation` /
  `ViewUpdate`)已存在,直接复用;`ViewUpdate` 的 serde tag 已是 REST spec 的 kebab-case action 名。
- `HadoopFileSystemCatalog` / `MemoryCatalog` 不实现(默认报 unsupported)。

**`vendor/iceberg-catalog-rest-0.9.0`**(新 vendor:从 crates.io 0.9.0 拷入,
`Cargo.toml` 增加 `[patch.crates-io]` 条目):

- `types.rs` 按 Iceberg REST OpenAPI spec 补 `CreateViewRequest`(name / location 可选 /
  schema / view-version / properties)、`LoadViewResult`(metadata-location / metadata /
  config)、`UpdateViewRequest`(identifier / requirements / updates,requirement 仅
  `assert-view-uuid`)。
- `catalog.rs` 实现上述六个 trait 方法,复用现成 `HttpClient`(认证、prefix、错误映射)。
- create 请求不在客户端拼 location:`location` 字段缺省,由 REST 服务端按 warehouse 分配。

### 4. 写路径语义

**CREATE VIEW**:

1. 解析 `CREATE [OR REPLACE] VIEW [IF NOT EXISTS] name [(col_alias, ...)] AS query`
   (sqlparser 原生 `Statement::CreateView`,已支持)。
2. 用现有 analyzer 分析 SELECT,得到输出列名/类型;列别名列表(若有)重命名输出;
   经现有 arrow→iceberg 类型转换得到 Iceberg `Schema`。
3. 构造 `ViewCreation`:
   - representation:单条 SQL representation,`dialect = "starrocks"`(NovaRocks 解析的
     就是 StarRocks 方言,与 StarRocks FE 互通;与 StarRocks `IcebergView.STARROCKS_DIALECT` 一致);
   - `default_namespace` = view 所在 db,`default_catalog` = catalog 名;
   - properties:用户 COMMENT(如有)存入 `comment` 属性;version summary 写
     `engine-name=novarocks`。
4. `IF NOT EXISTS` 且已存在 → 静默成功;无该子句且已存在 → 报错。

**CREATE OR REPLACE**:目标已存在时 load-view 取当前 `ViewMetadata`,用
`ViewMetadataBuilder` 生成 `add-schema` + `add-view-version` + `set-current-view-version`
updates,带 `assert-view-uuid` requirement 走 commit endpoint——与 iceberg-java
`createOrReplace` 一致,保留版本历史,不用 drop+create(非原子且丢历史)。

**DROP VIEW**:REST `DELETE`,`IF EXISTS` 吞 404。**严格类型检查**:`DROP VIEW`
碰到同名表、`DROP TABLE` 碰到 view 均报明确错误(比 StarRocks 的静默重定向严格,
符合项目 fail-fast 规则)。

### 5. 读路径语义

**SELECT 展开**:

- 挂载点:外部表注册(`src/engine/query_prep.rs::register_external_table_by_name` 一线)
  load_table 报 no-such-table 时回退 `load_view`;表是常见情形,顺序为先表后 view
  (与 StarRocks `getTable` → `getView` 一致)。
- representation 选择:优先 `dialect == "starrocks"`(大小写不敏感);无匹配时回退
  **第一个 SQL representation**(与 iceberg-java `View.sqlFor` 语义一致,使 Spark 建的
  简单 view 可查)。解析失败报错并指明该 representation 的 dialect。
- 展开方式:用 `StarRocksDialect` 解析 view SQL,以 Derived 子查询内联(复用
  `src/engine/view_rewrite.rs` 的展开模式);view SQL 内未限定的表名用 view 元数据的
  `default-catalog` / `default-namespace` 补全,**不是**会话当前库。
- 嵌套 view 递归展开,带循环检测(展开栈中重复出现同一 view 标识 → 报错)。
- 本期不做 view 元数据缓存(每次查询 load-view;正确性优先,缓存留作后续)。

**SHOW CREATE VIEW**:load-view 后按 StarRocks `getExternalCatalogViewDdlStmt` 的格式
输出 `CREATE VIEW \`name\` (cols) [COMMENT/PROPERTIES] AS <sql>;`。

**SHOW VIEWS [FROM db]**:list-views endpoint,列出指定/当前 db 下的 view 名。

两个 SHOW 语句在 `src/sql/parser/dialect/` 新增解析(沿 `SHOW MATERIALIZED VIEWS`
的自定义解析先例);当前 parser 无任何 SHOW VIEWS / SHOW CREATE VIEW 支持。

### 6. 错误处理

- 所有 view 操作在非 REST iceberg catalog 上 → `view operations require a REST catalog`。
- load-view 404 → `unknown view: <db>.<name>`;SELECT 路径上表与 view 均不存在时报
  unknown table(保持现有报错口径)。
- REST 错误经现有错误映射包装,信息包含 endpoint 与 view 标识。
- 不做任何静默回退(无 dialect 匹配且无 SQL representation → 明确报错)。

### 7. 测试策略

- **`sql-tests/iceberg-rest`** 新增 case(golden 用 `--mode record --record-from target`):
  - `iceberg_rest_view_ddl`:create / if-not-exists / or-replace(验证替换后定义生效)/
    drop / if-exists / 重复创建报错 / drop 不存在报错 / DROP TABLE 与 view 类型不匹配报错;
  - `iceberg_rest_view_select`:SELECT 查 view、view 套 view、default-namespace 补全
    (view SQL 引用裸表名)、循环引用报错;
  - `iceberg_rest_view_show`:SHOW CREATE VIEW / SHOW VIEWS。
- **`sql-tests/iceberg-compatibility`** 新增:Spark 建 view → NovaRocks SELECT +
  SHOW CREATE VIEW(验证 dialect 回退);NovaRocks 建 view → Spark 查
  (Spark `sqlFor("spark")` 回退读 starrocks representation)。
- **fixture**:`docker/iceberg-rest/compose.yml` 的 REST 服务加
  `CATALOG_JDBC_SCHEMA__VERSION: V1`(JdbcCatalog view 支持开关,默认 V0 不支持 view)。
  注意:共享 Docker 工程,需重启共享 REST 服务一次,影响其他 worktree,择机执行。
- **单元测试**:vendored rest crate 的 view 类型序列化/反序列化与 endpoint 路径;
  registry 层沿用现有无 Docker 的 mock REST 测试基建(`registry.rs` 测试模块)。

### 8. 关键代码入口

| 层 | 位置 |
| --- | --- |
| vendored Catalog trait | `vendor/iceberg-0.9.0/src/catalog/mod.rs` |
| vendored REST 实现 | `vendor/iceberg-catalog-rest-0.9.0/src/{catalog,types}.rs`(新) |
| registry 包装 | `src/connector/iceberg/catalog/registry.rs` |
| backend trait | `src/connector/backend.rs` |
| DDL 路由 | `src/engine/statement.rs`(现有会话 view 处理在 `src/engine/statistics.rs`) |
| SELECT 展开 | `src/engine/query_prep.rs` + `src/engine/view_rewrite.rs` |
| SHOW 解析 | `src/sql/parser/dialect/`(参照 `materialized_view.rs`) |
| 测试 | `sql-tests/iceberg-rest/`、`sql-tests/iceberg-compatibility/`、`docker/iceberg-rest/compose.yml` |

### 9. 验收标准

1. REST catalog 上 CREATE / CREATE OR REPLACE / DROP / SHOW CREATE VIEW / SHOW VIEWS 可用,
   SELECT 查 view 结果正确(含嵌套与裸表名补全)。
2. Spark 建的 view NovaRocks 能查;NovaRocks 建的 view Spark 能查(简单 SQL)。
3. hadoop / memory catalog 上 view DDL 报明确错误。
4. 工作项 A 合入后,`vN.metadata.json` 是唯一被识别的元数据命名,相关单测通过。
5. `cargo fmt` / `cargo clippy` / `cargo test` 与上述 SQL suite 全绿。
