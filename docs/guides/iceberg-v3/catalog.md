# Catalog 接入

> Iceberg 的 catalog 决定 metadata 怎么存、commit 怎么协调。NovaRocks 当前主力是 Hadoop catalog，REST 客户端基础已就位但未走通 engine flow，其他 catalog 后端尚未实现。

| Catalog | 状态 | 备注 |
| --- | --- | --- |
| Hadoop | ✅ | `vN.metadata.json` + `version-hint.text` + commit lock 文件 |
| In-memory | ✅ | 仅测试 |
| REST | 🚧 | 客户端基础落地（PR #82），engine 路由 / 鉴权 / 端到端测试待补 |
| AWS Glue | ❌ | |
| Hive Metastore（HMS） | ❌ | |
| Nessie | ❌ | |
| JDBC | ❌ | |

---

## ✅ Hadoop catalog

**实现入口**：`src/connector/iceberg/catalog/hadoop_catalog.rs`

```sql
CREATE EXTERNAL CATALOG ice
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "warehouse" = "file:///tmp/wh"          -- 也支持 s3:// / oss:// / hdfs://
);
```

约定：

- metadata 文件名：`v{N}.metadata.json`（Hadoop convention）
- version hint：`version-hint.text` 单文件指向当前版本号
- commit lock：通过 rename 操作的原子性保证

> ⚠️ 与 StarRocks FE 期望的 `{version}-{uuid}.metadata.json` 命名约定不一致。如果同一份 warehouse 还要被 StarRocks FE 直接读，需要做命名转换；当前没有提供自动 shim，路线图上有跟踪。

## ✅ In-memory catalog

仅用于单元测试与 fixture，不要在生产配置。

## 🚧 REST catalog

**实现现状（PR #82）**：

- ✅ 属性解析：`iceberg.catalog.type=rest` + `uri`（必填）+ `warehouse`（可选，REST 服务也可在 `GET /v1/config` 中下发）
- ✅ Spec 要求的 config handshake：在 catalog 构造时调用 `GET /v1/config` 拉取 server defaults / overrides
- ✅ `build_iceberg_catalog` 统一 dispatcher（Hadoop / Memory / Rest）

**当前不能直接走通的部分**：

- ❌ engine flow 切换：`build_hadoop_catalog(&entry)?` 调用点尚未替换为 `build_iceberg_catalog(&entry)?`，所以 INSERT / SELECT / DDL 实际并不会路由到 RestCatalog
- ❌ 鉴权：当前只有匿名 + 默认 HTTP client，不支持 OAuth2 / SigV4 / Bearer token / `iceberg-rest-conformance-tests`
- ❌ 端到端 SQL 套件：缺一个起 `tabulario/iceberg-rest` + MinIO 的 docker-compose fixture

**临时占位语法**（解析通过，运行时仍走 Hadoop 路径，**不会真的写到 REST 服务**）：

```sql
-- TODO: 暂未端到端可用
CREATE EXTERNAL CATALOG ice_rest
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "http://localhost:8181",
  "warehouse" = "s3://my-warehouse/"
);
```

跟踪：参见 `NovaRocks Iceberg v3 完成度清单` §1（REST Catalog 子任务）。

## ❌ AWS Glue catalog

Spec：实现 `iceberg.catalog.type=glue`，使用 AWS Glue Data Catalog 作为 metadata 后端。AWS 用户的首选路径。

**TODO**：未实现，无替代方案。如果你必须走 Glue，目前只能让 Spark / Trino 把 Iceberg 表元数据写到 Glue，再让 NovaRocks 用 Hadoop catalog 直接读底层文件（不推荐，commit 协调会丢）。

## ❌ Hive Metastore（HMS）

Spec：实现 `iceberg.catalog.type=hive`，与传统 Hadoop 数仓共用 metastore。

**TODO**：未实现。

## ❌ Nessie catalog

Spec：git-like 分支语义，对 branch / tag 是一等公民。

**TODO**：未实现。NovaRocks 的 branch / tag 当前仅在 Iceberg 表内部生效（见 [Branch / Tag](branches-and-tags.md)），不是 Nessie 那种"跨多张表的事务分支"。

## ❌ JDBC catalog

Spec：spec 列出的轻量后端，把 metadata 存到 JDBC 后端（Postgres / MySQL）。

**TODO**：未实现。

## ❌ Catalog credential vending

Spec：REST catalog 在响应 `loadTable` 时下发临时 FileIO 凭据（SigV4 / token），客户端透传给底层对象存储。这是 Lakekeeper / Polaris 的核心安全模型。

**TODO**：未实现。即使后面 REST catalog engine flow 接通，也不会自动下发凭据；FileIO 凭据需要在 catalog properties 里静态指定。
