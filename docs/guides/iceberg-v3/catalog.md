<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Catalog 接入

Catalog 决定 Iceberg 元数据如何发现和提交。当前 provider 的 `NovaRocksCatalogFactory::adopt` 按已经验证的配置选择 Hadoop、REST 或 Hive 实现，并保留当前 generation 已构造的唯一客户端。代码入口是 `novarocks/connector/iceberg/src/catalog/{factory,rest,hive}.rs` 与 `catalog_runtime.rs`；这份操作指南不再使用早期 PR 的完成度作为当前能力判断。

## REST 本地测试

`docker/iceberg-rest/` 提供 REST Catalog、MinIO、Spark fixture，供 `iceberg`、`iceberg-rest` 和 `iceberg-compatibility` 等 SQL 套件使用。共享 runtime 由单一机器 owner 管理，host 端口来自保存的机器配置；不要在 worktree 中固定写 `localhost:8181` 或 `9000`。

已有 fixture 直接使用，确实缺失时按中央推荐配方创建；不做自动 digest/BOM/版本/健康/能力预检。绑定并解析一次 publication：

```bash
docker/iceberg-rest/up.sh
fixture_publication="$(python3 -c 'from pathlib import Path; print(Path("docker/iceberg-rest/runtime/current/published").resolve(strict=True))')"
source "$fixture_publication/env.sh"
trino --server "http://127.0.0.1:$NOVA_ENV_TRINO_PORT" --user root --file "$NOVAROCKS_ICE_REST_CATALOG_SQL"
```

最后一行要求已有按生成 FE/BE 配置启动的 NovaRocks 服务。生成 SQL 使用本次 `NOVAROCKS_ICEBERG_REST_URI`、客户端 warehouse 与对象存储端点。REST catalog 的每个 generation 由 `catalog_runtime.rs` 构造：先校验持久定义（`header.*`、`rest.auth.type`、`iceberg.rest.server-routing-headers`），再解析 FE 的 `CatalogControl` 身份，构造 FE 认证 owner 与 NovaRocks 的 HTTP 客户端（不跟随重定向、不重试、只用 HTTP/1.1），分别经 `with_auth_manager`、`with_client` 注入上游 `RestCatalog`。会话在第一次使用时经建立入口（`rest_protocol/attempt.rs`）建立一次，上游的读请求与 NovaRocks 自己发出的请求共用这一份会话与连接池；catalog 操作由 `NovaRocksRestCatalog` 承接，不是解析后仍走 Hadoop 的占位语法。

`runtime/current` 只是定位器，入口的 `published` 原子发布绑定和配置。`NOVA_ENV_REST_ENV_FILE` 和 `[env].fixture_env_file` 固定到本次 publication；`NOVA_ENV_RUNTIME_DIR` 存 SQLite 等稳定数据，不能用来拼接 env.sh。Spark 使用生成的容器网络端点，NovaRocks 使用 host 端点。

`up.sh --prepare-only` 不调用 Docker，只发布 unbound 入口并清除端点；实际消费须正常 up 并固定一次 publication。测试 fixture 的凭证不能被当作生产鉴权模型。REST 存储访问支持的静态/vended 模式由 provider 配置和消费方 `StorageAuthority` 契约决定，参见 [ADR-0151](../../adr/ADR-0151-credential-renewal-is-driven-by-the-consumer.md)；本指南不据本地匿名 fixture 推断全部远程服务的鉴权能力。

```bash
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg,iceberg-rest,iceberg-compatibility \
  --mode verify --cluster-mode cross-process --cluster-size 3
```

这是验收命令入口，文档更新本身不是套件通过证据。

## REST 认证

REST catalog 的认证只有一个入口：catalog 定义中的凭证绑定。材料保存在各角色本地的启动配置里，不写进 catalog 定义，也不随 native fragment 或 FE/BE 传输。每个角色的 `Authorization` 只由该角色的认证 owner 设置：

| 角色 | catalog 定义中的绑定 | 用途 |
|---|---|---|
| FE | `credential.catalog-control.*`（`consumer-role=frontend`，`mode=static`） | FE 的全部 catalog 请求：`/v1/config` 握手、浏览、加载、DDL、提交、FE 进程内的凭证续期 |
| BE | `credential.data-credential-vending.*`（`consumer-role=backend`，`mode=static`） | BE 为自己取得、续期 vended 存储凭证；只在委派模式（`credential.object-store-data.mode=vended`）下使用 |

catalog 定义没有 `CatalogControl` 绑定时，FE 的请求不带 `Authorization`。两个绑定必须引用不同的 `name`/`generation`。一个委派模式 catalog 的示例（其余对象存储属性按部署补齐）：

```sql
CREATE EXTERNAL CATALOG rest_vended PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "https://catalog.example/api",
  "iceberg.catalog.warehouse" = "wh",
  "header.x-tenant" = "t1",
  "credential.catalog-control.consumer-role" = "frontend",
  "credential.catalog-control.mode" = "static",
  "credential.catalog-control.name" = "rest-control",
  "credential.catalog-control.generation" = "v1",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "vended",
  "credential.data-credential-vending.consumer-role" = "backend",
  "credential.data-credential-vending.mode" = "static",
  "credential.data-credential-vending.name" = "rest-executor",
  "credential.data-credential-vending.generation" = "v1"
);
```

材料在 FE 与每个 BE 的配置中各自声明。OAuth2 client credentials 用 `kind = "iceberg-rest-oauth2"`，bearer 令牌用 `kind = "iceberg-rest-bearer"` 与 `token`；secret 可以写成 `${ENV:NAME}` 引用：

```toml
# FE config
[[connector.credentials]]
purpose = "catalog-control"
name = "rest-control"
generation = "v1"
kind = "iceberg-rest-oauth2"
client_id = "novarocks-fe"
client_secret = "${ENV:REST_FE_CLIENT_SECRET}"

# Each BE config
[[connector.credentials]]
purpose = "data-credential-vending"
name = "rest-executor"
generation = "v1"
kind = "iceberg-rest-oauth2"
client_id = "novarocks-be"
client_secret = "${ENV:REST_BE_CLIENT_SECRET}"
```

OAuth2 的令牌请求只取 catalog 定义中的属性：令牌端点 `oauth2-server-uri`（缺省为 `{uri}/v1/oauth/tokens`）、`scope`（缺省 `catalog`）、`audience`、`resource`；服务端经 `/v1/config` 下发的这些属性被忽略。令牌换取一次后缓存，不刷新；收到 401 或 419 时不换令牌重发。

**会被拒绝的配置。** 下列配置在建 catalog 与构造 generation 时报错：

- `rest.auth.type`（任意大小写）。NovaRocks 注入自己的认证 owner，上游不再按它选择认证方式；错误说明认证只能经 `CatalogControl` 或 `DataCredentialVending` 绑定配置；
- `token`、`credential` 等含敏感标记的属性：catalog 定义不保存秘密；
- 认证类的 `header.*`：`Authorization`、`Proxy-Authorization`、`Cookie`、`Set-Cookie`，以及名称含 `token`、`secret`、`password`、`credential`、`access_key` 的请求头（不分大小写）；错误同样指向这两个绑定；
- 协议类的 `header.*`：`Content-Type`、`Accept`、`User-Agent`、`X-Client-Version`、`X-Iceberg-Access-Delegation`。这些由客户端按请求设置；访问委派头是否发送由调用点决定。

API key、cookie、SigV4 等其他认证方式暂不支持。

**服务端经 `/v1/config` 下发的配置。**

- 下发 `token`、`credential` 或认证类请求头：会话建立失败，该 catalog 的查询与 DDL 都无法执行，错误说明服务端配置携带了认证材料。需要在服务端去掉这些下发项。
- 下发协议类请求头（例如 Gravitino 在 defaults 中下发 `header.X-Iceberg-Access-Delegation`）：NovaRocks 自己发出的请求不使用它，也不需要声明。
- 下发其他请求头（`header.<name>`）：FE 照常发送；要让 BE 在分布式 vended 读取中也发送它，须在 `iceberg.rest.server-routing-headers` 中声明。

**`iceberg.rest.server-routing-headers`。** 逗号分隔、不分大小写的请求头名称列表，声明「服务端经 `/v1/config` 下发的这些请求头是公开的路由信息，可以交给 BE」。catalog 定义中的 `header.*` 本身就是公开的，不需要声明；名称与值都与 `header.*` 相同的下发项按持久定义处理，值不同则按服务端下发处理。列表中不能出现认证类或协议类名称，也不能有空项。例如服务端下发 `header.x-region`：

```sql
"iceberg.rest.server-routing-headers" = "x-region"
```

**不跟随重定向。** 任何 REST 请求都不跟随 3xx，包括 `/v1/config`、令牌端点与上游发出的读请求。`uri` 与 `oauth2-server-uri` 必须配置为最终地址：遇到 3xx 时，读请求与会话建立失败，变更请求报告结果未知。

**vended 读取的准入错误。** 委派模式下，FE 为 BE 形成凭证租约时检查 BE 能否自己取得凭证；不能时在派发之前拒绝查询，BE 收不到任务。委派模式 catalog 上的 CTAS 在 stage 之前做同样的配置检查。

| 错误中的说明 | 含义 | 修正 |
|---|---|---|
| `declares no DataCredentialVending credential binding` | catalog 定义没有 BE 的取得身份 | 加上 `credential.data-credential-vending.*`，并在每个 BE 配置中声明对应的 `[[connector.credentials]]` |
| ``sent request header `<name>` through /v1/config ... add `<name>` to the `iceberg.rest.server-routing-headers` catalog property`` | 服务端下发了未声明的请求头，BE 无法完整复现 FE 的路由 | 确认它是公开的路由头后加入 `iceberg.rest.server-routing-headers`，或让服务端停止下发 |
| `routing context for execution nodes exceeds its bounds` | 路由请求头超过 16 个，或名称与值合计超过 4 KiB | 减少 `header.*` 与声明的下发请求头 |
| `routing address is not a valid URL`、`routing address must be http or https ...` 或 `routing address exceeds 2048 bytes` | 生效的 catalog 地址或服务端给出的凭证端点含用户信息、查询串或片段，或过长 | 修正 `uri`，或服务端的 `overrides.uri` / 凭证端点地址 |
| `cannot be forwarded to execution nodes` | 某个路由请求头的值含非可见 ASCII 字符 | 改用可见 ASCII 的值 |

FE 无法核对 BE 本地配置：BE 缺少 `DataCredentialVending` 所引用的身份时，该 BE 第一次读取数据时明确失败。BE 取得凭证时只使用 FE 冻结的地址与公开路由请求头，加上自己的令牌，不建立自己的 catalog 会话。

变更请求的结果判定（例如 400、404 报告结果未知、不清理产物，删除接受 204 或空响应体的 200）与上述认证规则的长期契约见 [ADR-0175](../../adr/ADR-0175-iceberg-rest-session-authority-and-mutation-evidence.md)。

## Hadoop 与 Hive

Hadoop 仅用于单写者测试，入口为 `novarocks/connector/iceberg/src/hadoop_catalog.rs`；使用 `iceberg.catalog.type=hadoop` 和明确 warehouse。CREATE/UPDATE 安装完整且不替换的版本文件，hint 仅作收尾；读路径按 hint 前探，缺失/非法时完整列举，损坏内容可显式 DROP。metadata 根固定，不支持其他 CREATE location、SetLocation、write.metadata.path 或 HDFS。它不提供生产并发、跨引擎 CAS、断电恢复或 Unknown 后安全继续；生产写入使用 REST catalog。详见 [ADR-0196](../../adr/ADR-0196-hadoop-test-catalog-complete-version-installation.md)。

**Hive Metastore（HMS）是永久只读兼容入口。** NovaRocks 可以读取由 Spark 等外部引擎写入的表，查询元数据表并进行 branch 或 snapshot ID 时间旅行。HMS 上的建库、建表、ALTER、ref/view 变更、CTAS、DML、TRUNCATE、ADD FILES、维护、ANALYZE、OPTIMIZE 与 MV 管理均在第一次外部副作用之前拒绝；不会因为这些请求写出数据或规划文件，也不会建立统计或 OPTIMIZE 作业。`CREATE / DROP CATALOG` 只管理 NovaRocks 自身配置，不属于 HMS 湖侧 mutation。

被拒绝时，错误会说明 `Hive Metastore catalog is a read-only compatibility entry`，列出操作并提示使用 Iceberg REST 或 Hadoop catalog 写入。需要写入时请选择这两类 catalog；HMS 不提供恢复写入的开关。长期边界见 [ADR-0169](../../adr/ADR-0169-read-only-hms-and-single-writer-admission.md)。

**HMS 读取的三项回退。** HMS 直接使用上游 `iceberg-catalog-hms`，不维护补丁。上游做不到的三处，读取行为与 REST、Hadoop 不同：

- **列表返回库中的全部名称。** `SHOW TABLES` 与 `information_schema.tables` 列出 Metastore 库中的全部条目，包括 Hive 表、Hive 视图与 Iceberg 视图。名称与 Trino 一致，类型不区分：`information_schema.tables` 把每个条目都标为 `BASE TABLE`，视图不会以 `VIEW` 出现。读取非 Iceberg 条目报 `No 'metadata_location' set on table`，并归为数据损坏；它表示该条目不是 Iceberg 表，不是 Iceberg 元数据损坏。列举不再按固定条目数或名字字节总量拒绝；完整枚举仍受原请求期限、取消与实际 I/O 资源约束。HMS 不支持视图列举，因此按 `table_type = 'VIEW'` 过滤时没有 VIEW 行，不因缺少这项能力报错。
- **缺表报为服务不可用。** 读取不存在的表时，错误不是 `unknown table`，而是 `Unavailable` 类的 provider 错误：正文为 `Operation failed for hitting thrift error`，其后的错误来源中含 `NoSuchObjectException` 与表名。看到这一组合时先核对库名与表名；它表示缺表，不是 Metastore 故障。按错误类别自动重试的调用方可能会重试它。
- **元数据读取不计入请求。** FE 读取 HMS 表的 metadata JSON、manifest list 与 manifest（包括元数据表与时间旅行规划）时，使用构造 catalog 时建立、不带请求上下文的 FileIO，不计入本次请求的存储准入、取消检查与依赖记账；取消查询要等在途读取结束。对 Metastore 的 Thrift 调用仍按请求准入，BE 读取数据文件不受影响。

以库 `sales` 为例：其中有 Iceberg 表 `orders`、Hive 表 `customers` 与 Hive 视图 `orders_v`，表 `returns` 不存在。

| 语句 | 结果 |
|---|---|
| `SHOW TABLES FROM hms.sales` | `customers`、`orders`、`orders_v`，与 Trino 列出的名称一致 |
| `SELECT table_name, table_type FROM hms.information_schema.tables WHERE table_schema = 'sales'` | 三行，均为 `BASE TABLE`；`orders_v` 的类型不准确 |
| `SELECT table_name FROM hms.information_schema.tables WHERE table_schema = 'sales' AND table_type = 'VIEW'` | 零行；HMS 没有视图列举能力，条目类型回退仍保留 |
| `SELECT * FROM hms.sales.orders` | 正常返回；FE 侧元数据读取不计入本次请求 |
| `SELECT * FROM hms.sales.customers` | `No 'metadata_location' set on table`，归为数据损坏 |
| `SELECT * FROM hms.sales.returns` | `Unavailable` 类 provider 错误，正文 `Operation failed for hitting thrift error`，来源中含 `NoSuchObjectException` |
| `INSERT INTO hms.sales.orders ...` | 拒绝：只读兼容入口 |

这三项是为不维护补丁而接受的回退，取舍与重新评估条件见 [HMS 版 ADR-0216](../../adr/ADR-0216-hms-uses-upstream-catalog-with-read-fallbacks.md)。系统关系与无视图列举能力时的语义见 [ADR-0226](../../adr/ADR-0226-engine-metadata-through-typed-system-relations.md)，固定列举额度的退役范围见 [ADR-0227](../../adr/ADR-0227-metadata-listing-and-task-transport-use-real-capacity-boundaries.md)。

Hive 的 provider 实现与 runtime 构造入口为 `catalog/hive.rs` 和 `catalog_runtime::build_hms_catalog`。本地 HMS fixture 由 `docker/iceberg-hive/` 管理，连接选定的版本化 REST fixture 网络供 Spark/对象存储访问。运行 `iceberg-hms` 前先按该目录操作说明启动 HMS。HMS 等外部 endpoint 尚在时，普通与 force catalog 删除均拒绝；先退出 HMS 自己的精确项目与连接。

## 生命周期与边界

`down.sh --runtime-only` 解绑并保留历史数据引用；加 `--purge` 才在全部私有位置清理成功后释放引用。共享实例停止/删除必须使用 `fixture-runtime.sh` 的精确 runtime ID，不能按旧项目名猜测。force catalog 删除仅条件解绑目标、保留 worktree 数据引用；对象存储任何删除都受 catalog/数据引用保护。

新对象存储会重建 benchmark READY 数据。旧 `nr-iceberg-rest`、`nr-iceberg-hive` 和卷保留，退役由用户另行安排。完整命令、owner locator 与操作保护见 [fixture README](../../../docker/iceberg-rest/README.md) 和 [ADR-0165](../../adr/ADR-0165-versioned-fixture-runtime-ownership.md)。其它 catalog 的支持面应查当前 provider 配置与测试，不能从旧 TODO 清单推断。
