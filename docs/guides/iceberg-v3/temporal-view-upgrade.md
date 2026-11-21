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

# 持久视图的时间精度升级

升级后的裸 `TIMESTAMP` 表示 `TIMESTAMP(3)`。旧版保存的视图中，`CAST(... AS TIMESTAMP)` 可能按微秒求值。准备切换前，从实际旧版取回定义、签名和值，为确实使用微秒的表达式明确指定 `TIMESTAMP(6)`。以下步骤适用于 Iceberg REST Catalog 的普通持久视图。

## 清点原定义

1. 使用实际部署的旧版完成清点。记录 NovaRocks 版本、Catalog、namespace、视图名以及原定义保存的默认 Catalog 和 namespace。使用 Catalog 的 List Views 接口枚举待迁移 namespace 下的视图，并加载各视图的当前 metadata。
2. 对每个视图保存原始 metadata、metadata location、view UUID、当前 version ID、当前版本的 schema ID 与 schema、全部 SQL representations、默认 Catalog 和 namespace。用 `SHOW CREATE VIEW` 从旧版保留查询所见的完整定义。保留与视图定义绑定的表、视图及版本来源，按实际解析后的依赖记录传递引用。字符串、注释和同名 CTE 都有各自的作用域，清点应依据定义绑定。
3. 用旧版读取视图和依赖视图的声明与实际值。HTTP 客户端在原 `POST /v1/statement` 请求带上 `X-Trino-Client-Capabilities: PARAMETRIC_DATETIME,PARAMETRIC_INTERVAL`，保持相同的 Catalog、schema 和时区，跟随每个 `nextUri` 到终页。保存完整 `columns` 和 `typeSignature`、所有数据页与终态。用足以区分毫秒与微秒的正负样本，包含有精度的值、NULL 和实际依赖输出。

例如，加载 Catalog 返回的视图 endpoint 后，从此次观察实体提取当前定义。目录配置决定 endpoint 及鉴权参数。

```sh
curl --fail-with-body --silent --show-error "$REST_VIEW_URI" > view-before.json
jq -e '
  .metadata as $m
  | ($m.versions[] | select(.["version-id"] == $m["current-version-id"])) as $v
  | {
      uuid: $m["view-uuid"],
      location: .["metadata-location"],
      version: $v["version-id"],
      schema: ($m.schemas[] | select(.["schema-id"] == $v["schema-id"])),
      default_catalog: $v["default-catalog"],
      default_namespace: $v["default-namespace"],
      representations: $v.representations
    }
' view-before.json
```

只有来源明确且已证明等价的替换进入发布清单。将未知来源、无法在原上下文绑定、或值与声明无法对照的定义交给定义作者处理。记录其影响的依赖及流量范围。

## 显式替换和对照

以下示例在原 Catalog 和 namespace 下执行。旧版的 `v` 定义为 `SELECT CAST('2024-01-01 00:00:00.123456' AS TIMESTAMP) AS ts`，依赖视图为 `v_dep`。旧版实际返回 `timestamp(6)` 和 `2024-01-01 00:00:00.123456`。

```sql
USE trc6c_views.view_precision;
SHOW CREATE VIEW v;
SHOW CREATE VIEW v_dep;
SELECT ts FROM v;
SELECT ts FROM v_dep;
```

以原定义为基础，仅改已核对的类型节点。替换前在同一保存上下文试读改写的 SELECT，对照声明、完整值和所绑定的依赖。若定义有注释、属性或多种方言的表示，按原版本和 Catalog 的替换能力核对其保留方式。在 Catalog owner 协调的发布窗口冻结待改视图及相关依赖的写者，并重新加载 metadata。对照当前 UUID、版本、metadata location 及已记录的依赖。有变化时暂停该批并重新清点。

```sql
USE trc6c_views.view_precision;
CREATE OR REPLACE VIEW trc6c_views.view_precision.v AS
SELECT CAST('2024-01-01 00:00:00.123456' AS TIMESTAMP(6)) AS ts;
SHOW CREATE VIEW v;
SELECT ts FROM v;
SELECT ts FROM v_dep;
```

每次替换后重新从 Catalog 取回 metadata，确认实际新版本和声明，并记录完成的替换。先发布旧版能读的等价定义并完成旧版复核。其余已验证的替换在新版接流量前发布。切换部署时等待旧 FE 和 BE 退出，再用正常部署配置启动新版，连接同一 Catalog。重复上述读回，比较 `timestamp(6)`、全部小数位、原字段顺序和依赖结果，确认原保存上下文。保留未替换的同类视图作为对照。

## 部分完成和恢复

- REST 替换使用 `assert-view-uuid` 防止在目标删除后重建的情况下提交。UUID 相同仍可能发生并发替换，因此发布窗口继续由 Catalog owner 协调写者。每次 SQL 结果都与实际读回版本核对。 [REST 视图替换语义](../../adr/ADR-0176-iceberg-rest-request-vocabulary-and-supported-server-floor.md)
- 分别记录已发布、确定未发布和结果未知。看到并发版本变化、结果未知或部分替换时，暂停后续发布，按保存的版本、SQL 和实际依赖重新读回，确定哪些操作已生效。 [Lake publication](lake-publication.md)
- 回退时先由同一 Catalog owner 确认当前版本确为此次替换产物，重新核对依赖及恢复所用版本的声明和值。需要恢复原 SQL 时，在原保存上下文用标准 `CREATE OR REPLACE VIEW` 提交已保存的原定义，并将读回版本记为恢复版本。涉及裸 `TIMESTAMP` 的原定义应先在实际回退版本上复核。涉及未知状态或其他写者版本时，由 Catalog owner 先确认实际归属，再决定恢复名单。
