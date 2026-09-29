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

`docker/iceberg-rest/` 提供 REST Catalog、MinIO、Spark fixture，供 `iceberg`、`iceberg-rest` 和 `iceberg-compatibility` 等 SQL 套件使用。共享 runtime 按输入版本化，host 端口由 owner 分配；不要在 worktree 中固定写 `localhost:8181` 或 `9000`。

先显式供给缺失输入，普通运行只验证本机 BOM；再绑定并解析一次 publication：

```bash
docker/fixture-inputs/provision.sh   # Explicit supply, when needed.
docker/iceberg-rest/up.sh
fixture_publication="$(python3 -c 'from pathlib import Path; print(Path("docker/iceberg-rest/runtime/current/published").resolve(strict=True))')"
source "$fixture_publication/env.sh"
mysql -h 127.0.0.1 -P "$NOVA_ENV_MYSQL_PORT" -uroot < "$NOVAROCKS_ICE_REST_CATALOG_SQL"
```

最后一行要求已有按生成 FE/BE 配置启动的 NovaRocks 服务。生成 SQL 使用本次 `NOVAROCKS_ICEBERG_REST_URI`、客户端 warehouse 与对象存储端点。REST 客户端构造由 `catalog_runtime::build_rest_catalog` 路径完成；catalog 操作由 `NovaRocksRestCatalog` 承接，不是解析后仍走 Hadoop 的占位语法。

`runtime/current` 只是定位器，入口的 `published` 原子发布绑定和配置。`NOVA_ENV_REST_ENV_FILE` 和 `[env].fixture_env_file` 固定到本次 publication；`NOVA_ENV_RUNTIME_DIR` 存 SQLite 等稳定数据，不能用来拼接 env.sh。Spark 使用生成的容器网络端点，NovaRocks 使用 host 端点。

`up.sh --prepare-only` 不调用 Docker，也不证明 BOM/健康；保存记录为 ready 才恢复端点，首次或缺失/deleting 记录发布 unbound。测试 fixture 的凭证不能被当作生产鉴权模型。REST 存储访问支持的静态/vended 模式由 provider 配置和消费方 `StorageAuthority` 契约决定，参见 [ADR-0151](../../adr/ADR-0151-credential-renewal-is-driven-by-the-consumer.md)；本指南不据本地匿名 fixture 推断全部远程服务的鉴权能力。

```bash
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg,iceberg-rest,iceberg-compatibility \
  --mode verify --cluster-mode cross-process --cluster-size 3
```

这是验收命令入口，文档更新本身不是套件通过证据。

## Hadoop 与 Hive

Hadoop 实现入口为 `novarocks/connector/iceberg/src/hadoop_catalog.rs`；使用 `iceberg.catalog.type=hadoop` 和明确 warehouse。它的 metadata/commit 约定由该实现管理，不应把 warehouse 当作可以绕过原 catalog 协调直接写入的路径。

Hive 的 provider 实现与 runtime 构造入口为 `catalog/hive.rs` 和 `catalog_runtime::build_hms_catalog`。本地 HMS fixture 由 `docker/iceberg-hive/` 管理，连接选定的版本化 REST fixture 网络供 Spark/对象存储访问。运行 `iceberg-hms` 前先按该目录操作说明启动 HMS。HMS 等外部 endpoint 尚在时，普通与 force catalog 删除均拒绝；先退出 HMS 自己的精确项目与连接。

## 生命周期与边界

`down.sh --runtime-only` 解绑并保留历史数据引用；加 `--purge` 才在全部私有位置清理成功后释放引用。共享实例停止/删除必须使用 `fixture-runtime.sh` 的精确 runtime ID，不能按旧项目名猜测。force catalog 删除仅条件解绑目标、保留 worktree 数据引用；对象存储任何删除都受 catalog/数据引用保护。

新对象存储会重建 benchmark READY 数据。旧 `nr-iceberg-rest`、`nr-iceberg-hive` 和卷保留，退役由用户另行安排。完整命令、owner locator 与操作保护见 [fixture README](../../../docker/iceberg-rest/README.md) 和 [ADR-0165](../../adr/ADR-0165-versioned-fixture-runtime-ownership.md)。其它 catalog 的支持面应查当前 provider 配置与测试，不能从旧 TODO 清单推断。
