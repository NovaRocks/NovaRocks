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

# NovaRocks Codex Environments

This directory holds only the Codex workspace setup manifest
(`environment.toml`).

The actual local Iceberg REST + MinIO + Spark test environment lives at
[`docker/iceberg-rest/`](../../docker/iceberg-rest/). The Docker services are
shared across worktrees by default; each Codex setup runs
`docker/iceberg-rest/up.sh --prepare-only`, which only generates this
worktree's runtime entry, NovaRocks server port, and config files. It does not
start Docker. See that README for usage, ports, and CI integration details.

Linux 新开发节点的显式安装入口见 [Linux 环境准备](../../tools/dev/README.md)。它可联网安装和 provision；Codex 自动 setup 的 `--prepare-only` 仍只生成离线 unbound 入口。

SQL runner 配置使用 `[cluster].trino_port`，对应同次 publication 的 `NOVA_ENV_TRINO_PORT`；命令行覆盖为 `--trino-port`。旧 `port`、`password` 和参照/diff 参数在解析期拒绝。普通 `--prepare-only` 只生成离线入口，不能证明 Trino 监听、fixture 健康或原生测试已通过。golden 与语料会话声明见 [SQL README](../../tests/sql/README.md#golden-格式)。
