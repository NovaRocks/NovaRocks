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

# SQL Tests

SQL tests have two physical roots:

- `tests/sql/correctness/` contains the small-data correctness corpus used by development and full CI.
- `tests/sql/benchmarks/` contains fixed-data performance workloads. They are not selected by correctness CI.

## Object Store Prerequisite

Iceberg 等对象存储套件使用显式 runner 配置中的端点与凭证；fixture 服务的 host 端口由 runtime owner 分配，不能假设 `9000/8181` 或默认凭证。先验证本机 BOM 并绑定，再解析一次 publication：

```bash
docker/iceberg-rest/up.sh
fixture_publication="$(python3 -c 'from pathlib import Path; print(Path("docker/iceberg-rest/runtime/current/published").resolve(strict=True))')"
source "$fixture_publication/env.sh"
```

生成的 `sql-test.toml` 同时包含实际端点和 `[env].fixture_env_file` 的不可变 publication 路径。缺少运行配置时 runner 明确报错，不回退固定端点。BOM 前置条件失败（75）是 BLOCKED；端口/身份/owner 失败是 VERIFY FAILED。verify/测试不自动 provision。

`up.sh --prepare-only` 无 Docker，只恢复已保存状态；unbound 不提供占位端点，保存的 ready 也不是健康证明。`NOVA_ENV_RUNTIME_DIR` 存放稳定运行数据，消费者使用 `NOVA_ENV_REST_ENV_FILE`，不能拼接 runtime/env.sh。解绑/purge、版本实例及 HMS 退出顺序见 [fixture 操作说明](../../docker/iceberg-rest/README.md)。

## Who Owns the Server

The runner's default `--cluster-mode all-in-one` does **not** start a server: it
connects to the host and port its config names, so a server must already be
running there. `--cluster-mode cross-process` is the opposite — the runner
launches and owns the FE/BE cluster itself, locating the binary through
`NOVAROCKS_BIN` or a build under `target/`.

## Default Standalone Flow

Start a server first. It takes one FE config and one BE config — copy
`novarocks-fe.toml.example` and `novarocks-be.toml.example` from the repo root,
or use the generated pair below. There is no `--port` flag:

```bash
NO_PROXY=127.0.0.1,localhost cargo run -p novarocks-server -- standalone \
  --role all-in-one --fe-config ./novarocks-fe.toml --be-config ./novarocks-be.toml
```

Inside a worktree, do not assume a port — source the generated environment and
use its configs, so this worktree cannot collide with another:

```bash
source docker/iceberg-rest/runtime/current/env.sh
NO_PROXY=127.0.0.1,localhost cargo run -p novarocks-server -- standalone \
  --role all-in-one --fe-config "$NOVAROCKS_FE_CONFIG" --be-config "$NOVAROCKS_BE_CONFIG"
```

When backgrounding the server, gate the first query on the `NOVAROCKS_READY`
marker it prints after binding — probing the port alone cannot tell a fresh
server from a leftover process that already owned it.

非隔离套件运行时使用已绑定 publication 的生成配置；即使 `filter` 不访问 Iceberg，runner 仍在启动时核验这组明确的 fixture 配置：

```bash
cargo run --manifest-path tests/sql/runner/Cargo.toml --bin novarocks-sql-test -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite filter --mode verify
```

所有非隔离套件都需要明确的端点、凭证和 `fixture_env_file`。默认 `tests/sql/runner/conf/default.toml` 不提供这些值，不能当作可直接运行的 fixture 配置。使用上述生成的 `--config`，或提供具有同一完整契约的显式配置；runner 不以默认端口或环境中的旧端点补齐缺失值。隔离套件由 runner 启动自己的 fixture，再统一投影其配置。

`tests/sql/correctness/README.md` carries the suite map — which engine area each
suite covers and what fixture or topology it needs. Choose suites from it rather
than running the whole corpus.

## Benchmark Flow

Benchmarks are deliberately separate from correctness suites. Use the local
wrapper, which builds the release server binary and defaults to SSB:

```bash
tools/benchmark/run-sql-benchmark.sh
```

Pass benchmark-runner options after the script name to choose a workload or an
output location. Benchmarks always use the cross-process FE/BE harness; choose
the number of BEs for this run with `--backend-count` (the default is one):

```bash
tools/benchmark/run-sql-benchmark.sh --suite tpc-ds --backend-count 2
tools/benchmark/run-sql-benchmark.sh --suite all --backend-count 4 --output-dir /tmp/novarocks-benchmarks
```

benchmark runner 在 suite hook 前解析固定共享数据。bootstrap 先 bind 并读取同次 publication，再核对实际 Spark image ID/BOM producer 后检查或构建 READY；第一次新对象存储需要重建数据，worktree purge 保留共享 READY。
It verifies results, performs one warmup pass, records five serial measured
passes, and captures a profile pass. Generated reports go to
`reports/sql-benchmarks/` and do not belong in correctness CI.

An external controller may attach an opaque environment JSON object and a
comparison key. The runner does not inspect host CPU, memory, or OS details;
it records these controller-provided values unchanged in both `run.json` and
`SUMMARY.md` for the controller's cross-machine comparison policy:

```bash
tools/benchmark/run-sql-benchmark.sh --backend-count 3 \
  --controller-environment '{"machine_pool":"nightly-a","storage":"local-minio"}' \
  --comparison-key nightly-a-release
```

## Explicit Iceberg Config

For Docker-backed Iceberg suites, prefer the generated fixture config:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql/runner/Cargo.toml --bin novarocks-sql-test -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite materialized-view --mode verify
```
