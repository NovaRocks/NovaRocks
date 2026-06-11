# NovaRocks Local Full CI Design

## 背景

NovaRocks 需要一套每天运行一次的本机全量质量验证流程。这个流程偏个人维护
用途，不先接 GitHub Actions，也不依赖外部通知或 Obsidian 写入。第一阶段的
目标是提供一个仓库内可重复执行的脚本入口，后续再由 Codex automation 或其他
本机调度器定时调用。

当前仓库已经具备几个稳定基础：

- Rust 质量入口：`cargo fmt --check`、`cargo clippy`、`cargo build`、
  `cargo test`。
- SQL 回归入口：`tests/sql-test-runner`。
- Docker Iceberg 测试环境：`docker/iceberg-rest/`，可生成 per-worktree 的
  runtime config、standalone-server config 和 SQL runner config。

设计必须遵守当前 NovaRocks 本机环境约定：从
`docker/iceberg-rest/runtime/current/env.sh` 发现端口和配置，不硬编码
standalone-server 端口；后台启动 server 后只以 `NOVAROCKS_READY` marker
作为 readiness 契约。

## 目标

- 提供一个本机可重复运行的 full CI 入口。
- 默认覆盖代码质量、Rust 全量测试、稳定 SQL suite、Iceberg/REST/Spark 相关
  Docker-backed suite。
- 为每次运行生成完整本地日志和摘要，便于失败后追溯。
- 失败处理采用分阶段策略：基础门禁 fail fast，SQL suite 间继续收集结果。
- 运行清理只管理脚本本次创建的进程和 runtime，不影响其他 worktree 或共享
  Docker 服务。
- 保留后续接入 Codex daily automation 的简单入口。

## 非目标

- 不在第一阶段接 GitHub Actions。
- 不实现邮件、Slack、系统通知或 Obsidian 摘要写入。
- 不自动 kill 占用端口的外部进程。
- 不停止或删除共享 Docker Compose 服务和共享 volume。
- 不把实验性 SQL suite 默认纳入 daily。
- 不实现自动重试或失败自动修复。

## 方案

采用分层 shell runner，保持依赖少，并贴合现有 `docker/iceberg-rest/*.sh`
风格。

新增结构：

```text
tools/ci/
  local-full-ci.sh
  lib/
    logging.sh
    command.sh
    server.sh
    sql_suites.sh
  suites/
    stable-sql-suites.txt
logs/
  ci-full/
```

`tools/ci/local-full-ci.sh` 是唯一主入口，负责参数解析、阶段编排、退出码和
summary。`lib/` 下 helper 只承载单一职责：日志目录、命令执行包装、
standalone-server 生命周期、SQL suite 解析和执行。

`logs/ci-full/` 存放本地运行产物。该目录应被 git 忽略，脚本首次运行时可以
创建目录。

## 命令入口

默认 daily 入口：

```bash
tools/ci/local-full-ci.sh
```

手动扩展入口：

```bash
tools/ci/local-full-ci.sh --all-discovered
tools/ci/local-full-ci.sh --suite iceberg-rest --suite optimizer
tools/ci/local-full-ci.sh --skip-cargo-test
tools/ci/local-full-ci.sh --keep-runtime
```

参数语义：

- `--all-discovered`: 从 `sql-tests/*/sql` 自动发现 suite，覆盖稳定清单。
- `--suite <name>`: 只运行指定 SQL suite，可重复传入。
- `--skip-cargo-test`: 调试 runner 时跳过最慢的 `cargo test` 阶段；daily 不用。
- `--keep-runtime`: 失败后保留当前 worktree runtime 方便排查；默认清理本次
  runtime。

参数互斥规则：

- `--all-discovered` 和显式 `--suite` 不同时使用。
- `--skip-cargo-test` 只影响 Rust test 阶段，不跳过 `cargo build`。
- `--keep-runtime` 不改变 server 进程清理策略，server 仍只清理本次启动的 PID。

## 执行流程

### 1. 初始化日志和运行上下文

脚本从仓库根目录运行，或自行解析 repo root 后切换到仓库根目录。

每次运行创建：

```text
logs/ci-full/YYYYmmdd-HHMMSS/
  summary.md
  env.log
  cargo-fmt.log
  cargo-clippy.log
  cargo-build.log
  cargo-test.log
  server.log
  sql/
```

`summary.md` 从运行开始就创建，并在每个阶段结束后追加状态。即使脚本中途
失败，也应保留已完成阶段的状态。

### 2. 准备 Docker Iceberg runtime

执行：

```bash
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh
```

脚本记录以下关键信息到 `env.log` 和 `summary.md`：

- repo path
- branch 和 commit
- `NOVAROCKS_STANDALONE_CONFIG`
- `NOVAROCKS_SQL_TEST_CONFIG`
- `NOVA_ENV_MYSQL_PORT`
- `NOVAROCKS_ICEBERG_REST_URI`
- `NOVAROCKS_SPARK_DEFAULTS`

如果 `up.sh` 或 `source env.sh` 失败，脚本 fail fast，summary 写入
`env.log` 尾部。

### 3. 基础代码门禁

依次执行：

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo build
cargo test
```

这些阶段 fail fast。任一阶段失败时，脚本停止后续 server 和 SQL 阶段，写明
失败阶段、耗时、日志路径和日志尾部。

使用 debug build 作为默认验证模式，符合当前 NovaRocks 本机开发约定。第一版
不引入 release build，避免 nightly 运行时间和资源消耗过高。

### 4. 启动 standalone-server

基础门禁通过后，启动：

```bash
NO_PROXY=127.0.0.1,localhost \
target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$RUN_DIR/server.log" 2>&1 &
```

脚本保存本次启动的 PID，并只管理这个 PID。

readiness 判断只等待：

```text
NOVAROCKS_READY mysql_port=
```

等待过程中，如果进程提前退出，脚本失败并写入 `server.log` 尾部。如果超过
固定超时时间仍未看到 marker，脚本 kill 本次 PID，失败退出。端口探测不能
替代 marker 判断。

### 5. 运行 SQL regression

默认从 `tools/ci/suites/stable-sql-suites.txt` 读取 suite 清单。

显式 `--suite` 时运行用户指定 suite。

`--all-discovered` 时扫描：

```bash
sql-tests/*/sql
```

并把父目录名作为 suite 名。

每个 suite 执行：

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite "$suite" \
  --mode verify
```

SQL suite 之间 continue-on-failure。每个 suite 的 stdout/stderr 写到：

```text
logs/ci-full/YYYYmmdd-HHMMSS/sql/<suite>.log
```

所有 suite 运行结束后，summary 汇总通过和失败列表。只要任一 suite 失败，
最终退出码为非零。

## 默认稳定 SQL suite 清单

第一版稳定清单应覆盖常规查询、优化器、物化视图、Iceberg 和 REST/Spark
路径，但避免自动纳入实验性目录。建议初始清单在实现时根据当前
`sql-tests/` 目录确认后落地，原则如下：

- 普通查询和表达式类 suite：例如 `filter`、`join`、`sort`、`project`、
  `set-op`、`cte`、`decimal`。
- 优化器 golden suite：例如 `optimizer`。
- 物化视图 suite：例如 `materialized-view`。
- Iceberg suite：`iceberg`、`iceberg-ddl`、`iceberg-dml`、`iceberg-rest`、
  `iceberg-compatibility`、`iceberg-ivm`。

如果某个候选 suite 在当前 main 上并不稳定，应暂不放入 stable 清单，并在
`stable-sql-suites.txt` 附近用简短注释说明原因。

## 日志和摘要

`summary.md` 结构：

```markdown
# NovaRocks Local Full CI Summary

- Status: PASS|FAIL
- Started at: <iso8601-start-time>
- Finished at: <iso8601-finish-time>
- Duration: <elapsed-seconds>
- Repo: <absolute-repo-path>
- Branch: <branch-or-detached-head>
- Commit: <short-commit-sha>
- Run dir: <absolute-run-dir>
- Runtime config: <NOVAROCKS_STANDALONE_CONFIG>
- SQL config: <NOVAROCKS_SQL_TEST_CONFIG>
- MySQL port: <NOVA_ENV_MYSQL_PORT>

## Stages

| Stage | Status | Duration | Log |
| --- | --- | --- | --- |

## SQL Suites

| Suite | Status | Duration | Log |
| --- | --- | --- | --- |

## Failure Tail
```

日志文件使用相对 run dir 的路径即可。summary 要能独立判断本次运行是否通过，
不要求用户翻多个日志才能知道失败在哪个阶段。

## 清理策略

脚本注册 `trap`，在退出时执行：

- 如果本次启动了 standalone-server 且 PID 仍存在，kill 该 PID。
- 默认执行 `docker/iceberg-rest/down.sh --runtime-only --purge` 清理当前
  worktree runtime。
- 如果传入 `--keep-runtime`，跳过 runtime purge。

脚本禁止执行：

- `docker/iceberg-rest/down.sh --docker`
- `docker/iceberg-rest/down.sh --docker --volumes`
- kill 非本次启动的进程
- 根据端口占用反查并 kill 进程

如果发现端口被占用或 runtime 异常，脚本应失败并把原因写入 summary，而不是
尝试自动修复。

## 后续 Codex Automation 接入

脚本稳定后，可以创建一个 Codex 本机 automation，每天调用：

```bash
tools/ci/local-full-ci.sh
```

automation prompt 只需要说明：

- 在 NovaRocks repo root 执行本地 full CI。
- 报告 `logs/ci-full/<timestamp>/summary.md` 的结论。
- 不自动清理非本次创建的进程。
- 不自动推送或创建 PR。

调度时间本设计不固定，后续根据用户偏好单独配置。

## 验收标准

- `tools/ci/local-full-ci.sh --help` 展示参数和默认行为。
- 基础门禁失败时 fail fast，summary 标出失败阶段和日志。
- server 等待逻辑依赖 `NOVAROCKS_READY mysql_port=` marker。
- SQL suite 间 continue-on-failure，并在最终 summary 中列出所有失败 suite。
- 退出时只清理本次 server PID 和当前 worktree runtime。
- 默认运行不依赖外部凭据、GitHub Actions、Obsidian 或通知服务。
- `--all-discovered` 能扫描 `sql-tests/*/sql`。
- `--suite` 能只运行指定 suite。
- `--keep-runtime` 能保留当前 worktree runtime 以便排查。

## 风险和缓解

- 全量 daily 时间较长：第一版默认 debug build，并保留 per-suite 日志，先以
  correctness 为主。后续再根据运行时间决定是否拆分阶段或加入缓存。
- Docker/Spark 镜像缺失：`docker/iceberg-rest/up.sh` 已集中处理镜像准备；
  CI runner 只记录 `up.sh` 日志，不重复实现镜像逻辑。
- suite 稳定性不一致：默认 stable 清单必须人工维护，`--all-discovered` 作为
  手动扩展入口。
- 共享 Docker 环境被其他 worktree 使用：脚本只清理当前 runtime，不停止共享
  Docker 服务。
- stale server 进程占端口：脚本不自动 kill 外部进程，而是 fail fast 并记录
  诊断信息，避免误伤用户环境。
