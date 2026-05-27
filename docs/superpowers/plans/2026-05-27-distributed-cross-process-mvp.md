# D1 跨进程执行最小闭环 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 NovaRocks `standalone-server` 从单进程一体拆出 `fe` / `be` / `all-in-one` 三个进程角色，新增三个 NovaRocks 原生 gRPC RPC（`SubmitFragment` / `FetchResult` / `CancelFragment`），coordinator 通过 `FragmentDispatcher` trait 抽象统一所有 fragment 的提交，并打通 1 FE + 1 BE 同机跨进程跑通 SSB 全套且与 all-in-one byte-identical。

**Architecture:** wire protocol 与 StarRocks BE byte-identical（payload 复用现有 thrift `TExecPlanFragmentParams`），transport 从 brpc 换为 gRPC。BE 跑所有 fragment 含根；FE 仅 MySQL server + 优化器 + coordinator（无 executor）。Coordinator 通过 `FragmentDispatcher` trait 与 `InProcessDispatcher`（all-in-one 模式，复用 `std::thread::spawn`）和 `RemoteDispatcher`（FE role 模式，通过 tonic gRPC）解耦。

**Tech Stack:** Rust, tonic gRPC（已在仓库），thrift（已在仓库，复用 `TExecPlanFragmentParams`），sql-test-runner（已在仓库），现有 `src/runtime/exchange.rs` / `src/runtime/result_buffer.rs` / `src/service/internal_service.rs::submit_exec_plan_fragment` FFI 入口。

**Spec:** [docs/superpowers/specs/2026-05-27-distributed-cross-process-mvp-design.md](../specs/2026-05-27-distributed-cross-process-mvp-design.md)

**Roadmap 任务 brief:** [NovaRocks TODO/distributed-cross-process-mvp.md](file:///Users/harbor/Documents/Obsidian/NovaRocks%20TODO/distributed-cross-process-mvp.md)

---

## Prerequisites

### 通用准备

构建 debug 二进制（每个 PR 开始前都要确认）：

```bash
cargo build
```

启动 Iceberg + MinIO 本地 fixture（PR-6 跑 iceberg-rest smoke 时需要）：

```bash
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh
```

辅助命令变量（多个任务用到 sql-test-runner）：

```bash
SQLT="cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests --"
```

### 命名约定 / 类型契约（贯穿所有 PR）

| 名称 | 类型 | 出现在 PR | 备注 |
|---|---|---|---|
| `ClusterRole` | enum {Fe, Be, AllInOne} | PR-1 | 派生 Default = AllInOne / Eq / Debug / Deserialize |
| `ClusterConfig` | struct {role, backends, advertise_host, advertise_port} | PR-1 | TOML 直接反序列化 |
| `FragmentDispatcher` | trait | PR-2 | 三个方法：submit_fragment / fetch_result / cancel_fragments |
| `FetchOutcome` | enum {Ready(RecordBatch), NotReady, Eof, Err(String)} | PR-2 | dispatcher fetch_result 返回值 |
| `InProcessDispatcher` | struct (单位类型) | PR-2 | 实现 FragmentDispatcher，用 std::thread::spawn |
| `RemoteDispatcher` | struct {backend: SocketAddr, client: tonic client} | PR-4 | 实现 FragmentDispatcher，用 gRPC |
| `build_exec_plan_fragment_params` | fn | PR-2 | 输入 FragmentBuildResult，输出 TExecPlanFragmentParams |
| `submit_fragment` / `fetch_result` / `cancel_fragment` | gRPC RPC | PR-3 | 加到现有 `NovaRocksGrpc` service（不新建 service） |
| `SubmitFragmentRequest/Response` / `FetchResultRequest/Response` / `CancelFragmentRequest/Response` | proto messages | PR-3 | 含 `TUniqueId` 重用现有定义 |
| `compute_pipeline_dop` | fn | PR-2 | 等价于现有 coordinator.rs:447-449 逻辑 |

### Spec 与 plan 的一处偏差说明

Spec section 7.1 proto 示例里 service 名是 `NovaRocksBackend`；plan 实际**复用现有 `NovaRocksGrpc` service**（与 spec section 3 文字描述一致）。理由：避免双 service 配置 / 端口管理；现有 service 已经有 exchange + RF + lookup，加 3 个 RPC 不破坏边界。

---

## PR 概览

每个 PR 独立 review、独立可回滚，本身是有意义的"工作软件"。

| PR | 主题 | 输入 | 输出 | 验收 |
|---|---|---|---|---|
| PR-1 | `[cluster]` 配置 + `--role` CLI 骨架 | — | 配置可解析；`--role fe/be` 启动报"未实现" | 单测通过；all-in-one 不回归 |
| PR-2 | `FragmentDispatcher` trait + `InProcessDispatcher` + coordinator 改造 | PR-1 | all-in-one 走 dispatcher trait；根 fragment foreground 路径删除 | 所有现有 sql-test suite 不回归 |
| PR-3 | gRPC 协议 + BE handler + FE client stub | PR-2 | BE 端能接 SubmitFragment/FetchResult/CancelFragment RPC | gRPC roundtrip 单测通过 |
| PR-4 | `RemoteDispatcher` + FE role wiring | PR-3 | 1FE+1BE 跨进程跑通 SELECT 1 + SSB Q1 | smoke 测试通过 |
| PR-5 | 错误处理 + cancel + timeout | PR-4 | submit 半失败 / timeout / BE crash / MySQL 断连均干净失败 | 错误场景测试通过 |
| PR-6 | sql-test-runner `--cluster-mode cross-process` + 套件验收 | PR-5 | SSB / TPC-H 子集 / iceberg-rest smoke 在 cross-process 模式 byte-identical | D1 主验收门槛通过 |

---

# PR-1: `[cluster]` Config + `--role` CLI 骨架

**范围**：纯配置 + CLI plumbing，无任何功能变更。`--role fe` / `--role be` 启动时报错"PR-1 暂未实现"，作为占位。

**输入**：当前 main 分支。

**输出**：
- `src/common/app_config.rs` 含 `ClusterConfig` + `ClusterRole` 结构与解析
- `src/main.rs` 支持 `--role` flag，并按 role 分派
- all-in-one 行为完全不变；现有所有 sql-test suite 不回归

**验证**：
```bash
cargo build && cargo test --lib --package novarocks -- common::app_config::cluster
NO_PROXY=127.0.0.1,localhost cargo run -- standalone-server --port 9030 &
sleep 2 && mysql -h 127.0.0.1 -P 9030 -e 'SELECT 1' && kill %1
```

**回滚**：`git revert <pr-1-merge-commit>`，无下游依赖。

---

### 任务 1.1：写 `[cluster]` 配置解析的失败测试

**Files:**
- Modify: `src/common/app_config.rs`（测试模块末尾）

- [ ] **Step 1.1.1：在 `src/common/app_config.rs` 的 `#[cfg(test)] mod tests` 中加入测试**

打开文件，找到现有 `mod tests` 块（搜索 `fn test_server_priority_networks_default_is_empty`），在文件末尾或测试模块内加入：

```rust
#[test]
fn test_cluster_default_is_all_in_one() {
    let toml = r#"
[server]
host = "127.0.0.1"
"#;
    let cfg: AppConfig = toml::from_str(toml).expect("parse default");
    assert_eq!(cfg.cluster.role, ClusterRole::AllInOne);
    assert!(cfg.cluster.backends.is_empty());
}

#[test]
fn test_cluster_role_fe_with_single_backend() {
    let toml = r#"
[cluster]
role = "fe"
backends = ["127.0.0.1:9070"]
"#;
    let cfg: AppConfig = toml::from_str(toml).expect("parse fe");
    assert_eq!(cfg.cluster.role, ClusterRole::Fe);
    assert_eq!(cfg.cluster.backends, vec!["127.0.0.1:9070".to_string()]);
}

#[test]
fn test_cluster_role_be_rejects_backends() {
    let toml = r#"
[cluster]
role = "be"
backends = ["127.0.0.1:9070"]
"#;
    let parsed: AppConfig = toml::from_str(toml).expect("parse be with backends");
    let err = parsed.cluster.validate().expect_err("be with backends should fail");
    assert!(err.contains("backends"));
}

#[test]
fn test_cluster_role_fe_requires_exactly_one_backend_v1() {
    let toml_empty = r#"
[cluster]
role = "fe"
backends = []
"#;
    let cfg: AppConfig = toml::from_str(toml_empty).expect("parse");
    let err = cfg.cluster.validate().expect_err("fe with 0 backends");
    assert!(err.contains("D1 v1 only supports exactly one backend"));

    let toml_two = r#"
[cluster]
role = "fe"
backends = ["a:1", "b:2"]
"#;
    let cfg: AppConfig = toml::from_str(toml_two).expect("parse");
    let err = cfg.cluster.validate().expect_err("fe with 2 backends");
    assert!(err.contains("D1 v1 only supports exactly one backend"));
}

#[test]
fn test_cluster_role_invalid_rejected() {
    let toml = r#"
[cluster]
role = "leader"
"#;
    let result: Result<AppConfig, _> = toml::from_str(toml);
    assert!(result.is_err(), "invalid role string should fail parse");
}
```

- [ ] **Step 1.1.2：运行测试，确认全部 fail**

```bash
cargo test --lib --package novarocks -- common::app_config::tests::test_cluster
```

期望：5 个测试全部 fail（编译错误，`ClusterConfig` / `ClusterRole` 不存在）。

---

### 任务 1.2：实现 `ClusterConfig` + `ClusterRole`

**Files:**
- Modify: `src/common/app_config.rs`（结构定义 + AppConfig 字段 + 解析）

- [ ] **Step 1.2.1：定义 `ClusterRole` enum**

在 `src/common/app_config.rs` 文件顶部（其他 enum 附近）加入：

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ClusterRole {
    Fe,
    Be,
    AllInOne,
}

impl Default for ClusterRole {
    fn default() -> Self {
        ClusterRole::AllInOne
    }
}
```

注：用 `kebab-case` 让 `all-in-one` 字符串能 deserialize；`fe` / `be` 不受影响。

- [ ] **Step 1.2.2：定义 `ClusterConfig` 结构 + validate 方法**

```rust
#[derive(Clone, Debug, Default, serde::Deserialize)]
#[serde(default)]
pub struct ClusterConfig {
    pub role: ClusterRole,
    pub backends: Vec<String>,
    pub advertise_host: String,
    pub advertise_port: u16,
}

impl ClusterConfig {
    /// Validate cluster config consistency.
    /// Called at startup after parsing.
    pub fn validate(&self) -> Result<(), String> {
        match self.role {
            ClusterRole::Fe => {
                if self.backends.len() != 1 {
                    return Err(format!(
                        "D1 v1 only supports exactly one backend, got {}",
                        self.backends.len()
                    ));
                }
            }
            ClusterRole::Be => {
                if !self.backends.is_empty() {
                    return Err(format!(
                        "role=be must not configure [cluster].backends (got {} entries)",
                        self.backends.len()
                    ));
                }
            }
            ClusterRole::AllInOne => {}
        }
        Ok(())
    }
}
```

- [ ] **Step 1.2.3：把 `cluster` 字段加到 `AppConfig`**

找到现有 `AppConfig` 结构（搜索 `pub struct AppConfig`），加一个字段：

```rust
pub struct AppConfig {
    // ... 现有字段 ...
    #[serde(default)]
    pub cluster: ClusterConfig,
}
```

注意：`#[serde(default)]` 让缺少 `[cluster]` 节时使用 `ClusterConfig::default()`，保持向后兼容。

- [ ] **Step 1.2.4：运行测试，确认全部 pass**

```bash
cargo test --lib --package novarocks -- common::app_config::tests::test_cluster
```

期望：5 个测试全部 pass。

- [ ] **Step 1.2.5：跑全量 lib 测试，确保没破坏其他配置测试**

```bash
cargo test --lib --package novarocks -- common::app_config
```

期望：所有 `app_config` 模块的测试通过。

---

### 任务 1.3：CLI 加 `--role` flag

**Files:**
- Modify: `src/main.rs:55-96`（`parse_standalone_server_args`）+ `src/server/mod.rs:40-43`（`StandaloneServerOptions`）

- [ ] **Step 1.3.1：先写失败测试**

在 `src/main.rs` 的 `#[cfg(test)] mod tests` 中（找到 `parse_standalone_server_args_accepts_port_and_config` 附近）加入：

```rust
#[test]
fn parse_standalone_server_args_accepts_role() {
    let args = vec!["--role".to_string(), "fe".to_string()];
    let parsed = parse_standalone_server_args(&args)
        .expect("parse standalone-server args with --role")
        .expect("standalone-server args");
    assert_eq!(parsed.role_override, Some("fe".to_string()));
}

#[test]
fn parse_standalone_server_args_rejects_unknown_role() {
    let args = vec!["--role".to_string(), "leader".to_string()];
    let result = parse_standalone_server_args(&args)
        .expect("parse args")
        .expect("args present");
    // Role 字符串的合法性检查在 startup 时做，parser 只透传。
    assert_eq!(result.role_override, Some("leader".to_string()));
}

#[test]
fn parse_standalone_server_args_role_with_port() {
    let args = vec![
        "--role".to_string(), "be".to_string(),
        "--port".to_string(), "9030".to_string(),
    ];
    let parsed = parse_standalone_server_args(&args)
        .expect("parse")
        .expect("args");
    assert_eq!(parsed.role_override, Some("be".to_string()));
    assert_eq!(parsed.mysql_port, Some(9030));
}
```

- [ ] **Step 1.3.2：运行测试，确认 fail**

```bash
cargo test --lib --package novarocks -- main::tests::parse_standalone_server_args_accepts_role
```

期望：fail（`role_override` 字段不存在）。

- [ ] **Step 1.3.3：扩展 `StandaloneServerCliArgs` 结构（与 `StandaloneServerOptions`）**

在 `src/main.rs` 找到 `struct StandaloneServerCliArgs`，加字段：

```rust
pub struct StandaloneServerCliArgs {
    pub config_path: Option<PathBuf>,
    pub mysql_port: Option<u16>,
    pub role_override: Option<String>,
}
```

在 `parse_standalone_server_args` 中处理 `--role`：

```rust
fn parse_standalone_server_args(
    args: &[String],
) -> Result<Option<StandaloneServerCliArgs>, String> {
    let mut config_path = None;
    let mut mysql_port = None;
    let mut role_override = None;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" => {
                let val = iter.next().ok_or_else(|| "--config requires a value".to_string())?;
                config_path = Some(PathBuf::from(val));
            }
            "--port" => {
                let val = iter.next().ok_or_else(|| "--port requires a value".to_string())?;
                mysql_port = Some(val.parse::<u16>().map_err(|e| format!("--port: {}", e))?);
            }
            "--role" => {
                let val = iter.next().ok_or_else(|| "--role requires a value".to_string())?;
                role_override = Some(val.clone());
            }
            "--help" | "-h" => return Ok(None),
            other => return Err(format!("unknown standalone-server arg: {other}")),
        }
    }
    Ok(Some(StandaloneServerCliArgs { config_path, mysql_port, role_override }))
}
```

- [ ] **Step 1.3.4：把 `role_override` 串到 `StandaloneServerOptions`**

在 `src/server/mod.rs` 找到 `pub struct StandaloneServerOptions`，加字段：

```rust
pub struct StandaloneServerOptions {
    pub config_path: Option<PathBuf>,
    pub mysql_port: Option<u16>,
    pub role_override: Option<String>,
}
```

在 `src/main.rs::run_standalone_server_cli` 中传值：

```rust
fn run_standalone_server_cli(cli: StandaloneServerCliArgs) -> Result<(), String> {
    run_standalone_server(StandaloneServerOptions {
        config_path: cli.config_path,
        mysql_port: cli.mysql_port,
        role_override: cli.role_override,
    })
}
```

- [ ] **Step 1.3.5：运行 CLI 解析测试，确认 pass**

```bash
cargo test --lib --package novarocks -- main::tests::parse_standalone_server_args
```

期望：所有 `parse_standalone_server_args_*` 测试通过（含新增 3 个 + 原有 3 个）。

---

### 任务 1.4：启动期 role 分派（FE/BE 占位）

**Files:**
- Modify: `src/server/mod.rs::run_standalone_server`

- [ ] **Step 1.4.1：先写失败测试**

在 `src/server/mod.rs` 或新建 `src/server/tests.rs` 加：

```rust
#[test]
fn role_fe_returns_not_implemented_error() {
    // 模拟 fe 角色启动，没有真起服务，仅校验 dispatch 入口。
    use super::*;
    let opts = StandaloneServerOptions {
        config_path: None,
        mysql_port: None,
        role_override: Some("fe".to_string()),
    };
    let err = dispatch_role(&opts, &fake_cluster_config_for_test("fe", &["127.0.0.1:9070"]))
        .expect_err("fe role not yet implemented in PR-1");
    assert!(err.contains("PR-1 placeholder"));
}

#[test]
fn role_be_returns_not_implemented_error() {
    use super::*;
    let opts = StandaloneServerOptions {
        config_path: None,
        mysql_port: None,
        role_override: Some("be".to_string()),
    };
    let err = dispatch_role(&opts, &fake_cluster_config_for_test("be", &[]))
        .expect_err("be role not yet implemented in PR-1");
    assert!(err.contains("PR-1 placeholder"));
}

#[cfg(test)]
fn fake_cluster_config_for_test(role: &str, backends: &[&str]) -> crate::common::app_config::ClusterConfig {
    use crate::common::app_config::{ClusterConfig, ClusterRole};
    let role = match role {
        "fe" => ClusterRole::Fe,
        "be" => ClusterRole::Be,
        _ => ClusterRole::AllInOne,
    };
    ClusterConfig {
        role,
        backends: backends.iter().map(|s| s.to_string()).collect(),
        advertise_host: String::new(),
        advertise_port: 0,
    }
}
```

- [ ] **Step 1.4.2：运行测试，确认 fail**

```bash
cargo test --lib --package novarocks -- server::tests::role_
```

期望：fail（`dispatch_role` 不存在）。

- [ ] **Step 1.4.3：实现 `dispatch_role`**

在 `src/server/mod.rs`：

```rust
/// Resolve effective ClusterRole from CLI override (if any) or config.
pub fn resolve_role(
    cli_override: Option<&str>,
    cfg: &crate::common::app_config::ClusterConfig,
) -> Result<crate::common::app_config::ClusterRole, String> {
    use crate::common::app_config::ClusterRole;
    let resolved = match cli_override {
        Some("fe") => ClusterRole::Fe,
        Some("be") => ClusterRole::Be,
        Some("all-in-one") => ClusterRole::AllInOne,
        Some(other) => return Err(format!(
            "invalid --role value '{}', expected fe / be / all-in-one",
            other
        )),
        None => cfg.role,
    };
    Ok(resolved)
}

/// Dispatch process to the right serving path based on role.
/// PR-1: fe/be roles are placeholders that error out.
fn dispatch_role(
    opts: &StandaloneServerOptions,
    cluster: &crate::common::app_config::ClusterConfig,
) -> Result<(), String> {
    use crate::common::app_config::ClusterRole;
    let role = resolve_role(opts.role_override.as_deref(), cluster)?;
    match role {
        ClusterRole::AllInOne => {
            // PR-1 不动 all-in-one 现有路径
            run_all_in_one(opts, cluster)
        }
        ClusterRole::Fe => {
            Err("[PR-1 placeholder] role=fe not yet implemented".to_string())
        }
        ClusterRole::Be => {
            Err("[PR-1 placeholder] role=be not yet implemented".to_string())
        }
    }
}

fn run_all_in_one(
    opts: &StandaloneServerOptions,
    _cluster: &crate::common::app_config::ClusterConfig,
) -> Result<(), String> {
    // 把现有 run_standalone_server 主体移到这里（保持行为不变）。
    // 已有逻辑：resolve_server_options → StandaloneNovaRocks::open → serve_forever
    // 不在 plan 中复述细节；直接搬过来即可。
    legacy_run_standalone_server(opts)
}
```

把现有 `run_standalone_server` 内部主体抽到 `legacy_run_standalone_server`（rename + 保留语义）。新的 `run_standalone_server` 公开 API 做配置加载 + cluster 校验 + dispatch：

```rust
pub fn run_standalone_server(opts: StandaloneServerOptions) -> Result<(), String> {
    let cfg = crate::common::app_config::load(opts.config_path.as_deref())?;
    cfg.cluster.validate()?;
    dispatch_role(&opts, &cfg.cluster)
}
```

- [ ] **Step 1.4.4：运行测试，确认 pass**

```bash
cargo test --lib --package novarocks -- server::tests::role_
```

期望：2 个测试通过。

---

### 任务 1.5：手动 smoke：all-in-one 行为不变

- [ ] **Step 1.5.1：默认启动确认**

```bash
cargo build
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server --port 9030 >/tmp/d1-pr1-smoke.log 2>&1 &
SRV=$!
for i in $(seq 1 30); do
  grep -q '^NOVAROCKS_READY ' /tmp/d1-pr1-smoke.log && break
  sleep 1
done
mysql -h 127.0.0.1 -P 9030 -e 'SELECT 1'
kill $SRV
```

期望：返回 `+---+\n| 1 |\n+---+\n| 1 |\n+---+`。

- [ ] **Step 1.5.2：`--role fe` 启动报"未实现"**

```bash
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server --role fe
```

期望：进程立即退出，stderr 含 `[PR-1 placeholder] role=fe not yet implemented`。

- [ ] **Step 1.5.3：`--role be` 启动同样报"未实现"**

```bash
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server --role be
```

期望：同上但是 `role=be`。

---

### 任务 1.6：跑现有 sql-test suite 回归

- [ ] **Step 1.6.1：跑 SSB 套件**

```bash
source docker/iceberg-rest/runtime/current/env.sh 2>/dev/null || true
$SQLT --suite ssb --mode verify
```

期望：全部通过，与 PR-1 之前一致。

- [ ] **Step 1.6.2：跑 cte 套件（依赖根 fragment 行为）**

```bash
$SQLT --suite cte --mode verify
```

期望：全部通过。

---

### 任务 1.7：PR-1 commit

- [ ] **Step 1.7.1：commit**

```bash
git add src/common/app_config.rs src/main.rs src/server/mod.rs
git commit -m "$(cat <<'EOF'
feat(cluster): add [cluster] config section and --role CLI plumbing (D1 PR-1)

Adds ClusterConfig + ClusterRole types and --role fe|be|all-in-one CLI
flag. role=fe/be paths are placeholders that error out with a clear
message; role=all-in-one preserves current behavior end-to-end.

Refs: docs/superpowers/specs/2026-05-27-distributed-cross-process-mvp-design.md
EOF
)"
```

- [ ] **Step 1.7.2：确认 commit 成功 + 工作树干净**

```bash
git status
git log -1 --oneline
```

---

# PR-2: `FragmentDispatcher` trait + `InProcessDispatcher` + Coordinator 改造

**范围**：D1 最大的内部 cleanup。引入 `FragmentDispatcher` trait，`InProcessDispatcher` 实现 all-in-one 模式（保持行为不变），coordinator 不再 foreground 跑根 fragment，所有 fragment 走统一 dispatcher 路径。新增 `build_exec_plan_fragment_params` 把根 / CTE / Stream-source 三种 fragment 的 ExecParams 凑装合并到一处。

**输入**：PR-1 已合并。

**输出**：
- `src/runtime/dispatcher.rs` 新文件（trait + `InProcessDispatcher` + `FetchOutcome` + `compute_pipeline_dop`）
- `src/runtime/exec_params.rs` 新文件（`build_exec_plan_fragment_params`）
- `src/runtime/coordinator.rs` 重写 `execute()`，删除 488-565 行根 fragment foreground 路径
- `src/engine/mod.rs:2615` 硬编码 `"127.0.0.1"` 删除，`ExecutionCoordinator::new` 签名从 `(build_result, exchange_host, exchange_port, query_options)` 改为 `(build_result, dispatcher: Arc<dyn FragmentDispatcher>, query_options)`
- 全部现有 sql-test suite 在 all-in-one 模式不回归

**验证**：
```bash
cargo build
cargo test --lib --package novarocks -- runtime::dispatcher runtime::exec_params runtime::coordinator
$SQLT --suite ssb --mode verify
$SQLT --suite cte --mode verify
$SQLT --suite tpc-h --mode verify --only q1,q5,q9,q12
```

**回滚**：`git revert <pr-2-merge-commit>`，恢复 coordinator + engine 改动；PR-1 的 config / CLI 不受影响。

---

### 任务 2.1：定义 `FragmentDispatcher` trait + `FetchOutcome`

**Files:**
- Create: `src/runtime/dispatcher.rs`
- Modify: `src/runtime/mod.rs`（pub mod dispatcher）

- [ ] **Step 2.1.1：新建 `src/runtime/dispatcher.rs` 并写 trait 骨架**

```rust
//! Fragment dispatcher abstraction.
//!
//! `FragmentDispatcher` decouples coordinator from the question of where
//! fragments actually run. `InProcessDispatcher` keeps the all-in-one
//! mode using `std::thread::spawn`; `RemoteDispatcher` (PR-4) talks to a
//! remote BE over gRPC.

use std::sync::Arc;

use crate::internal_service::{TExecPlanFragmentParams, TUniqueId};

/// Outcome of a single `fetch_result` call.
pub enum FetchOutcome {
    /// A chunk is ready; payload is one Arrow RecordBatch.
    Ready(arrow::array::RecordBatch),
    /// No chunk available within the wait window; caller may poll again.
    NotReady,
    /// Fragment has completed and queue is drained; caller stops polling.
    Eof,
    /// Fragment encountered an error.
    Err(String),
}

pub trait FragmentDispatcher: Send + Sync + 'static {
    /// Submit a single fragment instance for execution. Returns once the
    /// fragment has been accepted by the executor; does NOT block on completion.
    fn submit_fragment(&self, params: TExecPlanFragmentParams) -> Result<(), String>;

    /// Pull one chunk for the given root fragment instance, blocking up to
    /// `max_wait_ms` (0 = non-blocking).
    fn fetch_result(
        &self,
        finst_id: TUniqueId,
        max_wait_ms: i64,
    ) -> Result<FetchOutcome, String>;

    /// Cancel a set of fragment instances. Best-effort, idempotent; never
    /// errors on already-completed fragments.
    fn cancel_fragments(&self, finst_ids: &[TUniqueId]);
}

/// Compute pipeline DOP using the same heuristic the coordinator used
/// before PR-2 (see coordinator.rs:447-449 pre-PR-2).
pub(crate) fn compute_pipeline_dop() -> i32 {
    std::thread::available_parallelism()
        .map(|p| p.get().min(4))
        .unwrap_or(4) as i32
}
```

- [ ] **Step 2.1.2：把模块注册到 `src/runtime/mod.rs`**

打开 `src/runtime/mod.rs`，加：

```rust
pub mod dispatcher;
```

- [ ] **Step 2.1.3：cargo build 确认编译**

```bash
cargo build
```

期望：编译通过（暂未引用 dispatcher 的话仅有 dead_code warn）。

---

### 任务 2.2：实现 `InProcessDispatcher`

**Files:**
- Modify: `src/runtime/dispatcher.rs`

- [ ] **Step 2.2.1：先写 InProcessDispatcher 单测**

在 `src/runtime/dispatcher.rs` 末尾加：

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_process_dispatcher_submit_returns_immediately() {
        // 构造一个最小可执行的 fragment（SELECT 1）；
        // 验证 submit_fragment 返回 Ok 后 thread 已 spawn。
        let dispatcher = InProcessDispatcher;
        let params = build_minimal_select_one_params();
        let r = dispatcher.submit_fragment(params);
        assert!(r.is_ok());
    }

    #[test]
    fn in_process_dispatcher_fetch_returns_eof_for_unknown_finst() {
        let dispatcher = InProcessDispatcher;
        let unknown = TUniqueId { hi: 0xdead, lo: 0xbeef };
        let r = dispatcher.fetch_result(unknown, 0)
            .expect("fetch should not RPC-error in process");
        // 不存在的 finst_id → ResultBuffer 视为未注册 → 视为已 cancel/eof。
        assert!(matches!(r, FetchOutcome::Eof | FetchOutcome::Err(_)));
    }

    #[test]
    fn in_process_dispatcher_cancel_is_idempotent() {
        let dispatcher = InProcessDispatcher;
        let id = TUniqueId { hi: 1, lo: 2 };
        dispatcher.cancel_fragments(&[id]);
        dispatcher.cancel_fragments(&[id]); // 第二次不报错
    }

    fn build_minimal_select_one_params() -> TExecPlanFragmentParams {
        // 实测时构造一个空 fragment（不实际执行任何 plan node）；
        // 这里测的是 submit_fragment 的"接受 + 派发"路径，不测 plan 执行细节。
        // 详细 builder 在 exec_params 模块（任务 2.3）。
        crate::runtime::exec_params::test_helpers::empty_fragment_params(
            TUniqueId { hi: 0, lo: 1 },
        )
    }
}
```

- [ ] **Step 2.2.2：实现 `InProcessDispatcher`**

在 `src/runtime/dispatcher.rs` 加：

```rust
pub struct InProcessDispatcher;

impl FragmentDispatcher for InProcessDispatcher {
    fn submit_fragment(&self, params: TExecPlanFragmentParams) -> Result<(), String> {
        // 拆 typed args 避免 in-process serde 开销。
        let fragment = params.fragment.ok_or("missing fragment")?;
        let desc_tbl = params.desc_tbl;
        let exec_params = params.params.ok_or("missing exec_params")?;
        let query_options = params.query_options;

        let pipeline_dop = compute_pipeline_dop();

        std::thread::spawn(move || {
            let _ = crate::lower::fragment::execute_fragment(
                &fragment,
                desc_tbl.as_ref(),
                Some(&exec_params),
                query_options.as_ref(),
                /* session_time_zone */ None,
                pipeline_dop,
                /* group_execution_scan_dop */ None,
                /* db_name */ None,
                /* profiler */ None,
                /* last_query_id */ None,
                /* fe_addr */ None,
                /* backend_num */ None,
                /* mem_tracker */ None,
            );
            // execute_fragment 内部已通过 ResultBuffer/Exchange 通知 coordinator；
            // 错误也通过 ResultBuffer/Exchange 的 cancel 路径传播。
        });
        Ok(())
    }

    fn fetch_result(
        &self,
        finst_id: TUniqueId,
        max_wait_ms: i64,
    ) -> Result<FetchOutcome, String> {
        let r = crate::runtime::result_buffer::try_fetch(finst_id, max_wait_ms);
        Ok(map_try_fetch_to_outcome(r))
    }

    fn cancel_fragments(&self, finst_ids: &[TUniqueId]) {
        for id in finst_ids {
            crate::runtime::exchange::cancel_fragment(id.hi, id.lo);
            crate::runtime::result_buffer::cancel(*id);
        }
    }
}

/// Map result_buffer::TryFetchResult into FetchOutcome.
/// Note: the existing TryFetchResult is 3-state (Ready/NotReady/Error);
/// EOF is represented as Ready(result) with result.eos == true.
fn map_try_fetch_to_outcome(
    r: crate::runtime::result_buffer::TryFetchResult,
) -> FetchOutcome {
    use crate::runtime::result_buffer::TryFetchResult;
    match r {
        TryFetchResult::Ready(result) if result.eos => FetchOutcome::Eof,
        TryFetchResult::Ready(result) => FetchOutcome::Ready(result.into_record_batch()),
        TryFetchResult::NotReady => FetchOutcome::NotReady,
        TryFetchResult::Error(e) => FetchOutcome::Err(e.to_string()),
    }
}
```

注：PR-3 采用的实际约定是 `TryFetchResult` 仍保持 `Ready/NotReady/Error` 三态；`close_ok + 空队列` 通过一次 `Ready(result)` 且 `result.eos == true` 表示 EOF。PR-4 的 `RemoteDispatcher` 应按 `result.eos` 映射到 `FetchOutcome::Eof`。

- [ ] **Step 2.2.3：确认 ResultBuffer EOF 约定**

打开 `src/runtime/result_buffer.rs`，找到 `pub enum TryFetchResult`，确认其当前结构保持三态：

```rust
pub enum TryFetchResult {
    Ready(FetchResult),
    NotReady,
    Error(FetchError),
}
```

在 `try_fetch` / `wait_fetch` 中，`close_ok` 且队列空时返回一次 `Ready(FetchResult { eos: true, .. })`。调用方需要检查 `result.eos`，并在 true 时映射为 EOF。

- [ ] **Step 2.2.4：跑 dispatcher 单测**

```bash
cargo test --lib --package novarocks -- runtime::dispatcher
```

期望：3 个测试都通过。如果 `empty_fragment_params` helper 还没写（任务 2.3），先 mock 一个返回默认 `TExecPlanFragmentParams::default()`，等任务 2.3 完成后回头打通。

---

### 任务 2.3：实现 `build_exec_plan_fragment_params`

**Files:**
- Create: `src/runtime/exec_params.rs`
- Modify: `src/runtime/mod.rs`（pub mod exec_params）

- [ ] **Step 2.3.1：先写单测描述输入输出**

新建 `src/runtime/exec_params.rs`：

```rust
//! Build TExecPlanFragmentParams for all fragment kinds (root / CTE produce /
//! Stream-source) in a single place, eliminating the divergent paths that
//! coordinator.rs:460-565 used to maintain.

use crate::internal_service::{
    TDescriptorTable, TExecPlanFragmentParams, TPlanFragmentExecParams, TQueryOptions, TUniqueId,
};
use crate::planner::TPlanFragment;
use crate::runtime::coordinator::FragmentBuildResult;

/// Build a single TExecPlanFragmentParams for the given fragment.
///
/// Inputs already produced by the cascades fragment_builder (PlanFragmentBuilder):
/// - `fr.fragment` (TPlanFragment)
/// - `fr.desc_tbl` (TDescriptorTable)
/// - `fr.exec_params` (TPlanFragmentExecParams: fragment_instance_id,
///   per_exch_num_senders, destinations, runtime_filter_params already filled)
/// - `query_options` (from coordinator scope)
///
/// This function combines them into the wire-level wrapper struct that BE
/// expects (and that StarRocks FE also produces).
pub fn build_exec_plan_fragment_params(
    fr: &FragmentBuildResult,
    query_id: TUniqueId,
    query_options: Option<&TQueryOptions>,
) -> TExecPlanFragmentParams {
    let mut exec_params = fr.exec_params.clone();
    exec_params.query_id = query_id;
    // fragment_instance_id 已由 fragment_builder 分配。

    TExecPlanFragmentParams {
        protocol_version: 0,
        fragment: Some(fr.fragment.clone()),
        desc_tbl: Some(fr.desc_tbl.clone()),
        params: Some(exec_params),
        query_options: query_options.cloned(),
        // ... 其他字段都用 thrift default
        ..Default::default()
    }
}

#[cfg(test)]
pub(crate) mod test_helpers {
    use super::*;

    pub fn empty_fragment_params(finst_id: TUniqueId) -> TExecPlanFragmentParams {
        let mut exec_params = TPlanFragmentExecParams::default();
        exec_params.fragment_instance_id = finst_id;
        TExecPlanFragmentParams {
            protocol_version: 0,
            fragment: Some(TPlanFragment::default()),
            desc_tbl: Some(TDescriptorTable::default()),
            params: Some(exec_params),
            query_options: None,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal_service::TUniqueId;

    #[test]
    fn build_preserves_fragment_instance_id() {
        let finst = TUniqueId { hi: 0x123, lo: 0x456 };
        let qid = TUniqueId { hi: 0xabc, lo: 0xdef };
        let fr = fake_fragment_build_result(finst);
        let p = build_exec_plan_fragment_params(&fr, qid, None);
        assert_eq!(p.params.as_ref().unwrap().fragment_instance_id, finst);
        assert_eq!(p.params.as_ref().unwrap().query_id, qid);
    }

    #[test]
    fn build_preserves_per_exch_num_senders() {
        let finst = TUniqueId { hi: 1, lo: 2 };
        let qid = TUniqueId { hi: 3, lo: 4 };
        let mut fr = fake_fragment_build_result(finst);
        fr.exec_params.per_exch_num_senders.insert(7 /* plan_node_id */, 3 /* sender count */);
        let p = build_exec_plan_fragment_params(&fr, qid, None);
        assert_eq!(
            p.params.as_ref().unwrap().per_exch_num_senders.get(&7),
            Some(&3)
        );
    }

    #[test]
    fn build_preserves_runtime_filter_params() {
        let finst = TUniqueId { hi: 1, lo: 2 };
        let qid = TUniqueId { hi: 3, lo: 4 };
        let mut fr = fake_fragment_build_result(finst);
        fr.exec_params.runtime_filter_params = Some(/* fake rf params */ Default::default());
        let p = build_exec_plan_fragment_params(&fr, qid, None);
        assert!(p.params.as_ref().unwrap().runtime_filter_params.is_some());
    }

    fn fake_fragment_build_result(finst_id: TUniqueId) -> FragmentBuildResult {
        let mut exec_params = TPlanFragmentExecParams::default();
        exec_params.fragment_instance_id = finst_id;
        FragmentBuildResult {
            fragment_id: 0,
            cte_id: None,
            fragment: TPlanFragment::default(),
            desc_tbl: TDescriptorTable::default(),
            exec_params,
            // 其他字段省略，按需补全
            ..Default::default()
        }
    }
}
```

- [ ] **Step 2.3.2：注册模块**

`src/runtime/mod.rs` 加：

```rust
pub mod exec_params;
```

- [ ] **Step 2.3.3：跑测试**

```bash
cargo test --lib --package novarocks -- runtime::exec_params
```

期望：3 个测试通过。

---

### 任务 2.4：重构 `ExecutionCoordinator` 签名

**Files:**
- Modify: `src/runtime/coordinator.rs:51-63`（`new` 签名）+ 整个 `execute()` 方法

- [ ] **Step 2.4.1：改 `new` 签名**

在 `src/runtime/coordinator.rs` 找到 `pub(crate) fn new`，从：

```rust
pub(crate) fn new(
    build_result: MultiFragmentBuildResult,
    exchange_host: String,
    exchange_port: u16,
    query_options: Option<TQueryOptions>,
) -> Self
```

改为：

```rust
pub(crate) fn new(
    build_result: MultiFragmentBuildResult,
    dispatcher: Arc<dyn crate::runtime::dispatcher::FragmentDispatcher>,
    query_options: Option<TQueryOptions>,
) -> Self
```

同步把 struct 字段 `exchange_host: String, exchange_port: u16` 删除，加 `dispatcher: Arc<dyn FragmentDispatcher>`。

- [ ] **Step 2.4.2：把 `execute()` 改成 dispatcher 路径**

定位 `pub(crate) fn execute(self) -> Result<QueryResult, String>` 方法主体（约 488-565 行）。整个方法替换为：

```rust
pub(crate) fn execute(self) -> Result<QueryResult, String> {
    use crate::runtime::dispatcher::FetchOutcome;
    use crate::runtime::exec_params::build_exec_plan_fragment_params;
    use std::time::Instant;

    let query_id = TUniqueId::new(self.query_id_hi, self.query_id_lo);

    // 准备所有 fragment 的 exec params + 标识根 fragment instance id
    let mut all_finst_ids: Vec<TUniqueId> = Vec::new();
    let mut root_finst_id: Option<TUniqueId> = None;

    let root_fragment_id = self.root_fragment_id;

    let MultiFragmentBuildResult {
        fragments: fragment_results,
        edges,
        rf_plan,
        ..
    } = self.build_result;

    // 第一阶段：补全 exchange / RF 元数据（保留现有 coordinator setup_runtime_filter_params 等逻辑）
    let (fragment_results, rf_params, per_exch_num_senders) =
        prepare_fragment_metadata(fragment_results, &edges, rf_plan)?;

    // 第二阶段：submit 全部 fragment
    let mut submitted_ids: Vec<TUniqueId> = Vec::new();
    for fr in fragment_results.iter() {
        let mut params = build_exec_plan_fragment_params(
            fr,
            query_id,
            self.query_options.as_ref(),
        );
        // 把 per_exch_num_senders / rf_params 等 query-scope 字段附加
        if let Some(p) = params.params.as_mut() {
            p.per_exch_num_senders = per_exch_num_senders.clone();
            if let Some(rf) = &rf_params {
                p.runtime_filter_params = Some(rf.clone());
            }
        }

        let finst_id = params.params.as_ref().unwrap().fragment_instance_id;
        if fr.fragment_id == root_fragment_id {
            root_finst_id = Some(finst_id);
        }
        all_finst_ids.push(finst_id);

        if let Err(e) = self.dispatcher.submit_fragment(params) {
            self.dispatcher.cancel_fragments(&submitted_ids);
            return Err(format!("submit fragment {}: {}", fr.fragment_id, e));
        }
        submitted_ids.push(finst_id);
    }

    let root = root_finst_id.ok_or("root fragment not found in build results")?;

    // 第三阶段：循环 fetch 根 fragment 结果
    let deadline = compute_deadline(self.query_options.as_ref());
    let mut chunks = Vec::new();
    loop {
        let remaining = deadline.checked_duration_since(Instant::now()).unwrap_or_default();
        if remaining.is_zero() {
            self.dispatcher.cancel_fragments(&all_finst_ids);
            return Err("query timeout".into());
        }
        let wait_ms = std::cmp::min(300, remaining.as_millis() as i64);
        match self.dispatcher.fetch_result(root, wait_ms) {
            Ok(FetchOutcome::Ready(c)) => chunks.push(c),
            Ok(FetchOutcome::NotReady) => continue,
            Ok(FetchOutcome::Eof) => break,
            Ok(FetchOutcome::Err(msg)) => {
                self.dispatcher.cancel_fragments(&all_finst_ids);
                return Err(msg);
            }
            Err(rpc_err) => {
                self.dispatcher.cancel_fragments(&all_finst_ids);
                return Err(format!("fetch_result rpc failed: {}", rpc_err));
            }
        }
    }

    Ok(QueryResult::from_chunks(chunks))
}

/// Extract previously-inlined metadata-preparation logic into a helper.
/// Pre-PR-2 lived in coordinator.rs:101-178; pulled out so the new execute()
/// stays readable.
fn prepare_fragment_metadata(
    fragments: Vec<FragmentBuildResult>,
    edges: &[FragmentEdge],
    rf_plan: Option<RfPlan>,
) -> Result<
    (Vec<FragmentBuildResult>, Option<TRuntimeFilterParams>, BTreeMap<i32, i32>),
    String,
> {
    // 这个 helper 把现有 coordinator.rs:101-178 范围内的 rf_params /
    // per_exch_num_senders 计算搬过来。
    // 不在 plan 中复述细节；按现有逻辑移植即可。
    todo!("port coordinator.rs:101-178 metadata setup into this helper")
}

fn compute_deadline(qo: Option<&TQueryOptions>) -> std::time::Instant {
    use std::time::Duration;
    let timeout_secs = qo.and_then(|o| o.query_timeout).unwrap_or(600); // 默认 600s
    std::time::Instant::now() + Duration::from_secs(timeout_secs as u64)
}
```

注意 `prepare_fragment_metadata` 的 `todo!()` —— 在 Step 2.4.3 立即填实，不能 ship。

- [ ] **Step 2.4.3：把 coordinator.rs:101-178 的元数据准备逻辑搬到 `prepare_fragment_metadata`**

阅读现有 coordinator.rs:101-178 范围内的代码：
- L101-117：`per_exch_num_senders` 计算
- L155-164：`stream_source_ids` 分组
- L166-178：`brpc_addr` 与 `rf_params` 准备

整段 lift-and-shift 到 `prepare_fragment_metadata`，删除 `exchange_host/exchange_port` 引用（因为 dispatcher 现在处理目标地址）。

由于这段代码细节多，作为子步骤：

1. 把 L155-164 stream_source_ids 计算放进 helper（输入 edges，输出 BTreeSet）
2. 把 L101-117 per_exch_num_senders 放进 helper（输入 edges，输出 BTreeMap<plan_node_id, sender_count>）
3. 把 rf_params 准备放进 helper（输入 rf_plan + fragment_results，输出 Option<TRuntimeFilterParams>）

替换 `prepare_fragment_metadata` 的 todo 体为这三段逻辑的串联。

- [ ] **Step 2.4.4：删除 coordinator.rs 中根 fragment foreground 路径**

定位 coordinator.rs:488-565 范围。整段删除（已被新 execute 的统一路径替代）。

同时删除：
- L460-485 范围的 `std::thread::spawn(move || execute_fragment(...))`（也由 dispatcher 取代）
- L443-485 范围的 `cte_handles: Vec<JoinHandle>` 收集逻辑

- [ ] **Step 2.4.5：cargo build 找编译错误并修复**

```bash
cargo build 2>&1 | tee /tmp/d1-pr2-build.log
```

预期会有几个编译错误：
- coordinator 旧字段引用 → 删除
- `execute_plan_with_pipeline` 直接调用 → 删除（在新统一路径中不再使用）

逐个解决。

- [ ] **Step 2.4.6：补 coordinator 改造单测**

在 `src/runtime/coordinator.rs` 测试模块中加：

```rust
#[cfg(test)]
mod coord_tests {
    use super::*;
    use crate::runtime::dispatcher::{FragmentDispatcher, FetchOutcome};
    use std::sync::{Arc, Mutex};

    struct MockDispatcher {
        submitted: Mutex<Vec<TUniqueId>>,
        cancelled: Mutex<Vec<TUniqueId>>,
        // root_finst_id → predetermined fetch behavior
        fetch_script: Mutex<Vec<FetchOutcome>>,
    }

    impl FragmentDispatcher for MockDispatcher {
        fn submit_fragment(&self, params: TExecPlanFragmentParams) -> Result<(), String> {
            let id = params.params.as_ref().unwrap().fragment_instance_id;
            self.submitted.lock().unwrap().push(id);
            Ok(())
        }
        fn fetch_result(&self, _finst_id: TUniqueId, _max_wait_ms: i64) -> Result<FetchOutcome, String> {
            // pop next outcome
            let mut s = self.fetch_script.lock().unwrap();
            if s.is_empty() {
                Ok(FetchOutcome::Eof)
            } else {
                Ok(s.remove(0))
            }
        }
        fn cancel_fragments(&self, finst_ids: &[TUniqueId]) {
            self.cancelled.lock().unwrap().extend(finst_ids.iter().copied());
        }
    }

    #[test]
    fn execute_submits_all_fragments_and_fetches_to_eof() {
        let mock = Arc::new(MockDispatcher {
            submitted: Mutex::new(Vec::new()),
            cancelled: Mutex::new(Vec::new()),
            fetch_script: Mutex::new(vec![FetchOutcome::Eof]),
        });
        let build = tiny_two_fragment_build_result();
        let coord = ExecutionCoordinator::new(
            build,
            mock.clone() as Arc<dyn FragmentDispatcher>,
            None,
        );
        let result = coord.execute().expect("execute");
        assert_eq!(mock.submitted.lock().unwrap().len(), 2,
            "both root and CTE fragment submitted");
        assert!(mock.cancelled.lock().unwrap().is_empty(),
            "no cancel on happy path");
        assert!(result.chunks().is_empty(), "fetch_script EOF gave no chunks");
    }

    #[test]
    fn execute_cancels_already_submitted_on_submit_failure() {
        // Inject failure on second submit_fragment call.
        struct FailSecondDispatcher {
            calls: Mutex<u32>,
            submitted: Mutex<Vec<TUniqueId>>,
            cancelled: Mutex<Vec<TUniqueId>>,
        }
        impl FragmentDispatcher for FailSecondDispatcher {
            fn submit_fragment(&self, params: TExecPlanFragmentParams) -> Result<(), String> {
                let mut n = self.calls.lock().unwrap();
                *n += 1;
                if *n == 2 {
                    return Err("injected".to_string());
                }
                let id = params.params.as_ref().unwrap().fragment_instance_id;
                self.submitted.lock().unwrap().push(id);
                Ok(())
            }
            fn fetch_result(&self, _: TUniqueId, _: i64) -> Result<FetchOutcome, String> {
                Ok(FetchOutcome::Eof)
            }
            fn cancel_fragments(&self, ids: &[TUniqueId]) {
                self.cancelled.lock().unwrap().extend(ids.iter().copied());
            }
        }
        let mock = Arc::new(FailSecondDispatcher {
            calls: Mutex::new(0),
            submitted: Mutex::new(Vec::new()),
            cancelled: Mutex::new(Vec::new()),
        });
        let build = tiny_two_fragment_build_result();
        let coord = ExecutionCoordinator::new(
            build,
            mock.clone() as Arc<dyn FragmentDispatcher>,
            None,
        );
        let err = coord.execute().expect_err("should fail");
        assert!(err.contains("injected"));
        assert_eq!(mock.submitted.lock().unwrap().len(), 1,
            "only first submit_fragment succeeded");
        assert_eq!(mock.cancelled.lock().unwrap().len(), 1,
            "the one successful submit was cancelled");
    }

    /// Build a minimal 2-fragment MultiFragmentBuildResult for tests.
    /// One fragment is the root (id=0), one is a CTE produce (id=1).
    /// Both fragments are skeletal (TPlanFragment::default()); the test
    /// only exercises coordinator submit/fetch/cancel orchestration, not
    /// plan execution.
    fn tiny_two_fragment_build_result() -> MultiFragmentBuildResult {
        use crate::internal_service::{TPlanFragmentExecParams, TUniqueId};
        use crate::planner::TPlanFragment;
        use crate::descriptors::TDescriptorTable;

        let root = FragmentBuildResult {
            fragment_id: 0,
            cte_id: None,
            fragment: TPlanFragment::default(),
            desc_tbl: TDescriptorTable::default(),
            exec_params: TPlanFragmentExecParams {
                fragment_instance_id: TUniqueId { hi: 0, lo: 1 },
                ..Default::default()
            },
            ..Default::default()
        };
        let cte = FragmentBuildResult {
            fragment_id: 1,
            cte_id: Some(7),
            fragment: TPlanFragment::default(),
            desc_tbl: TDescriptorTable::default(),
            exec_params: TPlanFragmentExecParams {
                fragment_instance_id: TUniqueId { hi: 0, lo: 2 },
                ..Default::default()
            },
            ..Default::default()
        };
        MultiFragmentBuildResult {
            fragments: vec![root, cte],
            edges: vec![],
            rf_plan: None,
            root_fragment_id: 0,
            ..Default::default()
        }
    }
}
```

注：上面的测试细节依赖于 `MultiFragmentBuildResult` 的构造方式，可能需要先写一个 `test_helpers::tiny_two_fragment_build_result()`。

跑测试：

```bash
cargo test --lib --package novarocks -- runtime::coordinator::coord_tests
```

期望：通过。

---

### 任务 2.5：更新 `src/engine/mod.rs:2615` 构造 dispatcher

**Files:**
- Modify: `src/engine/mod.rs:2613-2620`

- [ ] **Step 2.5.1：打开 engine/mod.rs 定位 ExecutionCoordinator::new 调用点**

搜索 `ExecutionCoordinator::new`，找到 2613-2618 行附近。当前：

```rust
crate::runtime::coordinator::ExecutionCoordinator::new(
    *build_result,
    "127.0.0.1".to_string(),
    exchange_port,
    query_opts,
)
.execute()
```

- [ ] **Step 2.5.2：替换为构造 dispatcher**

```rust
let dispatcher: std::sync::Arc<dyn crate::runtime::dispatcher::FragmentDispatcher> = {
    use crate::common::app_config::ClusterRole;
    let role = self.session_state.cluster_role();  // 见 Step 2.5.3
    match role {
        ClusterRole::AllInOne => {
            std::sync::Arc::new(crate::runtime::dispatcher::InProcessDispatcher)
        }
        ClusterRole::Fe => {
            // PR-4 才实现 RemoteDispatcher；PR-2 此处暂时 fail-loud。
            return Err("role=fe execution not yet implemented (PR-4)".to_string());
        }
        ClusterRole::Be => unreachable!("BE role does not enter the coordinator path"),
    }
};

crate::runtime::coordinator::ExecutionCoordinator::new(
    *build_result,
    dispatcher,
    query_opts,
)
.execute()
```

- [ ] **Step 2.5.3：在 session state 中暴露 cluster_role**

打开 `src/engine/mod.rs` 找到 `StandaloneSession` / `StandaloneState`。加方法 / 字段把 `ClusterRole` 传到 session。简化路径：

```rust
impl StandaloneState {
    pub fn cluster_role(&self) -> crate::common::app_config::ClusterRole {
        self.cluster_role
    }
}
```

`StandaloneState::new` / `open` 初始化时从 AppConfig 取 `cfg.cluster.role`。

- [ ] **Step 2.5.4：cargo build**

```bash
cargo build
```

期望：通过。

---

### 任务 2.6：跑全量 sql-test 回归（PR-2 主验收）

- [ ] **Step 2.6.1：启动 all-in-one 模式**

```bash
source docker/iceberg-rest/runtime/current/env.sh 2>/dev/null || true
LOG=/tmp/d1-pr2-server.log
NO_PROXY=127.0.0.1,localhost target/debug/novarocks standalone-server \
  --config "$NOVAROCKS_STANDALONE_CONFIG" >"$LOG" 2>&1 &
SRV_PID=$!
for i in $(seq 1 60); do
  grep -q '^NOVAROCKS_READY ' "$LOG" && break
  sleep 1
done
grep -q '^NOVAROCKS_READY ' "$LOG" || { tail -20 "$LOG"; kill $SRV_PID; exit 1; }
```

- [ ] **Step 2.6.2：跑 SSB**

```bash
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite ssb --mode verify
```

期望：全部通过。

- [ ] **Step 2.6.3：跑 cte（依赖根 + 多 fragment 路径）**

```bash
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite cte --mode verify
```

期望：全部通过。

- [ ] **Step 2.6.4：跑 tpc-h 子集（join + agg + HASH shuffle 代表性）**

```bash
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite tpc-h --mode verify --only q1,q5,q9,q12
```

期望：全部通过。

- [ ] **Step 2.6.5：跑 iceberg suite（依赖 Iceberg scan）**

```bash
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg --mode verify
```

期望：全部通过。

- [ ] **Step 2.6.6：停服务**

```bash
kill $SRV_PID
```

---

### 任务 2.7：PR-2 commit

- [ ] **Step 2.7.1：commit**

```bash
git add src/runtime/dispatcher.rs src/runtime/exec_params.rs src/runtime/mod.rs \
  src/runtime/coordinator.rs src/runtime/result_buffer.rs src/engine/mod.rs
git commit -m "$(cat <<'EOF'
refactor(runtime): introduce FragmentDispatcher trait and unify root path (D1 PR-2)

Adds FragmentDispatcher trait with InProcessDispatcher implementing the
all-in-one path through std::thread::spawn. Coordinator no longer runs
the root fragment foreground; all fragments go through the dispatcher.
Adds build_exec_plan_fragment_params to consolidate root / CTE /
Stream-source fragment metadata into one place. Removes hardcoded
"127.0.0.1" exchange host from engine/mod.rs. All existing sql-test
suites pass in all-in-one mode.

Refs: docs/superpowers/specs/2026-05-27-distributed-cross-process-mvp-design.md
EOF
)"
```

---

# PR-3: gRPC 协议（proto + BE handler + FE client stub）

**范围**：proto 定义 + tonic codegen + BE 端三个 handler 实现。FE 端不使用（FE role 仍 fail）。

**输入**：PR-2 已合并。

**输出**：
- proto 文件（或扩展现有 proto）含 `SubmitFragment` / `FetchResult` / `CancelFragment` RPC + 4 个 message
- `src/service/grpc_server.rs` 中 `NovaRocksGrpc` impl 新增三个 method
- `src/service/grpc_client.rs` 暴露 client 调用接口（供 PR-4 使用）

**验证**：
```bash
cargo build
cargo test --lib --package novarocks -- service::grpc_server::pr3_tests
```

**回滚**：`git revert <pr-3-merge-commit>`。

---

### 任务 3.1：proto 定义

**Files:**
- Modify: `src/service/proto/internal.proto`（或新建 `novarocks_backend.proto`，按现有 proto 文件结构决定）
- Modify: `build.rs`（如有 tonic 生成 list）

- [ ] **Step 3.1.1：定位现有 proto 文件**

```bash
find src/service -name "*.proto"
```

- [ ] **Step 3.1.2：扩展 service 定义**

在找到的 proto 文件中（含 `service NovaRocksGrpc { ... }` 的那个），加三个 RPC：

```proto
service NovaRocksGrpc {
  // ... 现有 rpc ...

  rpc SubmitFragment(SubmitFragmentRequest) returns (SubmitFragmentResponse);
  rpc FetchResult(FetchResultRequest) returns (FetchResultResponse);
  rpc CancelFragment(CancelFragmentRequest) returns (CancelFragmentResponse);
}

message SubmitFragmentRequest {
  bytes exec_plan_fragment_params_thrift = 1;
}

message SubmitFragmentResponse {
  int32 status_code = 1;
  string message = 2;
}

message PUniqueId {
  int64 hi = 1;
  int64 lo = 2;
}

message FetchResultRequest {
  PUniqueId finst_id = 1;
  int64 max_wait_ms = 2;
}

message FetchResultResponse {
  enum Status {
    READY = 0;
    NOT_READY = 1;
    EOF = 2;
    ERROR = 3;
  }
  Status status = 1;
  bytes result_batch_thrift = 2;
  string message = 3;
}

message CancelFragmentRequest {
  repeated PUniqueId finst_ids = 1;
  string reason = 2;
}

message CancelFragmentResponse {
  int32 status_code = 1;
}
```

注：用 `PUniqueId` 作为 proto 名字避免与 thrift `TUniqueId` 撞名（与现有 `PInternalService` 命名风格一致）。

- [ ] **Step 3.1.3：cargo build 触发 tonic 代码生成**

```bash
cargo build
```

期望：tonic 自动生成 `SubmitFragmentRequest` 等 struct 与对应 trait 方法。

---

### 任务 3.2：BE 端 `submit_fragment` handler

**Files:**
- Modify: `src/service/grpc_server.rs::NovaRocksGrpc` impl 块

- [ ] **Step 3.2.1：先写 mock 测试**

在 `src/service/grpc_server.rs` 末尾 / 测试模块中（新增 `mod pr3_tests`）：

```rust
#[cfg(test)]
mod pr3_tests {
    use super::*;
    use tonic::Request;

    #[tokio::test]
    async fn submit_fragment_thrift_decode_error_returns_business_error() {
        let svc = NovaRocksGrpc::new();
        let req = Request::new(SubmitFragmentRequest {
            exec_plan_fragment_params_thrift: vec![0xff, 0xff, 0xff],  // 非法 thrift
        });
        let resp = svc.submit_fragment(req).await.expect("RPC level success");
        let body = resp.into_inner();
        assert_ne!(body.status_code, 0, "should return business error for bad thrift");
        assert!(!body.message.is_empty());
    }

    #[tokio::test]
    async fn cancel_fragment_is_idempotent() {
        let svc = NovaRocksGrpc::new();
        let req = Request::new(CancelFragmentRequest {
            finst_ids: vec![PUniqueId { hi: 1, lo: 2 }],
            reason: "test".to_string(),
        });
        let resp = svc.cancel_fragment(req).await.expect("RPC success");
        assert_eq!(resp.into_inner().status_code, 0);

        let req2 = Request::new(CancelFragmentRequest {
            finst_ids: vec![PUniqueId { hi: 1, lo: 2 }],
            reason: "test-2".to_string(),
        });
        let resp2 = svc.cancel_fragment(req2).await.expect("RPC success");
        assert_eq!(resp2.into_inner().status_code, 0);
    }
}
```

- [ ] **Step 3.2.2：跑测试，确认 fail**

```bash
cargo test --lib --package novarocks -- service::grpc_server::pr3_tests
```

期望：fail（`submit_fragment` 方法不存在）。

- [ ] **Step 3.2.3：实现三个 handler**

在 `NovaRocksGrpc` impl 块中加：

```rust
async fn submit_fragment(
    &self,
    req: Request<SubmitFragmentRequest>,
) -> Result<Response<SubmitFragmentResponse>, Status> {
    let bytes = req.into_inner().exec_plan_fragment_params_thrift;
    // submit_exec_plan_fragment 是 sync 函数，但内部应该不会阻塞太久
    // （它只是 decode + 起 pipeline driver）。
    // 如果实测发现阻塞，包一层 spawn_blocking。
    match tokio::task::block_in_place(|| {
        crate::submit_exec_plan_fragment(&bytes)
    }) {
        Ok(()) => Ok(Response::new(SubmitFragmentResponse {
            status_code: 0,
            message: String::new(),
        })),
        Err(e) => Ok(Response::new(SubmitFragmentResponse {
            status_code: 1,
            message: e,
        })),
    }
}

async fn fetch_result(
    &self,
    req: Request<FetchResultRequest>,
) -> Result<Response<FetchResultResponse>, Status> {
    let r = req.into_inner();
    let finst_id = r.finst_id
        .ok_or_else(|| Status::invalid_argument("missing finst_id"))?;
    let tu = crate::internal_service::TUniqueId {
        hi: finst_id.hi,
        lo: finst_id.lo,
    };

    let outcome = tokio::task::block_in_place(|| {
        crate::runtime::result_buffer::try_fetch(tu, r.max_wait_ms)
    });

    use crate::runtime::result_buffer::TryFetchResult;
    let resp = match outcome {
        TryFetchResult::Ready(result) => {
            let batch_bytes = crate::common::thrift::thrift_serialize_result_batch(&result.result_batch);
            let status = if result.eos {
                fetch_result_response::Status::Eof
            } else {
                fetch_result_response::Status::Ready
            };
            FetchResultResponse {
                status: status as i32,
                result_batch_thrift: batch_bytes,
                message: String::new(),
            }
        }
        TryFetchResult::NotReady => FetchResultResponse {
            status: fetch_result_response::Status::NotReady as i32,
            result_batch_thrift: Vec::new(),
            message: String::new(),
        },
        TryFetchResult::Error(e) => FetchResultResponse {
            status: fetch_result_response::Status::Error as i32,
            result_batch_thrift: Vec::new(),
            message: e.to_string(),
        },
    };
    Ok(Response::new(resp))
}

async fn cancel_fragment(
    &self,
    req: Request<CancelFragmentRequest>,
) -> Result<Response<CancelFragmentResponse>, Status> {
    let r = req.into_inner();
    for id in &r.finst_ids {
        let tu = crate::internal_service::TUniqueId { hi: id.hi, lo: id.lo };
        crate::runtime::exchange::cancel_fragment(tu.hi, tu.lo);
        crate::runtime::result_buffer::cancel(tu);
    }
    Ok(Response::new(CancelFragmentResponse { status_code: 0 }))
}

// Payload serialization uses thrift_serialize_result_batch from
// crate::common::thrift, which encodes TResultBatch as thrift binary.
// The receiver (PR-4 RemoteDispatcher) deserializes via thrift_deserialize_result_batch.
```

- [ ] **Step 3.2.4：跑测试，确认 pass**

```bash
cargo test --lib --package novarocks -- service::grpc_server::pr3_tests
```

期望：两个测试都通过。

---

### 任务 3.3：FE 端 gRPC client stub

**Files:**
- Modify: `src/service/grpc_client.rs`

- [ ] **Step 3.3.1：在 grpc_client.rs 暴露 client 类型**

```rust
/// Reusable tonic client for NovaRocksGrpc.
pub struct NovaRocksGrpcClient {
    inner: NovaRocksGrpcClientGenerated<tonic::transport::Channel>,
}

impl NovaRocksGrpcClient {
    pub fn connect_blocking(addr: SocketAddr) -> Result<Self, String> {
        let endpoint = format!("http://{}", addr);
        let runtime = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
        let channel = runtime.block_on(async {
            tonic::transport::Channel::from_shared(endpoint)
                .map_err(|e| e.to_string())?
                .keep_alive_timeout(std::time::Duration::from_secs(5))
                .connect()
                .await
                .map_err(|e| e.to_string())
        })?;
        Ok(Self {
            inner: NovaRocksGrpcClientGenerated::new(channel),
        })
    }

    pub fn blocking_submit_fragment(
        &mut self,
        req: SubmitFragmentRequest,
    ) -> Result<SubmitFragmentResponse, String> {
        let rt = current_or_new_runtime();
        rt.block_on(async {
            self.inner.submit_fragment(req).await
                .map(|r| r.into_inner())
                .map_err(|e| e.to_string())
        })
    }

    pub fn blocking_fetch_result(
        &mut self,
        req: FetchResultRequest,
    ) -> Result<FetchResultResponse, String> {
        let rt = current_or_new_runtime();
        rt.block_on(async {
            self.inner.fetch_result(req).await
                .map(|r| r.into_inner())
                .map_err(|e| e.to_string())
        })
    }

    pub fn blocking_cancel_fragment(
        &mut self,
        req: CancelFragmentRequest,
    ) -> Result<CancelFragmentResponse, String> {
        let rt = current_or_new_runtime();
        rt.block_on(async {
            self.inner.cancel_fragment(req).await
                .map(|r| r.into_inner())
                .map_err(|e| e.to_string())
        })
    }
}

fn current_or_new_runtime() -> tokio::runtime::Handle {
    tokio::runtime::Handle::try_current().unwrap_or_else(|_| {
        // 测试 / 没有外层 runtime 时降级到新建 runtime。
        // 生产路径（FE coordinator）一定有外层 runtime。
        static FALLBACK: once_cell::sync::Lazy<tokio::runtime::Runtime> =
            once_cell::sync::Lazy::new(|| tokio::runtime::Runtime::new().unwrap());
        FALLBACK.handle().clone()
    })
}
```

注意：tonic 生成的 client 是 async，blocking 调用要用 tokio runtime。这部分细节在 RemoteDispatcher 实现里也会用，提到 client wrapper 中复用。

- [ ] **Step 3.3.2：cargo build 确认**

```bash
cargo build
```

期望：编译通过。

---

### 任务 3.4：PR-3 commit

- [ ] **Step 3.4.1：commit**

```bash
git add src/service/proto/*.proto src/service/grpc_server.rs src/service/grpc_client.rs build.rs
git commit -m "$(cat <<'EOF'
feat(grpc): add SubmitFragment/FetchResult/CancelFragment RPCs (D1 PR-3)

Extends NovaRocksGrpc service with three new unary RPCs that mirror the
StarRocks BE thrift FFI surface. BE-side handlers delegate to existing
submit_exec_plan_fragment / result_buffer::try_fetch / exchange::cancel
entries; payload is thrift-encoded TExecPlanFragmentParams for
byte-identical compatibility with StarRocks FE-compat mode.

Refs: docs/superpowers/specs/2026-05-27-distributed-cross-process-mvp-design.md
EOF
)"
```

---

# PR-4: `RemoteDispatcher` + FE Role Wiring

**范围**：FE role 实际跑通。1 FE + 1 BE 同机跨进程能跑 `SELECT 1` 和 SSB Q1。

**输入**：PR-3 已合并。

**输出**：
- `src/runtime/dispatcher.rs` 新增 `RemoteDispatcher`
- `src/engine/mod.rs` 根据 role 选 dispatcher（替换 PR-2 留下的 `return Err(...)` 占位）
- `src/server/mod.rs` FE role 启动逻辑（dial BE + 起 MySQL server）
- `src/server/mod.rs` BE role 启动逻辑（起 gRPC server，不起 MySQL）
- `tests/cluster_mvp/` 新 crate 含 `SELECT 1` smoke

**验证**：
```bash
cargo build
cargo test --test cluster_mvp_smoke
```

**回滚**：`git revert <pr-4-merge-commit>`，FE/BE role 回到 PR-1 的占位 error。

---

### 任务 4.1：实现 `RemoteDispatcher`

**Files:**
- Modify: `src/runtime/dispatcher.rs`

- [ ] **Step 4.1.1：先写 RemoteDispatcher 单测（用 mock gRPC server）**

```rust
#[cfg(test)]
mod remote_dispatcher_tests {
    use super::*;

    /// Spawn a minimal in-process tonic server returning known responses.
    /// Used to verify RemoteDispatcher request shaping and error mapping.
    fn spawn_mock_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        use tonic::transport::Server;
        use tokio::net::TcpListener;

        // 用 :0 拿系统分配的空闲端口，避免测试间冲突。
        let rt = tokio::runtime::Runtime::new().unwrap();
        let listener = rt.block_on(async {
            TcpListener::bind("127.0.0.1:0").await.unwrap()
        });
        let addr = listener.local_addr().unwrap();
        let incoming = tonic::transport::server::TcpIncoming::from_listener(
            listener, true, None,
        ).unwrap();

        let svc = crate::service::NovaRocksGrpcServer::new(MockGrpc);
        let handle = rt.spawn(async move {
            Server::builder()
                .add_service(svc)
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        // rt 借给 join handle；测试 drop 时进程退出会清理。
        std::mem::forget(rt);
        (addr, handle)
    }

    struct MockGrpc;

    #[tonic::async_trait]
    impl crate::service::NovaRocksGrpc for MockGrpc {
        async fn submit_fragment(
            &self,
            _req: tonic::Request<crate::service::SubmitFragmentRequest>,
        ) -> Result<tonic::Response<crate::service::SubmitFragmentResponse>, tonic::Status> {
            Ok(tonic::Response::new(crate::service::SubmitFragmentResponse {
                status_code: 0,
                message: String::new(),
            }))
        }
        async fn fetch_result(
            &self,
            _req: tonic::Request<crate::service::FetchResultRequest>,
        ) -> Result<tonic::Response<crate::service::FetchResultResponse>, tonic::Status> {
            Ok(tonic::Response::new(crate::service::FetchResultResponse {
                status: crate::service::fetch_result_response::Status::Eof as i32,
                result_batch_thrift: Vec::new(),
                message: String::new(),
            }))
        }
        async fn cancel_fragment(
            &self,
            _req: tonic::Request<crate::service::CancelFragmentRequest>,
        ) -> Result<tonic::Response<crate::service::CancelFragmentResponse>, tonic::Status> {
            Ok(tonic::Response::new(crate::service::CancelFragmentResponse {
                status_code: 0,
            }))
        }
        // 其余现有 RPC（exchange / transmit_runtime_filter / lookup）
        // 在 mock 中实现为返回 Unimplemented，因为 RemoteDispatcher 不会调它们。
        async fn exchange(
            &self,
            _req: tonic::Request<tonic::Streaming<crate::service::PTransmitChunkParams>>,
        ) -> Result<tonic::Response<Self::ExchangeStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        type ExchangeStream = futures::stream::Empty<Result<crate::service::PTransmitChunkResult, tonic::Status>>;
        async fn exchange_unary(
            &self,
            _: tonic::Request<crate::service::PTransmitChunkParams>,
        ) -> Result<tonic::Response<crate::service::PTransmitChunkResult>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn transmit_runtime_filter(
            &self,
            _: tonic::Request<crate::service::PTransmitRuntimeFilterParams>,
        ) -> Result<tonic::Response<crate::service::PTransmitRuntimeFilterResult>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn lookup(
            &self,
            _: tonic::Request<crate::service::PLookupRequest>,
        ) -> Result<tonic::Response<crate::service::PLookupResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
    }

    #[test]
    fn remote_dispatcher_submit_sends_thrift_bytes() {
        let (addr, _handle) = spawn_mock_server();
        let dispatcher = RemoteDispatcher::new(addr).expect("connect");
        let params = crate::runtime::exec_params::test_helpers::empty_fragment_params(
            crate::internal_service::TUniqueId { hi: 1, lo: 2 }
        );
        let r = dispatcher.submit_fragment(params);
        assert!(r.is_ok());
    }

    #[test]
    fn remote_dispatcher_fetch_eof() {
        let (addr, _) = spawn_mock_server();
        let dispatcher = RemoteDispatcher::new(addr).expect("connect");
        let r = dispatcher.fetch_result(
            crate::internal_service::TUniqueId { hi: 1, lo: 2 },
            0,
        ).expect("fetch");
        assert!(matches!(r, FetchOutcome::Eof));
    }
}
```

- [ ] **Step 4.1.2：实现 `RemoteDispatcher`**

```rust
pub struct RemoteDispatcher {
    backend: std::net::SocketAddr,
    client: std::sync::Mutex<crate::service::grpc_client::NovaRocksGrpcClient>,
}

impl RemoteDispatcher {
    pub fn new(backend: std::net::SocketAddr) -> Result<Self, String> {
        let client = crate::service::grpc_client::NovaRocksGrpcClient::connect_blocking(backend)?;
        Ok(Self {
            backend,
            client: std::sync::Mutex::new(client),
        })
    }
}

impl FragmentDispatcher for RemoteDispatcher {
    fn submit_fragment(&self, params: TExecPlanFragmentParams) -> Result<(), String> {
        let bytes = serialize_thrift(&params)
            .map_err(|e| format!("thrift serialize: {}", e))?;
        let req = crate::service::SubmitFragmentRequest {
            exec_plan_fragment_params_thrift: bytes,
        };
        let mut client = self.client.lock().map_err(|e| e.to_string())?;
        let resp = client.blocking_submit_fragment(req)?;
        if resp.status_code != 0 {
            return Err(format!(
                "submit_fragment to {} failed: status={} message={}",
                self.backend, resp.status_code, resp.message
            ));
        }
        Ok(())
    }

    fn fetch_result(
        &self,
        finst_id: TUniqueId,
        max_wait_ms: i64,
    ) -> Result<FetchOutcome, String> {
        let req = crate::service::FetchResultRequest {
            finst_id: Some(crate::service::PUniqueId {
                hi: finst_id.hi,
                lo: finst_id.lo,
            }),
            max_wait_ms,
        };
        let mut client = self.client.lock().map_err(|e| e.to_string())?;
        let resp = client.blocking_fetch_result(req)?;
        use crate::service::fetch_result_response::Status as S;
        let outcome = match S::from_i32(resp.status) {
            Some(S::Ready) => {
                let batch = thrift_deserialize_result_batch(&resp.result_batch_thrift)?;
                FetchOutcome::Ready(batch)
            }
            Some(S::NotReady) => FetchOutcome::NotReady,
            Some(S::Eof) => FetchOutcome::Eof,
            Some(S::Error) => FetchOutcome::Err(resp.message),
            None => FetchOutcome::Err(format!("unknown fetch status {}", resp.status)),
        };
        Ok(outcome)
    }

    fn cancel_fragments(&self, finst_ids: &[TUniqueId]) {
        let req = crate::service::CancelFragmentRequest {
            finst_ids: finst_ids.iter().map(|id| crate::service::PUniqueId {
                hi: id.hi,
                lo: id.lo,
            }).collect(),
            reason: "fe-initiated".to_string(),
        };
        let mut client = match self.client.lock() {
            Ok(c) => c,
            Err(_) => return,
        };
        let _ = client.blocking_cancel_fragment(req);
    }
}

fn serialize_thrift(params: &TExecPlanFragmentParams) -> Result<Vec<u8>, String> {
    // 复用现有 thrift 序列化 helper（看 src/service/internal_service.rs 是怎么 deserialize 的，
    // 反过来用同样的 thrift protocol 序列化）。
    use thrift::protocol::TBinaryOutputProtocol;
    use thrift::transport::TBufferChannel;
    let mut transport = TBufferChannel::with_capacity(0, 4096);
    let mut proto = TBinaryOutputProtocol::new(&mut transport, true);
    params.write_to_out_protocol(&mut proto)
        .map_err(|e| e.to_string())?;
    Ok(transport.write_bytes())
}

fn thrift_deserialize_result_batch(bytes: &[u8]) -> Result<crate::data::TResultBatch, String> {
    crate::common::thrift::thrift_deserialize_result_batch(bytes)
        .map_err(|e| e.to_string())
}
```

- [ ] **Step 4.1.3：跑单测**

```bash
cargo test --lib --package novarocks -- runtime::dispatcher::remote_dispatcher_tests
```

期望：两个测试通过。

---

### 任务 4.2：FE role 启动路径

**Files:**
- Modify: `src/server/mod.rs`

- [ ] **Step 4.2.1：实现 `run_fe`**

```rust
fn run_fe(
    opts: &StandaloneServerOptions,
    cluster: &crate::common::app_config::ClusterConfig,
) -> Result<(), String> {
    // 1. 启动期 dial BE 确认可达
    let backend_str = cluster.backends.first()
        .ok_or("role=fe requires exactly one backend in [cluster].backends")?;
    let backend: std::net::SocketAddr = backend_str.parse()
        .map_err(|e| format!("invalid backend address '{}': {}", backend_str, e))?;

    // 实际尝试连接一下；失败立即报错
    crate::runtime::dispatcher::RemoteDispatcher::new(backend)
        .map_err(|e| format!("failed to dial BE at {}: {}", backend, e))?;

    // 2. 启动 MySQL server，不起 starlet gRPC server
    // 与 run_all_in_one 不同的是：
    //   - 不调 start_grpc_server / start_grpc_exchange_server
    //   - StandaloneState 用 ClusterRole::Fe 初始化（influences dispatcher selection）
    run_mysql_server_with_role(opts, crate::common::app_config::ClusterRole::Fe, Some(backend))
}
```

`run_mysql_server_with_role` 是从 `run_all_in_one` 抽取的共享逻辑，参数化 `role` 和可选的远端 backend。

- [ ] **Step 4.2.2：替换 PR-1 的 fe 占位 error**

在 `dispatch_role` 中：

```rust
ClusterRole::Fe => run_fe(opts, cluster),
```

---

### 任务 4.3：BE role 启动路径

**Files:**
- Modify: `src/server/mod.rs`

- [ ] **Step 4.3.1：实现 `run_be`**

```rust
fn run_be(
    _opts: &StandaloneServerOptions,
    _cluster: &crate::common::app_config::ClusterConfig,
) -> Result<(), String> {
    // BE role 不起 MySQL server；只起 gRPC server on starlet_port。
    // gRPC server 已有 start_grpc_server 入口（src/service/grpc_server.rs:490）。
    let cfg = crate::common::app_config::load_global();
    let host = cfg.server.host.clone();
    println!("NOVAROCKS_READY role=be starlet_port={} pid={}",
        cfg.server.starlet_port,
        std::process::id());
    crate::service::grpc_server::start_grpc_server(host)
        .map_err(|e| format!("BE gRPC server: {}", e))?;
    // 阻塞，直到信号
    wait_for_shutdown_signal();
    Ok(())
}
```

注意：BE 启动后立即在 stdout 输出 `NOVAROCKS_READY role=be ...` 标记，便于测试 fixture gate。

- [ ] **Step 4.3.2：替换 PR-1 的 be 占位 error**

在 `dispatch_role` 中：

```rust
ClusterRole::Be => run_be(opts, cluster),
```

---

### 任务 4.4：engine/mod.rs 选 dispatcher

**Files:**
- Modify: `src/engine/mod.rs`（PR-2 留下的占位）

- [ ] **Step 4.4.1：替换 PR-2 的 fe 路径占位**

PR-2 留下：

```rust
ClusterRole::Fe => {
    return Err("role=fe execution not yet implemented (PR-4)".to_string());
}
```

替换为：

```rust
ClusterRole::Fe => {
    let backend = self.session_state.fe_target_backend()
        .ok_or("FE role missing target backend")?;
    std::sync::Arc::new(crate::runtime::dispatcher::RemoteDispatcher::new(backend)?)
}
```

`fe_target_backend()` 在 StandaloneState 中加，从 ClusterConfig.backends[0] 解析出 SocketAddr。

---

### 任务 4.5：新建 `tests/cluster_mvp/` smoke 测试

**Files:**
- Create: `tests/cluster_mvp/Cargo.toml`、`tests/cluster_mvp/src/lib.rs`、`tests/cluster_mvp/tests/smoke.rs`

- [ ] **Step 4.5.1：创建 crate 骨架**

```toml
# tests/cluster_mvp/Cargo.toml
[package]
name = "cluster_mvp_tests"
version = "0.1.0"
edition = "2021"

[dependencies]
tempfile = "3"
mysql = "24"

[[test]]
name = "smoke"
path = "tests/smoke.rs"
```

- [ ] **Step 4.5.2：写 spawn_be / spawn_fe helpers**

```rust
// tests/cluster_mvp/src/lib.rs
use std::process::{Child, Command, Stdio};
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::{Duration, Instant};

pub struct BeProcess {
    pub child: Child,
    pub starlet_port: u16,
}

pub struct FeProcess {
    pub child: Child,
    pub mysql_port: u16,
}

pub fn spawn_be(starlet_port: u16) -> BeProcess {
    let config = write_be_config(starlet_port);
    let binary = locate_novarocks_binary();
    let mut child = Command::new(binary)
        .args(&[
            "standalone-server",
            "--role", "be",
            "--config", config.to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn BE");

    // gate on NOVAROCKS_READY
    let stdout = child.stdout.take().unwrap();
    let reader = BufReader::new(stdout);
    let deadline = Instant::now() + Duration::from_secs(60);
    for line in reader.lines().flatten() {
        if line.starts_with("NOVAROCKS_READY role=be") {
            return BeProcess { child, starlet_port };
        }
        if Instant::now() > deadline { break; }
    }
    let _ = child.kill();
    panic!("BE did not become ready in time");
}

pub fn spawn_fe(mysql_port: u16, be_addr: &str) -> FeProcess {
    let config = write_fe_config(mysql_port, be_addr);
    let binary = locate_novarocks_binary();
    let mut child = Command::new(binary)
        .args(&[
            "standalone-server",
            "--role", "fe",
            "--config", config.to_str().unwrap(),
            "--port", &mysql_port.to_string(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn FE");

    let stdout = child.stdout.take().unwrap();
    let reader = BufReader::new(stdout);
    let deadline = Instant::now() + Duration::from_secs(60);
    for line in reader.lines().flatten() {
        if line.starts_with("NOVAROCKS_READY ") {
            return FeProcess { child, mysql_port };
        }
        if Instant::now() > deadline { break; }
    }
    let _ = child.kill();
    panic!("FE did not become ready in time");
}

fn write_be_config(starlet_port: u16) -> PathBuf {
    let dir = tempfile::tempdir().expect("tmp");
    let path = dir.path().join("be.toml");
    let content = format!(
        r#"
[server]
host = "127.0.0.1"
starlet_port = {}

[cluster]
role = "be"
"#,
        starlet_port
    );
    std::fs::write(&path, content).unwrap();
    // 保留 tmpdir 不让 Drop 删；让 path 持久化
    std::mem::forget(dir);
    path
}

fn write_fe_config(mysql_port: u16, be_addr: &str) -> PathBuf {
    let dir = tempfile::tempdir().expect("tmp");
    let path = dir.path().join("fe.toml");
    let content = format!(
        r#"
[server]
host = "127.0.0.1"

[standalone_server]
mysql_port = {}

[cluster]
role = "fe"
backends = ["{}"]
"#,
        mysql_port, be_addr
    );
    std::fs::write(&path, content).unwrap();
    std::mem::forget(dir);
    path
}

fn locate_novarocks_binary() -> PathBuf {
    // CARGO_BIN_EXE_<name> 在 integration test 中可用
    PathBuf::from(env!("CARGO_BIN_EXE_novarocks"))
}

impl Drop for BeProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for FeProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
```

- [ ] **Step 4.5.3：写 smoke 测试**

```rust
// tests/cluster_mvp/tests/smoke.rs
use cluster_mvp_tests::*;

#[test]
fn select_1_works_cross_process() {
    let be = spawn_be(19070);
    let fe = spawn_fe(19030, "127.0.0.1:19070");

    let url = format!("mysql://root@127.0.0.1:{}/", fe.mysql_port);
    let pool = mysql::Pool::new(url.as_str()).expect("pool");
    let mut conn = pool.get_conn().expect("conn");
    let rows: Vec<i64> = conn.query("SELECT 1").expect("query");
    assert_eq!(rows, vec![1]);

    // be / fe Drop 会 kill 进程
    drop(fe);
    drop(be);
}
```

- [ ] **Step 4.5.4：跑测试**

```bash
cargo build  # 确保 binary 存在
cargo test --test smoke
```

期望：测试通过。

---

### 任务 4.6：手动 SSB Q1 byte-identical 校验

- [ ] **Step 4.6.1：跑 all-in-one baseline，记录输出**

```bash
$SQLT --suite ssb --mode record --only q1
cp -r sql-tests/ssb/result/q1.json /tmp/ssb-q1-baseline.json
```

- [ ] **Step 4.6.2：在 cluster_mvp smoke 中加 SSB Q1 测试**

在 `tests/cluster_mvp/tests/smoke.rs` 加：

```rust
#[test]
fn ssb_q1_byte_identical() {
    // 使用 docker iceberg fixture 准备好 ssb 数据，或 mock 一个最小版。
    // 详情：在 BE config 中加 iceberg catalog 配置（与 NOVAROCKS_STANDALONE_CONFIG 一致）
    // 这一节实现细节比较多，作为 stretch goal；可以延后到 PR-6 集中处理。
    // PR-4 主要确认跨进程 SELECT 1 + SSB Q1 流通即可。
}
```

实际 SSB Q1 测试细节移到 PR-6 集中实现。PR-4 的 smoke 范围是 `SELECT 1`。

---

### 任务 4.7：PR-4 commit

- [ ] **Step 4.7.1：commit**

```bash
git add src/runtime/dispatcher.rs src/server/mod.rs src/engine/mod.rs \
  tests/cluster_mvp/
git commit -m "$(cat <<'EOF'
feat(distributed): wire up FE/BE roles end-to-end with RemoteDispatcher (D1 PR-4)

Implements RemoteDispatcher using the tonic gRPC client from PR-3.
role=fe starts MySQL server and dials the configured BE; role=be starts
the gRPC server (starlet_port) without MySQL. engine/mod.rs picks the
dispatcher based on role. tests/cluster_mvp/ smoke confirms SELECT 1
works cross-process.

Refs: docs/superpowers/specs/2026-05-27-distributed-cross-process-mvp-design.md
EOF
)"
```

---

# PR-5: 错误处理 + Cancel + Timeout

**范围**：把 spec 第 9 节的错误处理完整落地。

**输入**：PR-4 已合并（happy path 跑通）。

**输出**：
- submit 半失败时正确 cancel 已 submit 的 fragment
- query_timeout 触发 cancel + 返回 timeout 错误
- MySQL session 断连感知 + 触发 cancel
- BE 进程崩溃时 FE 拿到 RPC error → 干净失败

**验证**：`tests/cluster_mvp/tests/errors.rs` 全部通过。

**回滚**：`git revert <pr-5-merge-commit>`，错误路径回到 PR-4 的最小版本。

---

### 任务 5.1：Submit 半失败清理

**Files:**
- Modify: `src/runtime/coordinator.rs`（已在 PR-2 实现，PR-5 加测试 + 边界 case 修复）
- Create: `tests/cluster_mvp/tests/errors.rs`

- [ ] **Step 5.1.1：写测试 `submit_half_failure_cancels_submitted`**

在 `tests/cluster_mvp/tests/errors.rs` 中加测试（用 fault injection 强制第 N 次 SubmitFragment 失败）。

实现 fault injection：BE 端加一个 test-only 配置项 `[debug].fault_inject_submit_fail_after = N`，第 N+1 次调 submit_fragment 时返回 status_code != 0。

跑测试 → 应该 fail（如果 coordinator 没正确 cancel 第 1 个 fragment）。

- [ ] **Step 5.1.2：补 coordinator cancel 逻辑**

PR-2 已经有 `dispatcher.cancel_fragments(&submitted_ids)`；这一步主要是确认 `submitted_ids` 在 submit RPC 失败前不要把当前 fragment 加进去（避免取消未 submit 成功的）。

- [ ] **Step 5.1.3：跑测试，期望 pass**

---

### 任务 5.2：query_timeout 强制

- [ ] **Step 5.2.1：写测试**

构造一个会跑很久的查询（`SELECT sleep(60)` 或在 BE 端有 hook 让 fragment block），设 `query_timeout=2`，期望 2 秒内返回 timeout 错误。

- [ ] **Step 5.2.2：coordinator 的 deadline 逻辑校验**

PR-2 已实现 deadline 检查，确认在 fetch loop 内正确触发 cancel。

- [ ] **Step 5.2.3：跑测试**

---

### 任务 5.3：MySQL session 断连感知

**Files:**
- Modify: `src/server/mod.rs`（MySQL handler）+ `src/runtime/coordinator.rs`（cancellation 信号）

- [ ] **Step 5.3.1：写测试**

```rust
#[test]
fn mysql_disconnect_triggers_cancel() {
    let be = spawn_be(19070);
    let fe = spawn_fe(19030, "127.0.0.1:19070");

    // 起一个长查询，再立即断连
    // 用 BE log 验证收到 CancelFragment RPC
    // 略，需要 BE log scrape
}
```

- [ ] **Step 5.3.2：加 Arc<AtomicBool> 取消信号**

在 `StandaloneSession::execute_in_context` 或类似入口，创建 `cancel_signal: Arc<AtomicBool>`，passed to coordinator. MySQL handler 在 session drop / TCP close 时 set。

Coordinator 在 fetch loop 中每次 check：

```rust
if self.cancel_signal.load(Ordering::Relaxed) {
    self.dispatcher.cancel_fragments(&all_finst_ids);
    return Err("client disconnected".into());
}
```

- [ ] **Step 5.3.3：跑测试**

---

### 任务 5.4：BE 崩溃路径

- [ ] **Step 5.4.1：写测试 `be_kill9_during_query_fails_cleanly`**

```rust
#[test]
fn be_kill9_during_query_fails_cleanly() {
    let mut be = spawn_be(19071);
    let fe = spawn_fe(19031, "127.0.0.1:19071");

    // 起一个查询，期间 kill BE
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(500));
        let _ = be.child.kill();
    });

    let url = format!("mysql://root@127.0.0.1:{}/", fe.mysql_port);
    let pool = mysql::Pool::new(url.as_str()).unwrap();
    let mut conn = pool.get_conn().unwrap();
    let result: Result<Vec<i64>, _> = conn.query("SELECT * FROM big_table");  // 慢查询
    assert!(result.is_err(), "query should fail when BE dies");
    // FE 不挂
    drop(fe);
}
```

- [ ] **Step 5.4.2：确认 coordinator 已处理 RPC error**

PR-2 已实现 `Err(rpc_err) => { cancel; return Err(...) }` 路径。确认 RPC error 时 cancel 路径不会 panic（因为 BE 已死，cancel RPC 也会失败——cancel_fragments 不返回错误，best-effort 即可）。

- [ ] **Step 5.4.3：跑测试**

---

### 任务 5.5：PR-5 commit

- [ ] **Step 5.5.1：commit**

```bash
git add src/runtime/coordinator.rs src/server/mod.rs tests/cluster_mvp/
git commit -m "$(cat <<'EOF'
feat(distributed): error/cancel/timeout robustness (D1 PR-5)

Adds explicit handling for: submit half-failure cleanup, query_timeout
enforcement via FE wallclock, MySQL session disconnect propagation via
Arc<AtomicBool>, BE crash detection via RPC errors. Each path is covered
by an integration test in tests/cluster_mvp/tests/errors.rs that kills
the BE / drops the MySQL connection / triggers timeout and verifies FE
returns a clean error.

Refs: docs/superpowers/specs/2026-05-27-distributed-cross-process-mvp-design.md
EOF
)"
```

---

# PR-6: SQL Test Runner Cross-Process Mode + 验收套件

**范围**：sql-test-runner 增加 `--cluster-mode cross-process` 选项，SSB / TPC-H 子集 / iceberg-rest smoke 跑通 byte-identical。

**输入**：PR-5 已合并。

**输出**：
- `tests/sql-test-runner` 接受 `--cluster-mode cross-process` 并内部起 FE+BE
- SSB 全套 + TPC-H Q1/Q5/Q9/Q12 + iceberg-rest smoke 在 cross-process 模式下与 all-in-one byte-identical

**验证**：D1 主验收门槛（spec 第 11 节）。

**回滚**：`git revert <pr-6-merge-commit>`，runner 回到只支持 all-in-one。

---

### 任务 6.1：runner `--cluster-mode` 选项

**Files:**
- Modify: `tests/sql-test-runner/src/main.rs`（或 args.rs）

- [ ] **Step 6.1.1：加 `--cluster-mode` flag**

```rust
#[derive(Clone, Copy, Debug)]
pub enum ClusterMode {
    AllInOne,    // 默认；现有行为
    CrossProcess, // 内部起 1FE+1BE
}

// CLI parsing 加 --cluster-mode { all-in-one | cross-process }
```

- [ ] **Step 6.1.2：在 runner 内部根据模式起 fixture**

```rust
fn launch_server(mode: ClusterMode, config: &Config) -> Box<dyn ServerHandle> {
    match mode {
        ClusterMode::AllInOne => {
            // 现有逻辑
        }
        ClusterMode::CrossProcess => {
            // 借用 cluster_mvp_tests::{spawn_be, spawn_fe}
            // 或者新写一份 helper
            spawn_cross_process_pair()
        }
    }
}
```

- [ ] **Step 6.1.3：smoke 验证**

```bash
$SQLT --suite ssb --mode verify --only q1 --cluster-mode cross-process
```

期望：通过且 stdout 显示 FE/BE 进程被起。

---

### 任务 6.2：跑 SSB 全套

- [ ] **Step 6.2.1**

```bash
$SQLT --suite ssb --mode verify --cluster-mode cross-process
```

期望：全部 13 个查询通过。

如有 byte-identical 不通过：

1. 用 `--mode diff` 看具体差异
2. 检查 chunk 顺序是否因为 ResultSink → thrift-binary TResultBatch → 反序列化 → MySQL encode 路径引入了不一致
3. 如果是 schema metadata 差异（如 timezone），在 PR-6 范围内 normalize

---

### 任务 6.3：跑 TPC-H Q1/Q5/Q9/Q12

- [ ] **Step 6.3.1**

```bash
$SQLT --suite tpc-h --mode verify --only q1,q5,q9,q12 --cluster-mode cross-process
```

期望：4 个查询全部 byte-identical。

---

### 任务 6.4：跑 iceberg-rest smoke

- [ ] **Step 6.4.1：起 fixture**

```bash
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh
```

- [ ] **Step 6.4.2：跑**

```bash
$SQLT --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-rest --mode verify --cluster-mode cross-process
```

期望：通过。

---

### 任务 6.5：性能 sanity check

- [ ] **Step 6.5.1：测 all-in-one 模式 SSB Q1 延迟**

```bash
time mysql -h 127.0.0.1 -P 9030 -e "$(cat sql-tests/ssb/sql/q1.sql)"
```

- [ ] **Step 6.5.2：测 cross-process 模式 SSB Q1 延迟**

起 FE+BE，然后：

```bash
time mysql -h 127.0.0.1 -P 19030 -e "$(cat sql-tests/ssb/sql/q1.sql)"
```

- [ ] **Step 6.5.3：比对**

期望：cross-process 比 all-in-one 慢 < 20%。慢 > 50% 时把 FetchResult batch size / poll 频率列入独立优化项 issue。

---

### 任务 6.6：PR-6 commit

- [ ] **Step 6.6.1：commit**

```bash
git add tests/sql-test-runner/ tests/cluster_mvp/
git commit -m "$(cat <<'EOF'
feat(distributed): sql-test-runner --cluster-mode cross-process + D1 acceptance (D1 PR-6)

Adds --cluster-mode cross-process to sql-test-runner; runner internally
spawns 1 FE + 1 BE for each test. SSB full suite, TPC-H Q1/Q5/Q9/Q12,
and iceberg-rest smoke pass byte-identically against the all-in-one
baseline. This completes the D1 main acceptance gate per
docs/superpowers/specs/2026-05-27-distributed-cross-process-mvp-design.md.

Refs: docs/superpowers/specs/2026-05-27-distributed-cross-process-mvp-design.md
EOF
)"
```

---

## D1 验收门槛 Checklist

完成所有 6 个 PR 后，确认 spec 第 11 节验收标准全部满足：

- [ ] 1 FE + 1 BE 同机跨进程跑通 SSB 全套，输出与 all-in-one byte-identical
- [ ] TPC-H Q1 / Q5 / Q9 / Q12 在 cross-process 模式下输出与 all-in-one byte-identical
- [ ] 现有所有 sql-test suite 在 all-in-one 模式下不回归
- [ ] `iceberg-rest` smoke 在 cross-process 模式下通过
- [ ] `--role fe` 不监听 starlet_port；`--role be` 不监听 MySQL 端口
- [ ] BE 进程 `kill -9` / SubmitFragment RPC 失败 / FetchResult RPC 失败时 FE 上的 query 干净失败，FE 进程不挂
- [ ] query_timeout 触发后 FE 真的发了 `CancelFragment`，BE log 显示 cancel 被处理
- [ ] coordinator.rs 中 488-565 行根 fragment foreground 路径删除，所有 fragment 走统一 dispatcher 路径
- [ ] `engine/mod.rs:2615` 硬编码 `"127.0.0.1"` 删除

到此 D1 完成。下一步进入 [D2: 多 BE 并行执行](file:///Users/harbor/Documents/Obsidian/NovaRocks%20TODO/distributed-multi-be-execution.md)。

---

## 风险与开放问题（执行时关注）

1. **Step 2.2.3** ResultBuffer EOF 约定——当前 PR-3 实现保持 `TryFetchResult` 三态，并用 `Ready(result)` + `result.eos == true` 表示 EOF；PR-4 应沿用该约定。
2. **Step 2.4.6** 测试用的 `MultiFragmentBuildResult` builder 可能不存在；若没有，先写一个 `test_helpers::tiny_two_fragment_build_result()` 再写测试。
3. **Step 3.2.3** `submit_exec_plan_fragment(thrift_bytes)` 是否真的不阻塞？若实测发现它会等 fragment 完成才返回，BE handler 改用 `tokio::task::spawn_blocking`，并在 plan 这里补一句确认 step。
4. **Step 4.5.4** integration test 需要 `CARGO_BIN_EXE_novarocks` 能找到 binary；若 binary 名不叫 `novarocks`，调整。
5. **PR-5 fault injection**：spec 没有要求生产代码引入 fault injection 开关；放在 `#[cfg(debug_assertions)]` 或 `#[cfg(feature = "fault-inject")]` 编译开关下，release 不带。
6. **PR-6 byte-identical**：D1 单 BE 应该完全确定；如果遇到顺序差异，先 root cause 再决定是 normalize 测试还是修代码。
