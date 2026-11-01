---
id: ADR-0059
title: "Configuration is injected from the composition root, not read from a process global"
domain: [configuration]
status: superseded
supersedes: []
superseded-by: ADR-0072
date: 2026-08-12
provenance:
  - "spec: 2026-08-11 CFG application-config ownership design"
  - "discussion: 2026-08-11 common/app_config.rs ownership, on whether to build novarocks-common"
  - "PR: pending — CLS-0 + CFG Phase 1 aggregate-core residue cut"
code-anchors:
  - "novarocks-server/src/main.rs (run_standalone_server_cli)"
---

## 问题

应用配置应该以什么形式到达需要它的代码？是一个进程级 `config()` 单例供任意深度的代码
随时读取，还是由组合根加载后按值注入？

## 背景与执行事实

`common/app_config.rs` 长期持有 `static CONFIG: OnceLock<RwLock<&'static NovaRocksConfig>>`，
由 `Box::leak` 填充，任何代码都能调用 `config()` 取到它；`config()` 本身在未安装时还会
惰性读盘和读环境变量。围绕它有一层 40 余个访问器组成的门面。

三个执行事实促成本裁决：

1. **门面的多数访问器无人调用。** 清点时 36 个访问器没有生产调用方，删除后
   `config.rs` 从 391 行降到 116 行，没有任何行为变化。门面主要在服务它自己。
2. **全局读取的真实规模被别名 import 掩盖。** `use crate::novarocks_config::config as
   novarocks_app_config;` 让按 `app_config::config()` 直接 grep 只能找到 16 处，实际是 76 处
   （其中 58 处在 `common/config.rs` 自身）。一个连规模都难以准确测量的耦合，无法评估其
   影响面。
3. **单例让"配置何时生效"无法在类型上表达。** `open_with_config` 拿到一份已加载、已校验的
   config，却把它装进全局再由 `open_body` 读回来；FE 的 query-control 超时在每次查询准入时
   重新读取并重新校验，导致一份不可用的 `[runtime]` 配置要等到第一条查询才报错，而不是启动时。

## 考虑过的选项

1. **保留单例，只清理死访问器。** 成本最低，但保留了"任意深度代码可随时读配置"这一
   形态，无法阻止新的深层读取重新长出来，也无法把校验前移到启动。
2. **新建 `novarocks-common` crate 承载配置。** 讨论中明确允许废除"不得建 novarocks-common"
   的限制。但配置 schema 的各个 section 天然属于各自的领域 crate（`[runtime]` 属执行、
   `[connector.object_store]` 属 fs、`[cluster]` 属 membership）；建一个公共 crate 会把
   本该分家的东西重新聚成一堆，只是换了个位置的聚合 core。
3. **组合根加载并注入，删除全局。**（采纳）

## 裁决

删除进程级配置单例。加载与安装分离：`load_from_path` / `load_from_env_or_default` 返回
调用方拥有的 `NovaRocksConfig` 值，由组合根交给需要它的组件。

派生规则：

- **深层代码接收值，不取全局。** 需要配置的执行/连接器/MV 代码从构造时传入的字段读取
  （`ExecutionRuntimeConfig::exchange_io_max_inflight_bytes`、`StandaloneState::mv_partition_state_max_entries`、
  `FrontendQueryControlTimeouts`），而不是调用访问器。
- **校验尽量前移到启动。** FE query-control 超时在 coordinator 构造时校验一次，不可用的
  `[runtime]` 配置在启动失败，而不是在第一条查询上失败。
- **debug/test 开关归进程环境，不归配置文件。** 这些开关由启动进程的人（开发者或 SQL test
  runner）拥有，且被深层代码读取；走配置文件会迫使那些路径去够全局。它们在 release 构建中
  被编译掉。
- **真正的进程单例保留单例形态，但尺寸由组合根安装。** data runtime 有约 50 处深层调用方，
  它本身该是单例；只有它的线程尺寸从配置来，因此改为组合根调用
  `install_data_runtime_sizing`。

## 接受的妥协（诚实记录）

**data runtime 的安装顺序是运行时约束，不是类型约束。** `install_data_runtime_sizing`
必须先于任何 `data_runtime()` 使用；类型系统不表达这一点。缓解手段是：安装时若发现 runtime
已经用回退尺寸建好，直接返回 Err 让进程启动失败——即错误会在启动时确定性暴露，而不是让
进程带着被静默忽略的配置继续跑。但这仍然是运行时检查，不是编译期保证；本裁决接受它，因为
把 50 处深层调用方全部改成接收参数所付出的改动量，远大于这条约束的实际风险。

**`role=all-in-one` 下允许重复安装。** FE 与 BE 两个组合根共享同一份配置，要求它们协调
"谁来装"是没有价值的耦合，因此同值重复安装被接受，只有异值才报错。这意味着"安装恰好一次"
这一更强的不变式被放弃了。

**配置 schema 仍然聚在 core 里。** 本裁决只解决"配置如何到达代码"，没有解决"配置 schema
归谁定义"。`NovaRocksConfig` 及其全部 section 目前仍在聚合 core 内，各 section 迁往领域
crate 是 CFG Phase 2 的工作，其硬前置是 CLS-3。在那之前，领域 crate 仍需通过 core 才能
命名自己的配置类型。

## 何时重新评估

1. 若出现第二个"进程单例 + 组合根安装尺寸"的构造，说明这是一类模式而非个案，应把
   install-before-use 的约束提取成统一机制（例如一个显式的 startup 阶段类型），而不是
   每处各写一遍守卫。
2. CFG Phase 2 完成、各 section 迁入领域 crate 后，`NovaRocksConfig` 根 schema 的归属需要
   重新裁决：它应留在 `novarocks-server`，还是彻底消失、由各组合根分别组装。
3. 若将来引入运行时可变配置（在线改 `[runtime]` 参数），本裁决的"启动时冻结"前提失效，
   需要重新决定哪些值可变、由谁通知已冻结的持有者，届时不应以恢复全局单例作为答案。
