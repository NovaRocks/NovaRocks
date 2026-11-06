# 物化视图管理接续

FE 重启后，本部署自己写过的 MV 不会自动恢复可管理。本文说明为什么，以及两种把它恢复回来的方式。

## 为什么重启后管理是关着的

一个 MV 目标上的受管标记记录了是哪个部署、哪个进程实例（incarnation）在写它。新起的 FE
读到自己部署名下、但不是自己 incarnation 写的目标时，无法知道上一个写者停下来的那一刻有没有
一次已经发出、还没收到答复的效果（catalog commit 或对象删除）。假定没有，是唯一一种会损坏目标
的假定。

所以进程给每个这样的目标挂一道屏障，管理关闭，直到有证据说明旧写者已经不可能再动作。
**读不受影响**：已发布的物化结果就是湖里的事实，查询与改写照常。

`CALL novarocks_mv_management_status('<catalog>', '<database>', '<name>')` 会报告目标当前处于
哪个阶段、有几个未收束效果、本进程的 owner 与 incarnation，以及做声明时要引用的 challenge。

## 方式一：逐目标的运维声明

适合少量目标或一次性处理：

```sql
CALL novarocks_mv_management_status('ice_rest', 'analytics', 'orders_mv');
CALL novarocks_mv_resume_management(
  'ice_rest', 'analytics', 'orders_mv',
  '<challenge>', '<old-incarnation>',
  '<operator-reference>', '<evidence-reference-and-statement>'
);
```

声明先写审计再生效；`[mv_management].audit_log` 没有配置时命令直接拒绝——写不下来的声明不能生效。

## 方式二：部署级的启动隔离声明

持有几十个 MV 的部署逐个声明不现实。这时由**停掉上一个进程的那一方**写一份声明：它对**写者**
说话，而不是对每个目标说话。

声明本身不能让管理立刻打开。它确定的是"旧写者从哪一刻起不可能再动作"这个保守时刻 T；从 T 起还要
等满配置的远端效果寿命窗口，屏障才会退休。**没有配置远端保证时没有窗口可等，部署保持只读**——
这是默认，不是故障。

### 配置

```toml
[mv_management]
audit_log = "mv-management-audit.log"

# 两条路径都要有界，跨路径的重启屏障才可能自动退休。
[mv_management.catalog_commit_guarantee]
lifetime_ms = 60000
safety_margin_ms = 5000
basis = "provider-service-contract"
source = "Iceberg REST 服务合同 §4.2"

[mv_management.object_deletion_guarantee]
lifetime_ms = 120000
safety_margin_ms = 5000
basis = "deployment-enforced-bound"
source = "对象存储删除请求由入口网关在 120s 后拒绝"

[mv_management.startup_isolation]
# 绝对路径。它承载运维授权，按与本配置文件同等的权限保护。
evidence_file = "/var/lib/novarocks/mv-startup-isolation.toml"
# 每次启动一个新值，由编排同时写进环境与声明文件。
launch_nonce = "${ENV:NOVAROCKS_MV_LAUNCH_NONCE}"
```

### 声明文件

```toml
deployment = "prod-a"
# 本次启动的 FE incarnation，见下面的取法。
for_incarnation = "0199a1c2-3d4e-7f01-8a2b-3c4d5e6f7a8b"
# 与 NOVAROCKS_MV_LAUNCH_NONCE 相同。
nonce = "launch-7f3c9a"
# 已隔离的旧 incarnation，可以多个。
isolated_incarnations = ["0198f0e1-2c3d-7e90-8b1a-2d3c4e5f6a7b"]
# 隔离完成的时刻。宁可写晚，写晚只会推迟接续，写早会提前打开。
isolated_at_unix_ms = 1726650000000
# 后来的人据此判断这份声明可不可信。
source = "systemd: novarocks-fe.service MainPID 1234 exited 2026-09-18T07:00:00Z"

# 省略 [[target]] 表示本部署的全部目标。
# 列出则只覆盖列出的这些，其余保持只读。
# [[target]]
# catalog = "ice_rest"
# database = "analytics"
# name = "orders_mv"
```

未被读取的键会被拒绝：作者以为写下了、而进程根本没看的字段，比没写更糟。

### 编排流程

新 FE 的 incarnation 是进程自己在启动时铸的，编排事先不知道。所以声明**在进程起来之后写**，
进程每个维护周期重读一次，不是只在启动时读一次。

```bash
#!/bin/sh
# 1. 停掉上一个进程，并确认它真的退出了。这一步产生 isolated_at 与 source。
systemctl stop novarocks-fe
OLD_PID_EXIT_MS=$(date +%s000)

# 2. 本次启动的 nonce，写进环境；配置里的 ${ENV:...} 会解析它。
NONCE="launch-$(openssl rand -hex 4)"
systemctl set-environment NOVAROCKS_MV_LAUNCH_NONCE="$NONCE"
systemctl start novarocks-fe

# 3. 等 FE 可服务，然后问它自己的 incarnation 与上一个写者是谁。
trino --server http://127.0.0.1:8080 --user root --execute \
  "CALL novarocks_mv_management_status('ice_rest', 'analytics', 'orders_mv')"

# 4. 写声明。文件权限按配置文件同级保护。
umask 077
cat > /var/lib/novarocks/mv-startup-isolation.toml <<EOF
deployment = "prod-a"
for_incarnation = "$NEW_INCARNATION"
nonce = "$NONCE"
isolated_incarnations = ["$OLD_INCARNATION"]
isolated_at_unix_ms = $OLD_PID_EXIT_MS
source = "systemd: novarocks-fe.service MainPID $OLD_PID exited"
EOF
```

`NEW_INCARNATION` 取自第 3 步的 `LocalIncarnation` 行，`OLD_INCARNATION` 取自
`UnsettledEffect1Incarnation` 行。

### 什么情况下仍然保持只读

下列每一种都不会打开管理，而且都会在日志里说明原因：

- 没有声明文件——这是默认；
- 声明里的 `deployment` 不是本部署；
- `for_incarnation` 不是本进程（上一次启动留下的文件属于这一类）；
- `nonce` 不是本次启动的；
- `isolated_incarnations` 为空，或包含本进程自己；
- `[[target]]` 列表没有覆盖到某个目标——该目标保持只读，其余不受影响；
- `isolated_at_unix_ms` 晚于进程读到它的时刻；
- 该效果的路径没有配置远端保证（跨路径的重启屏障需要**两条**路径都有）；
- 从 T 起的窗口还没走完——这一种会在窗口到期后的某个维护周期自动接续。

声明只允许一次精确的重新观察发生。**那次观察看到什么，仍然决定旧写者的效果究竟做了什么。**

## 相关

- `docs/guides/deployment/distributed.md`：正常的 FE/BE 部署形态。
- `docs/guides/deployment/disposable-frontend.md`：蓝绿 drain 与 probes。
