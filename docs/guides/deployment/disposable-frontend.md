# 可丢弃 FE 部署与 Drain

LNP-8 将 FE 的 catalog bootstrap 与 serving lifecycle 分开：catalog desired state 只来自一个
显式 source；本地 SQLite、缓存和 worker runtime 可以随 Pod 删除重建，不能成为 catalog truth 或
backend membership authority。

## StaticFile 起步配置

将 `novarocks-fe.toml.example` 与 `novarocks-catalogs.toml.example` 一起复制到同一部署目录，
并将后者改名为 `novarocks-catalogs.toml`。FE config 必须保持：

```toml
[catalog_source]
mode = "static-file"
static_file_path = "novarocks-catalogs.toml"

[server]
frontend_drain_timeout_ms = 300000
frontend_cleanup_timeout_ms = 30000
```

`static_file_path` 相对 FE config 文件所在目录解析。文件是一次性、完整的 v1 snapshot；显式空
snapshot 合法，缺失、损坏或只写入一半的文件会使启动失败，而不会回退到 StateStore 或旧缓存。
Kubernetes 可把文件作为只读 ConfigMap/Secret volume 挂载；若使用 SQLite carrier，将其目录放在
`emptyDir`，并接受 Pod 删除后重建 accelerator。

## DynamicStateStore 迁移

仍依赖 SQL `CREATE/DROP CATALOG` 的部署应显式写入：

```toml
[catalog_source]
mode = "dynamic-state-store"

[state_store]
provider = "sqlite"
path = "meta/frontend-state.sqlite"
cluster_id = "production-cluster"
deployment_owner = "fe-a"
```

迁移前确认 catalog logical state 已完整写入 StateStore；不要同时配置 `static_file_path`，也不要因为
存在 `[state_store]` 而省略 mode。`managed-controller` 当前仍不支持。

## Probes 与蓝绿 drain

使用 management HTTP 而不是 Native gRPC port：`GET /livez` 表示 process 可响应，`GET /readyz`
只有 catalog bootstrap 完成且 FE 接收新 workload 时才返回 200。每个 catalog 的局部不可用不必让
base readiness 失败，应从 state/metrics 另行观测。

蓝绿切换顺序是：先等待 green `/readyz`，在 LB/Gateway external deactivate blue，再向 blue FE
发送 `SIGTERM`。旧 FE 的 Trino 入口在 drain 后拒绝新 statement；已准入 attempt 继续到
内部 deadline。不要依赖 LB 摘流本身实现这个线性化边界。

Kubernetes 示例：

```yaml
spec:
  terminationGracePeriodSeconds: 360
  containers:
    - name: novarocks-fe
      readinessProbe:
        httpGet:
          path: /readyz
          port: 8040
      livenessProbe:
        httpGet:
          path: /livez
          port: 8040
```

360 秒覆盖默认 300 秒 drain、30 秒 cleanup，另留 30 秒 orchestrator margin。不要添加一个会抢在
SIGTERM 前关闭 FE 的 `preStop` hook；external deactivate 属于平台路由层，SIGTERM 才是 FE 本地
不可逆 drain authority。
