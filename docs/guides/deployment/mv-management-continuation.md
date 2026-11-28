# 物化视图管理接续

| 重启后的操作 | 接续依据 |
| --- | --- |
| 非聚合 MV 的刷新与管理 | FE 重读湖上文档。首次写入前，以带 main 条件的槽变更登记本次 FE incarnation。 |
| MV 的 CREATE 与 DROP | 启动发现和部署级名字宽限。运行期间按 catalog、namespace、name 隔离结果未知的建删。 |
| 聚合 MV 的旧写者接续 | 逐目标屏障、现有运维声明与准确重新观察。 |

## 非聚合 MV

直接读取沿现有规则读取当前快照。查询改写与刷新按湖上的发布资格判断，适用的 Invalid 或未解除 Pending
会限制相应操作。零文件退役继续引用原数据文件。
重启后的首次写入先登记本进程。已确认的发布、退役或槽登记由原操作记收据。
未知结果保留在原进程能证明的范围内。跨进程恢复以当前发布、持久标记和 main 条件为准。

适用 Pending 记录了核验义务、源窗口、基准快照、目标映射修订、估计和内存预算。下一次刷新先比较基准到当前
main 的可见内容。Provider 证明两端净零后，FE 从基准 P 取得旧绑定，用原窗口、解释与预算重放并重新核验。
基准失去等价、历史不可读或确定性资源不足时，Pending 保持。`EXPLAIN REFRESH` 可查看资格与恢复原因。
Invalid 以及无法重放的 Pending 需要明确重建。当前 SQL 运维路径是等名字约束解除后 DROP 并重新 CREATE，
显式重建的扩展过程由后续入口交付。

## 名字隔离与启动宽限

FE 在派发 MV CREATE 或 DROP 前登记准确名字和效果。效果结果未知时，同名建删等待观察确认或声明的
效果寿命过去。无寿命保证时，隔离持续至本进程退出。DROP 确认原目标已不存在时可以释放名字。
刷新和读取仍按自身资格运行。

FE 启动后进入 `discovering`，MV CREATE 与 DROP 返回可重试的 `SERVER_STARTING_UP`。发现过程结束后：

- 发现完整并且找不到本部署的其他 incarnation 写过的目标，结束宽限，原因是 `no_restart_evidence`。
- 找到本部署旧写者，或者 catalog 不可达、目标无法解码使发现不完整，进入 `active`。
- `active` 在两个条件中先成立者处结束。catalog 提交和对象删除都有实际远端效果寿命保证，且自本次
  FE 启动起已等满相应寿命与安全余量，结束原因是 `lifetime`。接受本次启动的部署级全覆盖声明，
  结束原因是 `declaration`，结束状态在本次进程内锁存。

观察日志 `NOVAROCKS_MV_NAME_STARTUP_GRACE` 带 `state`、`incarnation`、`prior_incarnations`、
`discovery_complete` 与 `end_reason`。以 `ended` 判断部署级名字宽限结束。其它准入仍按各自合同运行。

### 声明的授权范围

结束名字宽限的声明必须绑定当前部署、进程 incarnation 与本次 launch nonce。它覆盖本部署除当前进程
之外的全部 incarnation，包含发现日志列出的写者，也覆盖因发现不完整而未被列出的写者。
写入方断言这些旧写者的 catalog 提交和对象删除已经收束。单凭旧 FE 退出只能说明它停止发起新操作。
声明的 `source` 应指出实际远端效果已收束的证据。

按以下顺序操作：

1. 确认旧 FE 和远端效果已经收束，记录来源与确认时刻。
2. 用新的 launch nonce 启动 FE。观察这次进程的 `incarnation`、已发现的旧写者和发现完整性。
3. 以同配置的权限保护写入下文声明文件。部署级声明省略 `[[target]]`，设
   `all_other_incarnations_settled = true` 并列出全部已知旧写者。
4. FE 每个维护周期读取声明。等待日志报告 `state=ended`、`end_reason=declaration` 后继续建删。

若外部引擎先删除了旧目标，重启发现可能缺少对应的旧写者证据，迟到的按名 DROP 仍可能删除新同名对象。
其他引擎或普通表的同名建表由各自的入口控制。存储表唯一命名是进一步收紧这一风险的边界。

## 聚合 MV 的逐目标接续

新 FE 看到本部署旧 incarnation 拥有的聚合目标时，按原策略保留接续屏障。
`CALL novarocks_mv_management_status` 报告当前阶段、未收束效果、本进程身份以及声明所需的 challenge。
适合少量目标或一次性处理：

```sql
CALL novarocks_mv_management_status('ice_rest', 'analytics', 'orders_mv');
CALL novarocks_mv_resume_management(
  'ice_rest', 'analytics', 'orders_mv',
  '<challenge>', '<old-incarnation>',
  '<operator-reference>', '<evidence-reference-and-statement>'
);
```

声明先写入 `[mv_management].audit_log` 再生效。缺少审计日志配置时命令拒绝。
`novarocks_mv_set_owner` 继续按现有授权与精确目标协议运行。

部署也可以用启动隔离文件证明旧写者从时刻 T 起停止发起操作。对于聚合目标，这份证据仍需和配置的
各相关远端效果寿命结合。等满自 T 起的窗口后进行准确重新观察，由该次观察决定旧效果的实际结论。
缺少所需远端保证、目标未被声明覆盖或窗口尚未到期时，聚合目标的屏障继续生效。

## 配置与文件

下例的寿命和余量用于展示配置字段，部署值来自实际服务合同和部署约束。

```toml
[mv_management]
audit_log = "mv-management-audit.log"

[mv_management.catalog_commit_guarantee]
lifetime_ms = 60000
safety_margin_ms = 5000
basis = "provider-service-contract"
source = "Iceberg REST 服务合同 §4.2"

[mv_management.object_deletion_guarantee]
lifetime_ms = 120000
safety_margin_ms = 5000
basis = "deployment-enforced-bound"
source = "对象存储删除请求由入口网关约束"

[mv_management.startup_isolation]
evidence_file = "/var/lib/novarocks/mv-startup-isolation.toml"
launch_nonce = "${ENV:NOVAROCKS_MV_LAUNCH_NONCE}"
```

结束名字宽限的部署级声明示例：

```toml
deployment = "prod-a"
for_incarnation = "0199a1c2-3d4e-7f01-8a2b-3c4d5e6f7a8b"
nonce = "launch-7f3c9a"
isolated_incarnations = ["0198f0e1-2c3d-7e90-8b1a-2d3c4e5f6a7b"]
all_other_incarnations_settled = true
isolated_at_unix_ms = 1726650000000
source = "Deployment audit: catalog and object deletion work for every prior FE incarnation settled"
```

只为聚合目标提供原隔离时刻的声明可省略 `all_other_incarnations_settled`，也可通过 `[[target]]`
限定 catalog、database 与 name。FE 拒绝未知字段、不匹配的部署或 launch、把自身列为旧写者、未来时刻、
缺失来源，以及用逐目标声明断言全部旧效果已收束。当发现日志列出旧写者时，全覆盖声明需要把它们列入
`isolated_incarnations`。

## 相关

- [可见元组维护与严格恢复](../../adr/ADR-0278-mv-maintains-visible-tuples-and-replays-verification.md)
- [FE/BE 分布式部署](distributed.md)
- [可替换 FE 与 drain](disposable-frontend.md)
