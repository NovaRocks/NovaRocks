# NovaRocks system scenarios

`novarocks-system-tests` is the imperative, process-boundary test frontend.
It owns scenario discovery, actors, bounded deadlines, oracle composition and
failure reporting. `novarocks-cluster-harness` remains the only application owner of
1FE+NBE configuration, launch, readiness, topology, faults, restart and cleanup.
Shared test-support owns the exact OS Child/process-group/wait mechanics through
its independent payload monitor.

Follow [test governance](../../docs/guides/development/test-governance.md) for
asset admission, approval snapshots, new exclusive attempt roots, retention and
report interpretation. Pass a fresh root through the existing `--artifact-root`;
the runner's `create_dir_all` does not enforce exclusive ownership. Existing
ignored report directories and the commands below remain supported. Preserve
unresolved failure materials and record the actual output location; a local
path is not a shared evidence handoff.

先准备主图并绑定真实程序：

```bash
tools/ci/dev-test.sh prepare logs/system-attempt/artifacts.json
source tools/ci/lib/build_capabilities.sh
export NOVA_CI_ARTIFACT_RECEIPT="$PWD/logs/system-attempt/artifacts.json"
server="$(ci_bound_binary novarocks)"
runner="$(ci_bound_binary novarocks-system-tests)"
"$runner" --list
"$runner" --list-default
"$runner" --binary "$server" \
  --config tools/ci/fixtures/system-scenarios-base.toml \
  --artifact-root "$(mktemp -d)" --cluster-size 3 --timeout-secs 300 \
  --only query-lifecycle/distributed-baseline
```

`--fixture-budget-secs` 表示整个CLI调用的预算（默认3600），先于scenario launch_config副作用准入；`--timeout-secs`进一步收窄每个scenario工作期限，不延长原完整期限。fixture、FE/BE首次启动/重启/执行、嵌套producer与teardown继承同一run identity和时钟，并通过独立 `NOVA_FIXTURE_RUN_STORAGE` 继承固定运行范围；普通JDBC位置不依赖namespace location，rename与显式/CTAS使用同一归属谓词。运行期到期由原monitor停止并等待真实进程组退出，即使payload原生exit0也保持FAIL；清理使用原完整期限的reserve，清理错误独立保留。每个prepared cluster有唯一deployment ID，重启稳定。vended保留私有REST，只有准确run scope的MinIO共享，真实双STS语义保持；未确认私有writer退出时不清共享IAM/S3。机器维护与TTL边界见 [ADR-0191](../../docs/adr/ADR-0191-machine-shared-fixture-and-bounded-scratch.md)。

CLI package 为 `novarocks-system-test-cli`，原 package `novarocks-system-test-runner` 为库，保留原 `CARGO_MANIFEST_DIR`。完整 `--list` 包括显式与暂停场景；默认 `--list-default` 与无 selector 使用同一集合，排除兼容场景与其它显式专项。Paimon 及 mixed UEA4 暂停项继续显示 DISABLED 并在 fixture/launch 前拒绝显式选择，production provider 仍注册。`performance` launch profile 选择 clean harness 配置，不表示测量实验。

## 声明与收束材料

UEA-5F 的六个 `exchange/` 场景在真实 1FE+3BE 上覆盖 gather/broadcast/shuffle 的有序背压、多查询 probe/build 进展、接纳后丢 Unit 应答与准确重放、grant 订阅新 generation、超传输期限的真实 Source 停泊、以及压力下短查询和 KILL QUERY。它们使用原受管 runtime 的私有 Hadoop Iceberg warehouse 和普通 SQL 写入种子数据，不调用共享 fixture 维护或 benchmark producer。

这六场景显式使用 floor1/max2/floating1/window2、单次传输 1000ms、既有 256KiB 共享发送预算及 8KiB batch target（不是新的消息拒绝边界）；没有修改生产默认值。种子保留六个各 4096 行的独立 Parquet 文件，并检查原文件大小的最小 split weight 前提；实际等权放置和单文件 page 边界共同保证 shuffle 流有第二个 Data Unit。`connector/distributed-writer-row-level-small-slots` 单独保留原 row-level 数据、oracle、DOP 和期限，仅使用 floor1/max2/floating0/window1 与原 authenticated writer append hold；原 `connector/distributed-writer-row-level` 继续使用默认配置。发现与影响声明是原 Scenario 集合的事实，注册存在不代表运行通过。

故障走原受认证的 BE→BE H2 actor：预转发 hold 与接纳后丢应答分开记录，场景用真实 statement nomination、BE process roster、stream/sequence/请求摘要精化候选。长等要求同 receiver node、fragment instance、driver 和 block epoch 的实际 `InputEmpty` park/wake；缺失 build Unit 与 probe 输入背压分别观察，不冒称 probe driver 的 dependency 事件。重放必须到原接收作者的 Idempotent 计数；订阅重建必须保留原需求 stream 并取得完整新代 snapshot。

Slot 指标中的 `in_use` 含已授予未送达的位置，不等于 payload 占用。只有原 owner `available=1` 才可用于界限和归零判断；不可观测不是零，也不跨时间拼装容量界。SQL 结果逐行核对独立唯一整数域。QueryOwner 的 deadline/cancel、原 RPC/解码/发送 guard 的退出、SQL actor join、query resource 收敛及角色 shutdown 各自保留，不能互相替代。本轮受影响场景验证不覆盖用户延后的 benchmark、完整默认或兼容集合。

`--list-declarations` 不启动 fixture/cluster，输出 schema 1 的完整注册集合，包括 default、explicit、compatibility、paused/paused_reason，以及原生 `declaration` 中的 `impact.paths`、`env_inputs`（value/file-content）、`initial_state`、`basis`、`heavy`。它是 `dev-verify.sh` 的场景影响来源；shell 不另存场景表。声明初始状态只接受已有隔离、复位或不可变事实，未证明的状态保留 null。暂停项仍拒绝执行；显式/重项不能当作暂停。

每个场景在 cluster shutdown 和 fixture teardown 后另写 schema 1 `scenario-teardown.json`，无论断言通过还是失败；`scenario-evidence.json` schema 5 保持原合同。收束材料分别给出 fixture launch_config、teardown 状态/错误、外部 resources，cluster launch、shutdown、error 和 partial_cleanup。部分 cluster 启动失败会注明由 monitor 补证，不能由一次总退出码推断清理。缺记录或缺必需 monitor/资源终态一律未知。`tools/ci` 调用 owner 在执行前后写 invocation 和范围身份，关联这两份材料；直接 CLI 的本地输出仍需完整材料才能组合。

仅绑定现成外部端点的场景尚无 G5 的隔离/复位保证；其 `initial_state=null`，即使单次成功也不能借此沿用历史结果。不增加共享 fixture 的删除或清空权限来补这个缺口。当前尝试的真实断言、前置、退出事实继续保留。

新增不随进程退出消失的资源 owner 时，必须在产生首个外部副作用前调用 cluster-harness 的 `resource_registration::register(root, owner, id, label)`，并在所有成功、失败和 shutdown 路径用 `Registration::settle` 写出实际清理结果。`list/collect` 递归观察本次 PID 新登记的资源；损坏/未知 owner 和未 settlement 保持 unsettled，cleanup_failed 不得被幂等 shutdown 覆盖。句柄 Drop 不代表释放。现有隔离 REST/MinIO 的登记覆盖 Compose 项目和私有 runtime；hook 镜像构建纳入登记范围，但释放记录不证明构建镜像已回收，也不新增镜像删除权。

登记仅记录 owner 事实，不授予回收权限。人工或工具后续确认必须只观察准确资源，保存独立确认，原清理违约永久保留；`dev-verify.sh confirm-cleanup` 不删除任何资源。规则与层结论见 [测试治理](../../docs/guides/development/test-governance.md#影响声明发现与开发入口)。

## 产物与有限授权

普通与受控步骤使用唯一显式 `--binary` 主 Server，原生 owner 按实际选中动作冻结角色需求、会话和 statement。dev 编入能力但不授予动作，标准 release 排除能力；缺能力/授权在 spawn 前失败。无授权角色也可运行同一 dev Server。兼容变体从实际 Cargo artifact 独立复制，启动及 restart 前保持准确程序与 grants 绑定。路径、identity、runner profile 不代替实际 FE/BE 自报，unknown 保留；原始 stdout/stderr、退出、清理失败和重跑材料分开保留。详见 [构建与授权](../../docs/guides/development/test-governance.md#构建选择与控制授权)。

## 受管角色、诊断与材料

角色从 `ManagedCommand` 构造受管启动，monitor 持真实 Child 并独立于 runner/test 进程完成组信号与实际 wait；harness 继续决定配置、marker、授权、原期限和 restart。收到客户端取消、角色 terminal、monitor 清理、场景断言通过是不同事实。成功路径检查退出和所需日志后释放普通诊断；失败路径保留 primary、cleanup 和原始输出，monitor 的失联回收成功不掩盖失联事实。

角色输出定位信息关联原始 stdout/stderr 和监护记录，不能按路径猜测 combined log。实时完整行观察仅接收完整 UTF-8，并有 1 MiB 诊断缓冲界限；原始双流与完整 capture 不受该界限截断。source clone 在 stop/restart 后继续 pin 原始材料，日志替换或截短显式失败。正常 stop 本身不证明后续场景断言成功，断言所需日志必须在释放前消费。

普通 nextest 首次受管调用懒建 run 级材料租约；外层测试启动嵌套 System CLI 前先 `ensure_run_retention(deadline)`，并继承原生上下文/状态根。直接调用 CLI 按调用进程归属，不自动获得 nextest 的逐测试隔离或正式汇总。普通失败/未释放材料进入默认三次已收束调用窗口，活动 pin 和缺终态的材料不被淘汰；正式 wrapper 材料继续按证据保留合同处理。

本库只接管明确受管的 payload，不宣称 Docker、外部数据库实例、逃离组的后代或所有 raw spawn 已被 containment。容器中的锁/运行材料目录需显式挂载支持的本地 APFS/ext/XFS/Btrfs/tmpfs，overlay 拒绝受管登记。API、raw 调用范围及 native Linux x86_64 未验证状态见 [test-support README](../test-support/README.md)，正式结果的 `execution-index.json` schema 1 和原生报告位置见 [CI README](../../tools/ci/README.md)。

## 兼容专项与程序需求

原生 `--list-compatibility [--only <exact>]` 列举兼容集合，`--list-binary-requirements --only <exact>` 输出所选场景完整生命周期的需求并集。shell 不维护另一张场景注册表。独立入口为：

```bash
tools/ci/native-compatibility-ci.sh
tools/ci/native-compatibility-ci.sh --only native-compatibility/mixed-build-same-island
```

| 场景（均在 native-compatibility/） | 性质与主 Server 以外的程序 |
|---|---|
| mixed-build-same-island | Compatible 共存；诊断 identity 不改变兼容材料 |
| raw-establish-compatibility-admission | 原生准入/拒绝，无额外程序 |
| other-island-hard-cut | Compatible 与 OtherIsland 的硬拒绝 |
| target-island-cutover | Compatible、OtherIsland；后续目标岛启动也需要 OtherIsland |
| island-drain-and-replacement | Compatible、OtherIsland；预冻结 replacement 程序与 grants，实际 restart |

上述场景消费本地 SQLite/动态 catalog 基础配置、harness 私有端口/目录和 Native trust，无 REST/MinIO/Spark/外部 MySQL 前置。cutover 同时两套 1FE+3BE，需要容量和 peer 授权；drain/replacement 需要 Unix SIGTERM。缺前置保留失败或 required-but-not-run，不用 fake 编排结果替代。原生 owner 为新增场景声明实际 fixture 和后继 launch 需求。

默认功能 CI 不准备变体；专项按需求并集每种只准备一次，不运行普通全量。何时选择专项与维护责任见 [兼容选择策略](../../docs/guides/development/test-governance.md#兼容专项的选择)。

Most scenarios build their Iceberg warehouse on the local filesystem under
the harness runtime directory. The vended credential scenarios and the default
`table-maintenance/target-replacement` scenario start an isolated Iceberg REST
and MinIO fixture and require the prepared fixture inputs and Docker service.
Target replacement uses REST because it deliberately overlaps a detached
OPTIMIZE job with another writer; Hadoop OPTIMIZE waits for completion.
The base config supplies SQLite StateStore and no shared
`[connector.object_store]` credentials. The generated
`$NOVAROCKS_FE_CONFIG` / `$NOVAROCKS_BE_CONFIG` pair also works
when started through the exact `--role all-in-one --fe-config ... --be-config
...` command; it adds object-store settings that local-filesystem scenarios
do not read.

凭据消费者场景为 `connector/vended-credential-refresh`、
`connector/vended-credential-targeted-deadline`、`connector/vended-credential-late-close`
与 `connector/vended-credential-unreachable`。逐个用准确 `--only` 和
`--cluster-size 3 --timeout-secs 180` 运行。fixture 识别真实 BE principal，
仅在对应刷新请求到达后 hold；late-close 在 BE[0] 使用开发能力下
显式授权的 exact-authority trigger。

Scenarios run sequentially. On failure the runner prints action history,
process diagnostics, the retained runtime/log directory and an exact rerun
command. Successful scenarios explicitly stop their own process group and
remove the generated runtime directory.

## In CI

两个按需期限场景为 `result-delivery/producer-wait-query-deadline-separation`
与 `result-delivery/original-query-deadline-cancels-producer-wait`。分别用正常
MySQL session 的 `query_timeout=120` 和 `2` 执行 `SLEEP(35)`，验证独立预期
Boolean true / 原 ERR 1317，再在同一 socket 执行 `SELECT 1`。每例独占
native 1FE+3BE，使用 `--launch-profile performance --timeout-secs 300` 和独立
artifact root；不构建数据 fixture，不参与默认基线。公开 root census、BE
query owner 计数与双零 FE/BE barrier 分别证明实际排空；ERR、客户端关闭和
进程 shutdown 保留为不同事实。35 秒是旧期限错误的复现输入，不是性能门。

`result-delivery/many-small-rows-cross-segment` 验证 200000 小行跨 Native
1 MiB 段时的准确顺序与字节；`result-delivery/large-row-cross-u24` 验证一个
17825860 字节大行跨 MySQL U24 packet（17 个合法 1 MiB 字符串字段）。输入在
`docs/testing/mem-1-m07/inputs/result-delivery-wire-boundary-v4.json` 冻结，客户端
明确声明 frozen profile 的 64 MiB logical packet allowance，独立
Python literal oracle 生成期待摘要。原始 actor 以 64 KiB scratch 流式校验
packet sequence、列数、行数、length-encoded 字段及数据摘要，不积累整行。
每项还要求真实 BE Task 创建，并保留前后两次连续零值的 FE window/governance
和 BE reservation/ingress 快照。这里只证明 wire 正确性与公开资源收敛；closing
满池、精确 alias 退出、全部 W=2 容量矩阵和 transport 测量仍是独立验收。

`result-delivery/producer-exit-participation-retention` 在严格校验 metadata 后暂停
客户端读行。客户端在 connect 前申请 4 KiB receive buffer，并记录 OS 实际值；
不修改 server profile。一个合法 1 MiB 字符串形成 S+8 Native 行字节，要求
连续两次观测同一个仍由 context 持有的 root：两个 Data position、End 已发布、
producer guard 已退出且 task 已为退休记录、最终 End 尚未 ACK。恢复后核对
独立冻结 schema/packet/行字节摘要并等待公开 owner 收敛。输入为
`inputs/root-context-retention-freeze-v1.json`。新增 BE ownership 指标只在
1024 个 context/root 扫描位置内作非阻塞 census；锁忙或覆盖不完整时明确
不可用并省略 ownership family。它不包含移出 context 的固定 backing 或其它
Arc 尾部，归零不证明 allocator dealloc，也不代替 ACK-only/ClosingRow 满池门。

`tools/ci/local-full-ci.sh` runs this registry as its own stable stage,
between the server binary smoke and the SQL suites. The stage discovers
default scenarios through `--list-default` and runs each one with a single `--only`
invocation, so every scenario gets an independent summary row, log and
artifact directory. It selects and reports only — `tools/ci/lib/system_scenarios.sh`
holds no cluster lifecycle of its own.

The same `query-lifecycle/distributed-baseline` scenario is reused by the
FoundationDB and MySQL StateStore provider gates as a feature-binary
coexistence smoke. Read it narrowly: it proves a feature-enabled binary still
completes a standard native topology, query and cleanup, not that any query
used that provider.

The `native-ingress/*` scenarios require the real 1FE+3BE launch. They send
authenticated raw gRPC requests to a BE to check header-stage refusal,
pre-prost resource checks, and the ordinary/control message boundaries. The
current-gate case reads the BE management `/metrics?type=json` surface before,
during, and after a runner-held original asynchronous Native ordinary request,
so an available gate can be distinguished from an unknown source. Actual Node
resources are read only from an available original Node snapshot; protocol
Context/ticket counts do not become capacity limits. The explicit
`blocking-saturation-control` case holds all eight original ordinary running
permits before Worker transfer and proves a ninth ordinary request waits.
Control Cancel and a real Context's Quiesce/Release must still complete. A second
Context uses its declared short lease: management must observe its actual
execution custody retire before any application probe, followed by the exact
LeaseExpired receipt. This validates independent timer progress without letting
the probing RPC itself trigger expiry. The hold awaits the original test-role
socket/cancellation asynchronously and does not fill a blocking thread pool.
`async-scheduling-pressure` runs four distributed SQL queries, waits for real
Exchange shuffle bytes while one remains active, and checks a small control
receipt; it is a liveness check under ordinary async work, not proof that all
async worker threads can be saturated safely.
`partial-body-deadline` leaves a gRPC request body half open and checks bounded
termination, the `running_deadline` counter, holder release, and a later
control receipt. HTTP/2 may end that half-open stream with a reset, leaving no
readable gRPC status; a fully received unary request has separate status checks.
`registry-contention-control` retains its existing scenario name but now
holds one exact Context's Accepted preparation in the existing supervised
Native host action. While that original action remains held, another Context
on the same BE must establish, renew its exact next lease, fence and Release.
The held Task must still replay as Planned and not Installed before release;
afterwards its real installation, covered stop, release and owner-idle barrier
must complete. The runner uses the existing preparation-hold rendezvous; the
poller stays free and no global registry mutex or lock-wait metric is involved.
Each selected case records its individual probes in `scenario-evidence.json`;
the scenario must execute at least one probe to count as passed. These are
correctness and coarse regression checks, not throughput benchmarks.

The `native-creation/*` scenarios also require the real 1FE+3BE launch and
check frozen task creation across the process boundary. An Accepted receipt
proves Worker ownership; Installed is a separate runtime-installation fact.
The `accepted-preparation-control-races` case checks preparation/control races
and authenticated covered-observation refusals. Native subscriptions require
nonzero generations; cursor-only requests cannot enter the production stream.
Preparation count, byte and per-context position charges remain until the job
actually exits. FE deployment positions remain occupied after Accepted or an
unknown RPC outcome, and its window W must fit the exact backend's advertised
per-context preparation capacity P. These are scenario and implementation
contracts; a listed scenario does not establish a passing acceptance gate.
The historical `normal-close-count-capacity` and
`normal-close-byte-capacity` names now cover retained close facts rather than
record capacity limits. They use fixed corpora of 2 and 32 receivers, retain
the original close facts while admitting fresh work, and wait through the
real 120-second retention horizon before checking that the forgotten stream
is refused. Neither case configures a retained-record count or byte cap;
terminal publication alone cannot stand in for physical exit or horizon expiry.
`frozen-replay-and-membership` sends authenticated raw creates to one BE: a
legal create, the identical request, and the same task identity with every
body fact changed. Both replays must retain the winner's exact entity and
return its current monotonic status, with no second apply marker and no lease
renewal. Creates under another
frontend process, another attempt or another backend's identity must read no
receipt. An initial domain naming an edge the descriptor never froze fails
preparation after Accepted; that spent identity remains retained, so a later
legal body cannot replace it. A replay after the context is released must
apply nothing.
`creation-payload-lifetime` reads the FE task-creation gauges, which fall only
when a payload's last holder drops it. An answered create must release its FE
replay payload while its statement still runs, and the static plans must stay until
the statement ends. With `create-task-ack-drop` armed, the lost
acknowledgement must be resent as the same frozen create and answered by
identity; every task must be priced, frozen and applied exactly once. A
cancelled statement must release everything it froze. BE preparation input and
its P/count/bytes charge remain owned until the actual preparation job exits;
these FE gauges do not prove BE budget release.
`fixed-plan-recovery` runs one delayed read cleanly and once with its admitted
BE killed before any row is read. The recovered run must complete on attempt
2 and freeze exactly as many static plans as the clean run, so the recovery
attempt encoded none of its own. A full per-target transport window cannot be
produced at the default task transport budget without a capacity probe. That
cross-target admission is therefore checked by the frontend's real admission
pass and transport supervisor composition tests, not by these scenarios.

### 内存归属观测

默认场景 `memory-attribution/observation-families` 启动原生 1FE+3BE，验证分布式查询及连接退出取消前后的每个 BE `/metrics`。证据写入场景目录的 `memory-attribution.json`，关联独立 PID 与 HTTP endpoint；两个进程尺寸段、固定归属分类、记录状态、故障与批量采样族均须存在。S1 未接生产观测 owner，query/residual/service 物理归属事实为零，资金域的生命周期记录独立存在；256 条 immortal unattributed 记录由角色与栈分片组成，`unattributed_bytes{role="unknown"}` 大于零。资金区与观测区分别导出容量、active/residual_retained/pending_reclaim 占用及残留记录上的 tagged/R1 small 字节，两区容量加永生记录数等于总容量；七类归属故障为零。签名的对账、账本盲区及残留字节为独立采样，不要求在并发 scrape 期间瞬时归零。

## 迁入的显式功能保护与维护频率

下列场景独占原生1FE+3BE，以有限输入和独立行袋/协议预期检验性质；timeout只停止挂死，不是性能门。选择时用准确selector，每次独占artifact root；`--launch-profile performance`是clean配置，原有需要fault的场景继续使用自己的fault-scenario要求。

| 性质 | 原生selector | 输入/实际退出 |
|---|---|---|
| 慢读与完全不读客户端 | `result-delivery/slow-client-progress`、`result-delivery/unread-client-progress` | 正常MySQL、冻结行/字节预期，进展/取消与owner收敛分开 |
| 独立并发结果 | `result-delivery/concurrent-results-drain` | 有限actors，失败取消其他live peers、实际join，保留primary及cleanup错误 |
| range/短查询 | `uea4/iceberg-range-functional`、`connector/iceberg-short-range-concurrent` | 多文件A4及独立4096行单文件manifest；准确行序/算术、LIMIT后退出、并发结果 |
| 同provider排队进展 | `catalog/provider-read-queued-progress` | held真实Parquet读、同provider waiting，释放后两个查询正确并退出 |
| planning责任与取消 | `uea4/catalog-planning-responsibility`、`uea4/catalog-planning-no-fe-quota`、`uea4/iceberg-held-read-cancellation`、`uea4/catalog-planning-smoke` | Iceberg/Paimon真实READY；FE/BE责任、held请求精确终态及公开收敛 |
| warm异质delete | `connector/iceberg-delete-warm-concurrent` | typed独立Java corpus receipt，固定旧/latest snapshot及私有mutation，有限并发和真实退出 |
| cold delete | `connector/iceberg-delete-applicability` | 完整冻结native manifest包含corpus/anomaly/scale；真实多文件与split/page-source退出 |

range/planning输入准备与校验见保留的 [range fixture](../benchmarks/uea4a2/README.md) 和 [planning fixture](../benchmarks/uea4a4/README.md)。原目录名不赋予测量责任。正常held转发仍要求真正forward完成；取消路径按同request精确connection的上游终态观察，不能把其他连接关闭或ERR当退出。

结果owner相关writer/buffer/ACK/取消/helper改动必跑对应场景；range/delete/planning/DOP由Connector/scan领域按provider/cache/range/admission/fixture改动必跑。高成本显式组每周由维护者手工相关领域轮次，记录缺输入与raw FAIL，不扩默认registry选择。周期调度与共享存储尚未交付；G7a/G7b已取消，不作为未来服务承诺。FE statement observation/auth保留，运行级计时collector退出；实际能力/profile见每次产物自报，未运行范围不能据接口存在宣称通过。

常规 Rust 使用唯一 workspace/tests/bins/dev 主图和 nextest 逐测试进程；不再维护 normal/control 七包串行分图。实际开发 cfg 决定能力是否编入，准确有限授权继续逐角色生效。HMS/MySQL 专用 feature/fixture 仍显式选择，默认主图结果不代替其完整矩阵。维护命令及独立 doctest 见 [测试治理](../../docs/guides/development/test-governance.md#原生-rust-执行进程回收与原始结果)。

MV 旧场景及独立 oracle 保留，禁止扩展旧屏障/CALL/directive。7B3 唯一承接非聚合冷恢复、配置/paused、known-committed、Unknown/迟到提交和管理接续；7B4 承接 spine SUM/COUNT、聚合状态、聚合冷恢复与后续刷新；7B5 承接 V/S 身份、发现/CREATE/DROP、REST 路由和实际 materialized S/fallback；7B11 同格式切换承接 `mv_recovery.rs` / `mv_uea7_storage.rs` 的 document graph 与 shared-document-revisions。替代保护实际通过后逐项退出，最后消费者退出再删共享载体；G2 不新建启动期缓存缺失或通用擦除接口。

### MV documents (explicit)

The explicit MV document stages are `mv/document-expiry-channel`,
`mv/document-consistent-read`, and `mv/document-keyed-writes`. They require private
REST/S3/Spark fixtures and native 1FE+3BE. The expiry and consistent-read stages
also require the matching test-controls FE artifact, with finite target/token
authorizations created by the scenario. They are excluded from `--list-default`.
Choose the original deadline before launch; the consistent-read stage performs
300 source commits plus 300 ordinary refreshes and can use `--timeout-secs 1800`.

These stages independently parse the frozen inline wire protocol; they do not
call product document codecs. Every external mutation retains before/request/
HTTP-status/response/after files in a new `document-evidence` subdirectory. Spark
retains its exact SQL, command locators, stdout/stderr and process exit. The
private recording REST proxy retains method/path/query only, forwards request
and response bodies unchanged, and distinguishes ordinary `snapshots=refs`
loads from the Full expiry load. Scenario evidence records inline property and
snapshot-record byte counts and historical interpretation diagnostics. A unit
or registry pass does not replace running these native stages on the integrated
candidate.

### Catalog publication (explicit)

The `catalog-publication/*` scenarios require the local publication Java image
inputs, the normal fixture BOM, and native 1FE+3BE. They are excluded from the
default suite. Select each exact name with a fresh `--artifact-root`:

- `catalog-publication/late-commit-observation` holds an ordinary ALTER after
  server-side requirements validation. SQL must report Unknown while metadata
  remains old and exactly one mutation was forwarded. Releasing the original
  request must yield a real service success and actual exit. A second phase
  holds an already committed request before handler reload, commits successor
  metadata externally, then requires the original SQL success to preserve its
  applied schema and the successor's property.
- `catalog-publication/stream-deadline` streams real responses for table commit,
  create table, namespace and view operations under an explicit 500 ms catalog
  evidence budget. SQL must return Unknown while the slow body still makes
  progress; bodyless drop responses must preserve Applied. Each body owner
  must exit. The outer query/scenario deadline must leave at least 15 seconds
  of margin; the normal production policy remains unchanged.
- `catalog-publication/expiry-concurrency` freezes expiry at the real JDBC
  boundary, adds a tag pointing to an exact candidate snapshot externally,
  observes the original CAS conflict, and requires reprepare to retain that
  ref, snapshot, and manifest-list object. Exact source rows remain readable.
- `catalog-publication/manifest-reprepare` freezes rewrite manifests, appends
  through a separate logical catalog, observes a real CAS conflict, then
  requires successful reprepare to preserve the append snapshot, its exact
  manifest-list object and the independently specified row set.

Each scenario owns a private catalog/warehouse/proxy and retains its original
SQL actor through actual join. Service NDJSON and before/after metadata are
written inside its normal scenario artifacts. Expected row/snapshot/ref facts
come from the scenario inputs and the competitor's real REST result, not the
maintenance candidate planner. Object checks sign independent HEAD requests
against the owned MinIO bucket. These cases do not claim coverage for every
expiry interleaving, all protection roots, or cross-generation coordination;
those have additional provider/application tests and separate acceptance rows.
A before-forward proxy hold cannot substitute for the late-request scene.


### JSON membership lifecycle

The explicit `query-lifecycle/json-membership-full-eos`, `json-membership-cancel`
and `json-membership-capacity` scenarios protect the null-aware mark join's real
build/probe lifecycle. Run them on the main dev artifact with native FE+3BE when
changing SQL mark planning, signed JSON IN equality, join build/probe or the
JSON normalization or Variant rendering. Each invocation creates private Hadoop Iceberg tables under
its managed runtime; it needs no shared external fixture.

The first two scenarios nominate an actual mark Task with an actual remote output
destination from the frozen graph. An authenticated exact statement/Task action
holds its sole build publication after normal build-input sealing, and the bounded
existing observation channel records the mark probe's real scheduler Dependency
wait. Ending that action releases the original publication. Cancellation uses
public KILL and checks original-connection reentry. The capacity scenario builds many distinct small JSON keys, consumes
all JSON producer values in an independent control and checks a small mark build
under the same target query budget. The target preserves the original QueryRoot
tracker's typed Resource failure and actual Task execution site; the original
runtime ancestry identifies that unique limit owner. A read-only sample witnesses
actual live mark build key pages on the same BE. It imposes no budget-percentage
threshold and does not infer physical memory from conservative reservations.
The producer may be the first allocation caller refused by the shared query
budget. Samples from different events, slots, Tasks or BEs are not added to
manufacture a memory peak. No sleep or backend-count
formula supplies a stage-completion oracle; observation polling only waits for
original facts. The shared SQL actor owns kill, socket close, actual join and
statement-observation drain on success, public error, deadline and failure.

Expected row values follow the fixed integer-key three-valued oracle in
`json_membership.rs`; the SQL suite additionally distinguishes JSON `null` from
SQL NULL and checks supported/rejected clauses. These are functional lifecycle
assets, not performance evidence. Scenario evidence and independent teardown must
both be inspected; a successful SQL prefix cannot discharge unfinished cleanup.
