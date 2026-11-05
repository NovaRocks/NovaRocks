# Memory behavior and evidence map

The historical M02a map below follows accepted spec revision 3 and approved
implementation-plan revision 3. Current attribution/lifecycle changes are described
in the M02b and M02c sections; historical receipts are not current-HEAD receipts. Execution receipts are
recorded in the approved plan; a suite name alone is not a passing receipt. Retain the
behavior of explicit grants, external bounds and shared-holder exposure; do not
retain unused APIs merely to keep historical round trips compiling.

## Baselines and P00 receipt

- Historical M02a P00 execution baseline:
  `b1d13989c50231ee7a5031163255b2607c5176c0`.
- Historical Reservation performance comparator:
  `af9d0591676c75ede5fb53c2d64949e2d80667c5`. Interpret prior comparisons against
  this exact revision; the active comparator and measurement entrypoint have retired.
  Do not copy its mechanism into the new core.
- P00 ran `cargo test -p novarocks-memory` on the current baseline on
  2026-09-30: exit code 0; 147 tests passed across 13 targets; zero failures.
  Local receipt: `/tmp/mem-m02a-baseline.log`. This was the existing core and
  observation baseline, not the candidate protocol.
- P00 ran `python3 tools/ci/check-memory-dependency-boundary.py`: exit code 0,
  PASS. The core declared/resolved normal closure was empty; its dev closure
  contained no first-party, Arrow or Tokio capability. No memory crate appeared
  in the StateStore API normal closure.
- Linux performance collection, new concurrency models, workspace validation
  and native 1FE+3BE verification have not run at P00.

| Existing baseline target | Passed | Long-term behavior to preserve or replace |
|---|---:|---|
| Library unit tests | 74 | Identity, policy, metadata, observation and core transitions; review old-mechanism tests individually |
| `allocator_observation` | 6 | CountingAllocator facts, coverage and source quality remain production contracts |
| `charge_ownership` | 3 | One live allocation has one capacity owner; replace the old Charge layout |
| `conservation` | 7 | Root and ancestor conservation; add independent-domain and root debt counterexamples |
| `control_branch` | 5 | Separate control capability; replace policy-only installation with actual prepaid floor |
| `floor_and_idle` | 8 | Protected floor, idle returns and truthful lowered-limit reporting |
| `holders_contracts` | 8 | Shared holder exposure does not multiply live backing |
| `idle_return` | 1 | Return genuine idle rights, without revoking active rights |
| `metadata_bounds` | 11 | Typed metadata limits; separate activity count from residual record bytes |
| `policy_projection` | 1 | Product dimensions and policy version are distinct from physical capacity |
| `quantized_refill` | 2 | Watermark/hysteresis behavior; separately count growth-gate visits |
| `reservation` | 10 | Historical shared-leaf mechanism is removed; its round trips are not a candidate oracle |
| `transitions` | 11 | Explicit authorization, O-to-L conversion, refusal atomicity and settlement after close |

Passing this baseline does not close the known root-capacity counterexample:
capacity 4, fulfilled 4, uncovered allocation 2, all released, then a new
account must not obtain 6. That target belongs to the new conservation suites.
P00 did not run this counterexample on the old implementation.

## Implemented candidate suites

| Stage / acceptance | Suite | Required behavior |
|---|---|---|
| P01 / V1 | `ledger_domains` | Independent domains retain independently redeemable F; split commits source and destination atomically; O-to-L does not double count |
| P01 / V1 | `ledger_debt` | Capacity 4-to-6 counterexample; one domain E cannot be offset by another domain F; accepting real facts is separate from requesting optional new F |
| P01 / V4 | `dynamic_capacity` | Unique CapacityWriter; target zero/recovery; checked invalid targets; floor is charged once and survives shrink/drain |
| P01 / V2,V4 | `hierarchical_control` | Funded boundaries have zero parent/root interaction; refill stops balance mutation at adequate parent slack; ancestor freeze prevents new down-grants; growth-gate/shrink handshake |
| P02 / V3,V5 | `owner_lifetime` | FactToken identity and final access; origin thread exit followed by remote free; generations do not replace allocation count/slot pin lifetime proof |
| P02 / V3 | `residual_handoff` | Teardown and seal are both required; payload survives account retirement; unique cursor; target close and handoff race; common-ancestor commitment remains unchanged |
| P02 / V3 | `metadata_bounds` | Bounded active slots; separately byte-charged stable record/index storage; checked overflow and typed exhaustion |
| P02 / V3,V6 | `residual_metadata` | Many historical residuals do not consume active slots; payload plus metadata transfer without new capacity, storage or slot acquisition; slab backing keeps an owner |
| P02 / V3 | `teardown_deadlines` | Task terminal, future Drop and timeout cannot imply real I/O exit; deadline exceeded preserves responsibility; deterministic exit events |
| P03 / V2,V5 | `thread_stock` | Stock derives from local domain rights; stock miss is distinct from query-limit denial; scope detach before await; no per-allocation query Arc traffic |
| P03 / V1,V2 | `step_settlement` | Workset 4 / stock 1 / allocation 1.5 covered locally; genuine E reported below quantum threshold; funded attach/settle/detach remain local |
| P03 / V5 | `hook_contract` | Every successful, failed, miss, threshold, remote-free and late-free branch has no heap allocation, blocking lock, parent traversal or callback |
| P03 / V4,V6 | `idle_drain` | Capacity reduction, shortage candidate and explicit reclaim all progress; no later poll required; active rights/floor protected; one static scan plus bottom-up round converges |
| P03 / V6 | `shortage_settlement` | Previously published covered free is settled before recheck; incomplete coverage gives Pending; completed shortage carries exact versions, coverage and age; new free starts follow-up |
| P04 / V6 | `refusal_classification` | QueryLimit, Pool, ImpossibleRequest, Closed, Invalid and MetadataExhausted remain distinct; request and constraint identities/versions survive |
| P04 / V6 | `snapshot_consistency` | Settled commitment and active live samples are separate; classification is one committed version; no fake instantaneous full-tree sample |
| P04 / V6 | `pressure_projection` | Historical M02a rule: handoff preserved U/N. Current rule is U=ΣC_query; actual Work teardown can lower U while reclassification preserves root C/N; residual origin is diagnostic |
| P04 / V8 | `allocator_observation` | Existing physical allocator and coverage facts survive core replacement |
| P05 / V7 | `stock_races` | Production-thread stress complements explicit loom exploration without replacing it |
| P05 / V7 | `ledger_loom`, `owner_loom` library models | Real synchronization seam or documented field mapping; growth/shrink, activate/seal, free/handoff/cursor, coverage/recheck and last-free/reclaim |

All behavior tests use independently issued rights and real transitions as
their oracle. Do not infer expected authorization from the candidate's own
aggregate result. Explicitly reject arithmetic overflow or invalid identity;
do not use saturating subtraction to conceal lost responsibility.

Models must report search bounds and completion. A timeout is not exhaustive
proof. Token identity, final access and record reuse require a separate
lifetime review and Miri/ASan where supported; logical models alone do not prove
address safety.

## Consumer inventory and removal boundary

| Current consumer | Required neutral interface/behavior |
|---|---|
| Worker `query_context.rs` | Store/clone Account; `create_account(Work, ExternalRef)`; one account per query execution |
| Native Adapter `native_query_context.rs` | Install Work policy from the existing query limit; the existing MemTracker remains the actual allocation mechanism in this slice |
| Frontend `metrics/management.rs` | Authority/account snapshot, P/B/H and C/L/F/O projections, capacity remainder, bound quality, activity count and control capability presence |
| Server `main.rs`, `app_config.rs` | One injected authority per process; B+H within P; real prepaid control floor before work admission; explicit composition/configuration refusal |
| Server `memory_observation.rs` and Native Adapter `backend_metrics.rs` | AttributingAllocator, retained CountingAllocator comparator, allocator/physical readings, coverage and unknown-source quality; existing jemalloc/cgroup facts |
| Role composition, BackendApplication and ExecutionRuntime | Inject/forward the same Arc authority; no second capacity authority |

No production allocator outside the memory crates consumes the historical
Grant/Charge/ExternalBound/Reservation/Holder/wait/reclaim/pressure mechanisms.
Their long-term grant/bound/holder semantics still need candidate coverage.
`novarocks-memory-arrow` has no downstream dependency and can be removed with
its workspace/Cargo entries, tests and benchmarks. Production Execution
Arrow/Chunk types are outside that deletion scope.

The Native Adapter test in `native_query_context_tests.rs` uses
`request_grant(4096)` only to check that tracker charges have not migrated into
the account. Update its neutral-interface assertion; do not keep a dead grant
API for that test. Other business types named reservation or holder are not
automatically deletion targets.

Remove the old shared Reservation, its protocol/model/benchmark and unused
wait/reclaim/pressure framework. Update exports, Cargo.lock, the dependency
guard and mutation fixtures, functional memory boundary guidance, and final ADR
status coherently. Keep the zero normal dependency rule, neutral dev closure,
and StateStore API boundary as actual Cargo graph checks.

## 当前维护边界与历史证据

当前阶段仅维护功能与数据正确性。stock/attribution成本manifest、bench binary、比较和采样脚本已退出，不再给出持续成本验收命令。旧P00、M02a/M02b记录只解释原版本和输入，不能作为当前HEAD通过；历史未验证Linux成本不补为PASS。Loom/Miri、unsafe地址/别名/生命周期、并发和独立预期全部保留。

相关核心/helper修改沿现有Rust测试链路；本地模型按其既有显式要求执行，不因测量资产退役减免。

The current M08a regression targets include Server `memory_observation`,
`memory_limit`, `cgroup_memory`, authority composition/configuration tests,
Native Adapter allocator/physical metrics and admission policy tests, Frontend
management JSON tests, and the core `allocator_observation` target.

Final integration must separately record workspace check/test and the native
1FE+3BE scenarios `query-lifecycle/distributed-baseline`,
`query-lifecycle/client-abandonment`, and
`query-concurrency/16-64-256-governance`. These verify current consumer
regression; they do not prove new stock/residual allocator wiring.

M02a defines the bounded teardown input and rejection protocol. Real push,
pull, poll, network/object-store, blocking/JNI and cleanup owners must supply
I/O kind/identity, cancel origin, maximum inflight work, actual exit condition,
D_io and its assumptions before integration/G4. A mocked deadline proves the
core preserves responsibility; it does not establish a production I/O bound.
M02b/M03/M08b/M10/M11 retain their own downstream acceptance duties.

Active scopes and external bounds retain their required control owners. A
production FactToken uses process-lifetime record storage and may survive the
authority facade's drop; it does not retain a query/account pointer. Authority
drop never substitutes for actual application or I/O exit. Final free and exact
reclaim, rather than origin identity or a zero-byte sample, govern storage safety.
Inactive settled authorization and pending drain domains are observed separately
from active scopes and hook live samples.

## M02b：尺寸分段归属的行为与证据映射

本节记录当前实现的测试入口；执行收据由 approved plan 保存，列出测试名不代表最终验收通过。[ADR-0167](../../../docs/adr/ADR-0167-size-banded-allocation-attribution.md) 是当前长期合同：64 B lane、8 B RecordRef/FactToken、512 B 阈值、尾部 8 B、Q=1 MiB；观测 lane 不授予资金，不消费 stock。

| 验收面 | 真实测试/模型 | 应证明的性质 |
|---|---|---|
| V1 格式 | `attribution_format`、lib `lane::token` / `attribution::tls`、Server `server_binary_smoke` | 非对齐尾部、原对齐/原 Layout、溢出失败、small 无 TLS；真实 GLOBAL 在 jemalloc/System 两构建 |
| V2 释放/保活 | `attribution_release`、`owner_lifetime`、`lane_exhaustion`；L1/L5、`owner_loom` | 同/远端晚 free 一次；槽 pin 和强 owner 保活；回收后不访问；旧代次拒绝 |
| V3 resize | `attribution_resize`、`attribution_explicit`；L2/L3 | 固定地址与必然搬迁后端各覆盖六条尺寸路线，断言地址关系、用户前缀、来源/count/两段事实；受控失败保持旧块，R1 切换不重不漏 |
| V4 作用域/explicit | `attribution_scope`、`attribution_explicit` | 嵌套/unwind、跨线程 poll、Pending/Ready/Drop、spawn 不继承；explicit 优先且恢复 outer |
| V5 政策隔离 | `domain_split`、`lane_lifecycle`、`attribution_scope`、`attribution_hook_contract`；Server/Worker 回归 | 创建/绑定观测 lane 不 qualify、不消费 stock；失败可见，不制造 SQL 拒绝 |
| V6 原生装配 | system `memory-attribution/observation-families`、`query-lifecycle/distributed-baseline`、`query-lifecycle/client-abandonment`、`query-concurrency/16-64-256-governance`；Server smoke | cross-process 1FE+3BE 独立进程观测/取消/退出；all-in-one 仅 smoke |
| V7 组成/对账 | `attribution_reconcile`（harness=false）、`attribution_explicit`、`domain_split`；lib `attribution::readout`、Native Adapter `backend_metrics` | 环境 small 只计进程；R1 small 成功后且仅一次；零尺寸无事实；flush 后 tagged 对账，盲区 signed |
| V8 分类 | `lane_lifecycle`、`residual_handoff`、`pressure_projection`；L4、嵌套祖先 `owner_loom` | stop 不改 class，Task→存活 Work 仍 Query；Work teardown U 可降，root C/N 重分类不降；来源不改 |
| V9 关闭规模 | `lane_membership` | 按目标成员计访问步骤，与其他账户/历史 lane 无关；不用耗时充当 oracle |
| V10 批量 | `attribution_batching`、lib `lane::slot` / `attribution::readout`；L1/L2/L3/L4 | hook 返回后余额 <Q，字节先于 count/unpin；在途无先验上界，Q×sampled pins 不是结清证明 |

L1–L5 直接执行生产 `SlotCore`、状态字和回收接口的 loom 原子实现：L1 未 flush +1/远端 −1；L2 计数不变 resize；L3 R1 跨阈值两段发布；L4 teardown/晚 free/flush/reclaim；L5 reuse/陈旧身份。`owner_loom` 覆盖账户/祖先交接与归还，`ledger_loom` 覆盖增长/债务/容量目标。每个 builder 为 max_threads=4、preemption_bound=2、max_branches=20000；完成仅说明有界搜索完成，超时或分支错误不是 PASS。

本地模型入口不接默认 CI，不下载工具链或组件：

```bash
tools/ci/memory-model-checks.sh --loom
tools/ci/memory-model-checks.sh --miri
tools/ci/memory-model-checks.sh --all
# Optional: select an already installed nightly, never install one here.
MIRI_TOOLCHAIN=nightly-YYYY-MM-DD tools/ci/memory-model-checks.sh --miri
```

脚本以 `--locked --offline` 执行 Cargo，Miri 预检已安装 nightly、miri、rust-src；缺项非零退出并报告，`--all` 在缺项时不先跑 Loom。Miri sysroot 在 rust-src 的 `library/` 内准备，使用其随附的 vendored std 依赖，不联网。Miri 使用 System 后端、Tree Borrows 与 strict provenance/symbolic alignment，并关闭 isolation（readout 读取 SystemTime；isolation 只限制宿主访问，不放宽 UB 检查）。选择 Tree Borrows 的原因见 ADR-0167：Stacked Borrows 把 std Box/Arc 交给 `dealloc` 的指针权限收窄到 `size_of::<T>()`，任何经该指针读取请求长度之后带内元数据的 allocator 都会被拒绝。Miri 覆盖 lib lane/TLS/readout、全部 `attribution_*`、旧 allocator observation，以及驱动资金域 FactToken 发布与释放的 `owner_lifetime`、`lane_lifecycle`；验证非对齐 token、realloc/受控失败、System segment、TLS，以及 owned store 在最后访问之后释放 backing。生产 store 的 segment 永不释放，回收后的逻辑晚访问不产生地址错误，Miri 看不到；这部分最终访问证明（I4）由 Loom 模型和静态复核承担。token 不含指针，不提供 provenance 豁免。独立 `attribution_reconcile` 避免 libtest 后台分配污染对账；压力测试不代替 Loom/Miri。

收敛边界：Miri 结论以 Tree Borrows 为别名模型，不等于 Rust 最终别名模型下的证明；当前不维护Linux成本采集入口；历史成本缺口保持未验证，不以本次功能通过关闭。P00 同一 33 套件选择集记录 822 cases /806 PASS /16 FAIL /0 SKIP，最终 SQL 必须与该失败集合比较，不能声称 all 全通过。当前指标/模型/定向测试不能证明所有生产 query/R1 已接线或硬内存治理交付。


## M02c：owner 作用域、记录容量与角色诊断

本节列出行为与原生入口，运行结论仍由本轮执行记录保存；不把历史收据当作当前源码通过。长期合同由 [ADR-0197](../../../docs/adr/ADR-0197-observation-owner-shards-and-coverage-shadow.md) 部分替代 ADR-0167，尺寸段、token、资金资格与真实释放规则保留。

| 验收面 | 目标 | 独立预期与边界 |
|---|---|---|
| V1–V3 完整 future | `attribution_future_scope` | 测试持有真实块并读尾部来源；构造、迁移 poll、输出、未 poll/取消/完成后的就地 Drop、构造/poll/析构 unwind；!Unpin 对象地址稳定，外层恢复 |
| V4–V5 分片句柄 | `attribution_lane_shards`、`lane_membership`、`lane_lifecycle` | 顺序线程选择不同记录，同线程重复选择相同；整组成员与汇总；未覆盖态缺口、停止与无新成员；不是性能证据 |
| V6 发布竞态 | `owner_loom` | 关闭、退役与祖先关闭相对组发布只见全组或空组，registry 与成员一致 |
| V7–V8 遮蔽 | `attribution_shadow`、`attribution_scope`；execution `kernel_memory_observation_tests` | 未覆盖/已封口 owner 嵌套服务与另一查询，构造/poll/Drop 不借用外层；显式 owner 优先，未覆盖 explicit 跨尺寸 resize 仍遮蔽；资金域失败保留原外层回落 |
| V9 无效证据 | `teardown_deadlines` | 不关闭、不封口、不改资金状态；有效重试保持残留交接与真实释放 |
| V10–V12 角色与读数 | `attribution_roles`、`attribution_hook_contract`；lib `attribution::roles` / `attribution::readout` | 表错误与显式冻结、角色 token 归属和跨线程/角色释放；锁住表时 hook 仍能完成；真实大块、R1-only小块与零字节强owner验证两区构成；真实块的buffered正负事实抵消后，独占slot pin仍保留记录，flush后才转待回收；按角色之和独立核对 |
| V13–V14 隔离与恢复 | `lane_region_exhaustion`、`lane_exhaustion` | 真实生产记录容量的观测区占满后资金域与 request_domain 仍可授予；所有分片被 tagged、R1 small 或强句柄保留，释放后在追赶预算推出的调用界内恢复 |
| V15–V16 存储与配置 | `lane_loom`；lib `lane` / `authority` | 同一边界 segment CAS 唯一发布、旧地址稳定；配置上限不得超过资金区 |
| V17–V18 出口 | Native Adapter `backend_metrics`；system `memory-attribution/observation-families` | 文本/JSON 保留 signed 样本、role刷新、两区状态；最终场景逐个 BE 检查256条永生记录、unknown>0、容量闭合和故障为零 |
| V19–V20 消费者与地址安全 | execution 本地 allocator 消费者；`tools/ci/memory-model-checks.sh` | 定向覆盖真实本地 allocator；全部有界Loom和Tree Borrows Miri，模型不替代地址验证 |

正常修剪与耗尽追赶预算由 `lane/shards.rs` 的常量给出；恢复测试据实际预算和区容量计算次数上界，不延长超时制造通过。记录可以只被 R1 小对象、slot pin 或零字节强句柄保留，不能按每条最少512B估算容量。

新增 `attribution_*` 源目标自动进入既有 Miri 循环。`lane_region_exhaustion` 占满真实 130944 条观测记录，规模大，只在原生进程测试中执行，不进入 Miri；owned 小库与所有 attribution 目标仍保护地址/生命周期。测试独立预期来自自己持有的真实分配、来源身份、证据和释放；不以进程总量差分推导归属。TLS const且无析构，在当前平台无法经公共接口制造不可用状态，测试不伪造该结果。

当前阶段没有吞吐、延迟或RSS验收门，分片争用收益尚未测量。上述机制与指标通过也不代表生产query/R1作用域、角色装配或资金硬治理已接线。
