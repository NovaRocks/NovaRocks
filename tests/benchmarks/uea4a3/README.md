# UEA-4A-3 集中性能测量协议

本目录固定 driver 直接 poll scan 流（ADR-0159）的**一次**集中对照测量。基线 B0′ 是任务分支基线
`94304f0154b6af25cc7d0378557ae2ab3ccd15ff` 依次 cherry-pick 三个既有缺陷修复得到的
`63237c9314827b9747b11860eb223fcee433b82d`；candidate 是开发完成后的本分支加上同样三个修复（测量时的 candidate
提交为 `6f984fdf2eea6786e798acbb5dea8cb6cd559d59`）。三个修复各自独立提交为 PR #1080、#1081、#1082，因此两侧只差
UEA-4A-3 本身。负载、口径和门限在看到 candidate 结果之前写定于 `workload.json`（`frozen_on`），测量启动后不修改；
若必须修改，作废本轮并重新冻结。下文与报告中的“B0”均指 B0′。三个修复合入 main 之后，复测的 B0′ 就是合入修复的
main、candidate 就是本分支 rebase 后的 HEAD。

三个修复都由冒烟运行暴露，都与 UEA-4A-3 的改动无关，但不修则对照不成立：

- PR #1080（withhold a create that lost to a closing context）：CreateTask 输掉与上下文关闭的竞争时，任务仍以 Live
  登记、但从未向观察者发布；回收它时观测不变式断言在持有注册表锁时 panic，锁中毒后 BE 退出。冒烟中 B0 的一个
  BE 在 Paimon 短查询负载下因此退出。修复把这种任务在观测源里登记为 withheld。
- PR #1081（park a sealed success's cancellation wait）：已成功封存的取消节点过了 deadline 后，`cancelled()`
  对已过期的 deadline 反复 `sleep_until`，每个仍在等待的中继都空转到持有者 drop。FE CPU 随已服务的查询数增长
  （冒烟中后段负载时 FE 占 3–6 核），先跑的负载与后跑的负载拿到的 CPU 不同，且哪一侧跑的查询多、后续负载就被
  拖得更厉害。修复让封存成功的等待在 deadline 过后只等通知、不再计时。
- PR #1082（retire a concluded query actor without cancelling its delivery）：supervisor 在查询得出结论后回收 actor 时，actor 把这次
  关闭当作停机，以 ServerShutdown 取消 work；客户端若仍在读结果，语句就以“server is shutting down”失败（冒烟中
  约百分之一的查询，两侧都有）。修复让关闭请求带上原因：停机照旧取消，回收只取消尚未得出结论的 work。

## 拓扑、构建与配置

- 两侧都用同一 toolchain 的 `--release` 构建，同一台机器、同一 1FE+3BE，由 candidate 的 release
  `novarocks-system-tests` 以 `--launch-profile performance` 启动，`--binary` 分别指向两侧 server。
- 配置相同：基础配置是 Paimon READY fixture 的 `base-server.toml`。场景用 overlay 替换两个角色的凭据列表
  （FE 为 `object-store-metadata`，BE 为 `object-store-data`，都是 fixture 的静态凭据），BE 另加
  `config_groups[*].driver_workers`。performance profile 清空子进程环境且只接受类型化的密钥名，场景把
  worktree 的对象存储密钥以 `NOVAROCKS_UEA4A3_S3_ACCESS_KEY_ID` / `NOVAROCKS_UEA4A3_S3_SECRET_ACCESS_KEY`
  传入。两侧都不设任何 scan 专属键：B0 以其默认值保留原 scan 线程池（CPU 核数）与队列，candidate 已删除该池。
  driver worker 数两侧相同（默认按 CPU 核数，或 `single-worker` 组固定为 1）。两侧启动的有效配置语义摘要
  （`effective_launch_config_semantics_sha256`）必须相同。
- 每个会话先 `SET query_timeout = 300`（`query_timeout_seconds`），客户端 socket 超时再多 30 秒，使超时以
  服务端的查询超时错误出现并计入。
- 运行顺序：每个配置组先跑 B0、紧接着跑 candidate，机器在组间的漂移对同一组的两侧相同。

## 输入

- 场景经 REST catalog 在 `uea4a3_perf` 库建表：`perf_wide` 为 32 个文件、每文件 25 万行一个 row group
  （`c1..c5` 由 `v` 派生，`c3 = v*2654435761 % 1000003`），`perf_small` 为 400 个 1000 行的单 row group 小文件。
  每次 INSERT 带 `ORDER BY`，使写入端收到一个 chunk、写出一个文件。表已按预期形状（文件数与每文件行数）存在时复用，
  冒烟运行会预先建好。`perf_point` 为 1 个 1000 行的文件，供控制面短扫描使用。报告记录全部数据文件（路径、
  行数、字节数）的 sha256，所有运行必须相同，否则作废。
- Paimon 用 READY fixture 的 `append_none` 与 `pk_sequence`（fixture 很小，这两项测的是短查询的调度开销，
  不是吞吐能力）。`wide_scan_delayed_io` 经 delayed S3 proxy（每个请求延迟 5 ms，proxy 在 runner 进程内）读同一
  张表，只作归因，不设门。

## 负载、窗口与统计

- 每个负载先热身 30 秒（丢弃），再测 3 个 120 秒窗口。每个客户端在自己的连接上闭环提交，窗口内提交的
  查询属于该窗口 cohort；窗口到点后停止新提交并收齐全部终局。吞吐 = 成功数 ÷（首次提交至最后一个 cohort
  终局的总时长），包含尾部 drain。错误与超时计入且不能丢弃；gated 负载的 candidate 出现任何错误或超时
  （含热身）即不通过，B0 出现错误则该轮作废。
- 延迟为提交至完整结果（结果行先原样收齐再取终局时间，摘要计算不计入延迟）；分位数取 nearest-rank
  `ceil(p×n)`，每轮单独计算后取三轮中位数。
- 结果一致性：每个查询记录结果行排序后的 sha256 前缀，每个客户端首次见到某个结果时另记其行数与前 3 行（有界
  预览）。以 B0 的多数结果为参照，candidate 的每个查询都必须返回参照结果；B0 的少数结果作为基线缺陷连同预览
  单独报告，不删样本、不使本轮作废（B0 没有多数结果时作废）。
- B0 噪声：三轮的 `max(|x_i/median-1|)`，吞吐 ≤10%、p95 ≤20%；超出时报告环境不稳定，不删样本。
- 每个窗口采样三个 BE 与 FE 的 RSS 峰值和 CPU 秒数；candidate 另记 `novarocks_driver_dispatch_latency_seconds`
  三段时延（按 BE、按轮，取直方图桶上界）与 `novarocks_scan_stream_pending_total` 的窗口增量（B0 没有这些指标，
  记为缺失）。

## 控制面

默认组先在空闲集群上把 KILL 样本查询完整跑一次（4 轮 `sha2(…, 512)` 覆盖 `perf_wide` 全表）：若它在 KILL 延迟的
2 倍（1 秒）内就完成，KILL 样本不可能有效，场景失败并要求重新冻结。然后在空闲集群上每秒提交一次短扫描
（单文件 `perf_point` 上 `v = 7`，60 个样本），并执行 20 次 KILL：先提交样本查询，500 ms 后 `KILL QUERY`，分别记录
KILL 语句往返与被取消查询收到 1317 的时间；查询先于 KILL 结束或返回其他结果的样本记为无效，不丢弃。再在 4 个
客户端运行 `wide_scan` 的满载期间重复同样的短扫描与 KILL 样本，并采样这段时间各进程的 CPU 与 RSS；背景负载至少
120 秒，并持续到样本采完。heartbeat 与 RF 发布时延没有可用的测量入口，不测，报告中如实列为未测。

## 门限

- 正常负载（`gated`）：吞吐退化 ≤15%，p95 增长 ≤25%（均为三轮中位数的对比），无错误，结果与 B0 相同。
- 控制面（candidate）：满载下短扫描与 KILL（被取消查询收到 1317 的时间）两类的 p99 各自 ≤2 秒，且 ≤同类空闲
  对照的 3 倍；样本必须完整、无无效样本；按操作类型分别统计，不跨类或跨 BE 平均分位数。
- 不预设加速倍数；DOP=1、单 worker、有序路径与 CPU 密集负载的结果同样呈现，不因形态而豁免门限。

## 运行

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo build --release -p novarocks-server -p novarocks-system-test-runner
python3 tests/benchmarks/uea4a3/run.py \
  --runner target/release/novarocks-system-tests \
  --b0 "$UEA4A3_B0_BIN" --candidate target/release/novarocks \
  --paimon-fixture "$PAIMON_FIXTURE_DIR" \
  --output reports/uea4a3/performance
python3 tests/benchmarks/uea4a3/compare.py reports/uea4a3/performance
```

`run.py` 按上面的顺序各启动一次集群，校验每次运行都写出回执，最后调用 `compare.py`；`compare.py` 从原始样本
重算全部统计，写出 `comparison.json` 与 `comparison.md`。正式运行只接受本目录的 `workload.json`，且 candidate
工作树在测试与报告目录以外不得有未提交改动。冒烟运行用另一份短窗口 manifest 加 `--smoke`，只验证链路，不评估
门限。`python3 tests/benchmarks/uea4a3/test_protocol.py` 离线检查比较逻辑。

## 冻结记录

- 2026-09-25 初次冻结。
- 2026-09-25，任何正式测量之前：基线由 B0 改为 B0′（见开头），先后三次各加一个共享修复。
- 2026-09-25，任何正式测量之前：结果校验由“B0 必须恰好一种结果”改为“以 B0 多数结果为参照”。冒烟中 B0（每 BE
  1 个 driver worker）的 `wide_scan` 24 次里有 1 次返回了不同的单行聚合结果，而多数结果与按数据定义算出的正确值
  一致；该少数结果不是漏读或重读整文件、也不是按 1024–65536 行对齐的块。原规则会让一个基线自身的错误结果作废整轮
  对照，新规则把它作为基线缺陷报告，candidate 仍须每次都返回正确结果。
- 2026-09-25，任何正式测量之前：`paimon_append`、`paimon_pk` 与控制面背景负载的客户端由 8 降为 4。冒烟中 8 个
  客户端的短查询使每个 BE 的 native ingress `ordinary` 类（默认 8 运行 + 8 等待，ADR-0157）饱和：
  `native_ingress_rejections_total{reason="waiting_capacity"}` 增长，查询以 ResourceExhausted 失败，并伴随 300 秒
  查询超时与 FE 暂时看不到可用 BE（`attempt manifest requires a nonempty frozen endpoint snapshot`）。这是两侧共有
  的控制面容量上界，不是本项测量对象；保持产品默认配置，把并发放到其容量以内，上界本身另行跟踪。
- 2026-09-25，任何正式测量之前：控制面短扫描改读新增的单文件表 `perf_point`。原查询在 400 个文件的 `perf_small`
  上即使空闲也要约 1.7 秒（dev-opt），不是短扫描。
- 2026-09-25，任何正式测量之前：KILL 样本查询由 2 轮改为 4 轮 `sha2`。按默认 DOP（执行线程数的一半）和原生
  fanout 验收中每行约 1 µs 的双轮开销估算，2 轮版本在空闲集群上约 0.7–1 秒即完成，500 ms 的 KILL 延迟无法保证
  落在执行期内；同时加入上述空闲预跑校验。门限与其余负载不变。

## 已知局限

- 单机 1FE+3BE 与 MinIO、runner（含 delayed proxy）共用 CPU；结果是同机同输入的相对对照，不是容量结论。
- Paimon fixture 很小，Paimon 行只反映短查询调度开销。
- heartbeat、RF 发布时延未测；driver 派发时延只有 candidate 有数据，交接 driver scheduler umbrella 的 E0，
  不代表 E0 已完成。
