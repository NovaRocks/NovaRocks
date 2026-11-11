---
id: ADR-0227
title: "Metadata listing and task transport use real capacity boundaries"
domain: [provider-spi, distributed-query-lifecycle, memory-governance]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0158
    scope: "Background assertion that descriptor and single-operation fixed limits are intrinsic request limits; identity replay and the existing supersession by ADR-0161 remain unchanged"
    link: "ADR-0158-task-creation-is-frozen-once-and-replayed-by-identity.md"
  - id: ADR-0170
    scope: "SDK-listing fixed page/name/token capacity rejection and the OpenDAL 16 MiB list-response limit; public APIs, no-accounting-fork and actual-exit rules remain active"
    link: "ADR-0170-third-party-crates-are-bounded-by-public-configuration-not-forked.md"
  - id: ADR-0196
    scope: "Ruling 3 fixed 65,536-entry inventory limit; complete discovery, hint, installation and lifecycle rules remain active"
    link: "ADR-0196-hadoop-test-catalog-complete-version-installation.md"
  - id: ADR-0216
    scope: "Fixed listing-capacity refusal in the all-name listing fallback; read-only entry, BASE TABLE typing, missing-table and metadata-I/O fallbacks remain active"
    link: "ADR-0216-hms-uses-upstream-catalog-with-read-fallbacks.md"
  - id: ADR-0224
    scope: "Background table retention of fixed candidate/thin-inventory/name/token limits; document, StateStore, identity and lifecycle rules remain active"
    link: "ADR-0224-mv-document-capacity-comes-from-real-boundaries.md"
date: 2026-11-10
provenance:
  - "discussion: 2026-11-10 accepted removal of unjustified metadata-listing and task-transport fixed quotas"
  - "discussion: 2026-11-10 confirmed that Joint records responsibility and does not provide total reply-byte capacity"
  - "discussion: 2026-11-10 accepted original Client byte-budget configuration, removed the whole-Root versus single-wire-payload startup comparison and retained explicit listing scenario inputs"
code-anchors:
  - "novarocks/spi/src/connector/listing.rs (ConnectorListingPageSize, ConnectorListingCollector)"
  - "novarocks/frontend-application/src/mv/domain/readiness.rs (MvReadinessPort)"
  - "novarocks/task-codec/src/budget.rs (TransportBudget)"
  - "novarocks/frontend-application/src/task_execution/intent.rs (NativeRequestWindows)"
  - "novarocks/task-codec/src/resource_preflight/paid_reply.rs (OperationReplyDecodePlan)"
  - "novarocks/workload-control/src/joint_admission/mod.rs (ResponsibilityConfig)"
  - "novarocks-server/library/src/app_config.rs (NativeIngressRuntimeConfig, FrontendWorkloadRuntimeConfig, validate_result_retained_config)"
  - "novarocks-server/library/src/composition.rs (compose_frontend_result_profile)"
  - "novarocks/workload-control/src/result_window/retained_bytes.rs (RetainedBytes)"
---

## 问题

元数据列举与 Native 任务传输的规模应由哪些真实边界约束，如何避免把历史固定额度、组批目标和责任报价误当成协议能力或物理内存授权？

## 背景与执行事实

表示范围、远端协议约束、实际资源容量和责任追踪是不同事实。相同的字节数不能因为被不同层引用就变成新的准入依据。

| 事实 | 原作者 | 能说明什么 |
|---|---|---|
| 分页请求与续读协议 | ConnectorListingPageSize / ConnectorListingCollector | 本次页大小、token 进度与完整枚举，不是全部名称的总量额度 |
| 受信目录的 SDK 调用 | 原 generation 的 IO 准入、deadline、stop 与退出监督 | 并发和真实活动寿命，不给不可见 SDK 分配虚构字节上界 |
| MV 薄清单 | 原 StateStore 分页与后端容量 | 完整清单和实际键/值边界，不继承 LocalResultBound |
| Native 编码长度 E | 原 codec / Native method 配置 | 方法单次 request/response 的有效线长与表示范围 |
| 排队载体 Q 与 Qhat(M) | 原 intent 作者及 transport 窗口 | 线上限 M 下的排队形态上界与实际队列容量；Q 可以大于 E |
| 回复布局 D(n,M) 与实际 scanner C(raw) | generated RustLayout 与原回复解码作者 | 原请求条目数 n、方法回复上限 M 下的 requested-backing 责任上界 |
| Joint claim / NativeReplyGrant | Joint 责任事务账本及原 custody | 责任报价、持有和退休；不授予物理内存，也没有回复总字节余量 |
| Client 快照实际保留预算 | 原 ResultCapacityConfig 与 RetainedBytes | 同 Root/holder 的实际字节、复制共存和最后别名退出，默认 8 MiB；可配置而不新增 owner |
| BE Root / process retained 预算 | 原 WorkerResultRetainedLimits 与 RootInputAuthority | 输入、hydrate、scratch 和输出保留的实际资源，不是一份 wire payload 的尺寸 |

系统关系以独立类型化批序列承载，见 [ADR-0226](ADR-0226-engine-metadata-through-typed-system-relations.md)。退出旧常量载体不等于免除实际数据的 FE、传输与 BE Task 资源责任。

## 考虑过的选项

**A. 继续固定总量或把常量调大。** 历史值易理解，但不能解释为什么同一合法目录或描述符必须在该数字处失败。**设计否决**：没有协议、实现表示或实际资源依据的常量不能充当业务规模拒绝依据；更大的无依据数字没有改变问题。

**B. 删除拒绝后不再保留累计事实。** 减少计数代码，但会把实际输入、分配和保留成本从原所有者的账中抹掉。**设计否决**：退役固定比较不授权绕过原资源容量、控制预留或真实退出。

**C. 以真实方法配置、资源窗口、协议检查和原布局作者约束。** 采纳。组批目标只决定分组；排队与回复布局分别由各自作者计算。

**D. 用 Joint 报价充当回复总字节预算，或借请求/结果窗口替代。** **设计否决**：Joint 没有该物理容量；不同窗口的授权范围不能靠重命名转移。独立回复总字节治理仍是**待评估**事项，须由 Native 传输与资源所有者明确容量、准入、最后持有者退款及 Foreground/Settlement 进展。

**E. 为 SDK 内部计量而 fork 网络或运行时库。** **设计否决**：延续 [ADR-0170](ADR-0170-third-party-crates-are-bounded-by-public-configuration-not-forked.md) 的 no accounting forks。目录的受信假设、公开配置、调用准入、期限和真实退出保持明确。

## 裁决

1. **No-invented-quota rule。** Connector 列举不自加固定条目数、页数、单名/总名字字节或 token 字节额度；Hadoop 不保留独立固定列举工作区，OpenDAL 不加独立固定列表响应体拒绝。MV 薄清单不继承 LocalResultBound。实际 StateStore 键容量、后端限制、IO 并发、期限与取消继续由原所有者执行。
2. **Complete-protocol rule。** 列举必须完整，不截断后回答不存在。默认页大小 256 只是请求粒度；空续读 token、重复 token 和带续读页违反请求页大小仍是协议错误。不分页而一次返回完整大列表的服务，不因总条目或名字字节被本地固定额度拒绝。永远产生新 token 的远端只能由原 deadline/stop 结束；受信端点假设不等于恶意服务安全保证。
3. **Target-versus-limit rule。** 删除单 Task 描述符 16 MiB 与计划包编码 64 MiB 粗总量门。48 MiB 保留为组批目标；大于目标但符合方法配置与真实资源窗口的不可分操作单独成批。不得将目标重新解释为 codec、FE、BE 或启动配置的硬拒绝。
4. **Effective-method-limit rule。** FE 客户端与 BE 入口按该 Native 方法各自有效配置检查 request/response 长度，部署时保持一致；默认 64 MiB 不是配置天花板。保留 gRPC 32 位长度表示、非零及配置关系、checked arithmetic、完整帧和原 typed 身份/引用校验。没有消息配置授权就不构造或解码相应载体。
5. **Queue-author rule。** 原 intent 给出 Qhat(M)，覆盖该方法下每种排队操作及必要控制预留。运行时保留准确 Q 和 E，实际 E 在任何排队预留之前核对。配置必须让一个允许的最大请求连同控制进展所需载体进入空窗口；暂满在原公平队列可取消等待，单个不可分请求连空真实窗口都不能容纳时才明确 typed Resource 拒绝。不要从编码长度直接猜排队成本。
6. **Reply-layout rule。** 回复解码继续使用原 scanner 和 generated RustLayout，不引入另一种语义解释器。OperationReplyDecodePlan 按原请求 n 与有效回复上限 M 计算 D，原实际回复再接受同模型 C(raw) 核对。当前 checked 形式为 `D(n,M) = (K + 8) * M + 4 * size_of(TaskOperationReceipt) * n`，K 由原生成布局的局部 field/storage 成本推导。局部收费落在各自 wire key/payload 上，覆盖 raw×8、String/Bytes 的 `4*max(len,8)`、generated/typed Vec×4、Box×2，以及同一 singular/oneof 的重复线上出现；不能只按最终可见值或固定 n 棵对象树报价。
7. **Responsibility-is-not-capacity rule。** D 与 NativeReplyGrant.capacity_bytes 是责任报价，不是物理申请、保留内存承诺或有限总预算余量。Joint 不提供回复总字节准入/背压，本裁决不新增该 quota，不挪用请求窗口或 ResultWindow。报价必须随同原 n/M 与 custody 到实际最后持有者退出；责任退休与原资源的真实归还分别取证，不能用其中一项代替另一项。
8. **Original-resource rule。** 系统快照与载体保留全部实际资源事实。FE 使用原语句字节作者，传输使用原 bytes/items 窗口，BE 使用原 Task 准备所有者；复制/扩容支付真实并存峰，实际 alias 退出后才归还。取消、超时、终态与停止等待均不等于实际退出。遵循 [ADR-0198](ADR-0198-one-logical-execution-owner-for-every-distributed-intent.md) 的责任、原预算与可取消背压规则。
9. **Original-client-budget rule。** `[runtime.frontend_workload].client_result_all_objects_bytes` 仅写入原 `ResultCapacityConfig.all_objects_bytes[0]`，默认仍为 8 MiB；它是每个原 Client holder 的保留预算，不是进程总额、新窗口位置或新的资源授予。完整快照继续同 Root 实际计费，多批不减少总保留；复制/编码支付 old+new 并存峰，最后别名退出才归还。沿原配置作者检查非零、实际表示范围和 checked 算术，不增加 profile 等值门、额外最大值或并发乘积门。Client 位置、CPU、Local/Internal 与 Native 操作回复的边界不变。
10. **Whole-root-budget rule。** 删除将 BE `result_retained_bytes_per_root` 与一份 wire payload 数值比较的错误启动门。整 Root 同时保护 Arrow 输入、hydrate、scratch、元数据和分段输出，其容量必须由原 `WorkerResultRetainedLimits` 与真实授予检查；保留非零、每 Root 不大于进程及实际计费/背压。默认每 Root 16 MiB、进程 4 GiB 不变，不借此删除其余 Root renderer、分段 codec 或 typed IPC 政策。
11. **Scenario-input rule。** 大名称 listing 及其取消场景显式使用 FE Client 192 MiB、每个 BE 原 Root 256 MiB、原 process 4 GiB。FE 数值来自原大快照组件正例；BE 数值来自既有结果支持 geometry，覆盖输入与 hydrate 各 96 MiB、2 MiB scratch、1 MiB 元数据及有限分段/发送持有。它们是场景资源输入，不是产品默认、最小预算、能力上限或物理内存界。原 1FE+3BE 必须保存全部有效配置，并实际返回 257 个完整名称（256×64 KiB 加 1 字节）；不能以 COUNT、截短、分批或组件成功代替。较小真实预算的类型化资源拒绝与原取消/实际退出断言保留。
12. **Scope rule。** 除本条明确删除的整 Root 与单份 wire payload 启动比较外，普通 SQL VALUES、常量池、StaticValues、其他 fragment/plan 动态比较、exchange/根结果与其余业务额度不因本条自动删除，也不因暂未修改而获得新的正当性。历史测量 profile 与冻结输入保留为历史资产，不能再拿它们给此处运行时恢复固定总量门。

本条只部分替代 frontmatter 列出的准确范围。Task 创建版 ADR-0158 已由 ADR-0161 替代的谱系不变，同号 Parquet ADR 不在范围。HMS 版 ADR-0216 的只读与读取回退继续有效，同号执行词汇 ADR 不在范围。ADR-0224 的文档、StateStore、身份与生命周期规则保持。

## 接受的妥协（诚实记录）

- 受信 catalog 可以给出很大的完整列表；移除本地固定总量不提供远端响应或 SDK 隐藏分配的统一物理上界。期限与取消也不替代仍在执行的调用或阻塞 job 的真实退出。
- FE 整份快照和大请求仍可能碰到实际资源窗口、进程内存或传输配置；本裁决不承诺任意规模成功，也不新增 spill 或流式 RPC。默认 Client 8 MiB 不足以保留超过 16 MiB 的名称输入；开放原预算配置没有取消实际计费，也不把支持场景配置推广成默认。
- 同 Root 的 retained 预算与一份 wire payload 保护不同对象。删除错误启动比较不保证任意 Root 配置都足够；原 geometry 的实际预留仍可明确拒绝较小预算。
- **Native 回复仍缺独立总字节治理。** 有效线长与调用并发不证明 canonical 回复在后续持有期的总内存上界，Joint 的完整记责不填补这个缺口。
- 布局报价依赖原 generated RustLayout 与 scanner 的一致性。上游布局或收据形态演进时必须一起更新作者与核对，不能保留失真的上界来冒充覆盖。
- 局部组件验证、Native smoke 与完整 SQL/System/工作区验收各有范围；这些长期规则不宣称最终验收已经完成。

## 何时重新评估

- 实际需求要求限制 Native 回复的累计保留内存时，由 Native 传输与资源治理所有者独立设计真实容量、准入、持有者及 Foreground/Settlement 进展；不可仅给 Joint 加一个容量名字。
- 受信目录边界改变，或上游公开 API 能明确限制/流式消费 SDK 内部响应时，重新评估调用准入与消费者，不为计量 fork 基础库。
- generated RustLayout、protobuf storage 或 typed canonical receipt 变化时，重新证明 D 对原 scanner 的覆盖，包含重复字段和嵌套出现，不以测试样本替代证明。
- 大目录、快照或操作在真实配置下稳定触发资源压力时，先核对原资源事实、同 Root 保留量、容量与进展；需要分页数据面、spill 或更强隔离时由对应所有者另行设计。不得把场景 profile 当成最小支持预算或物理上界。
