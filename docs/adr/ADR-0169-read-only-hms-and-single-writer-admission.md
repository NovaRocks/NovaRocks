---
id: ADR-0169
title: "Catalog owners admit mutations before effects and keep single-writer jobs within statement lifetime"
domain: [provider-spi]
status: active
supersedes: []
superseded-by: null
partially-superseded-by:
  - id: ADR-0231
    scope: "StatementJob/JobAttempt and catalog-dependent job completion/cancellation in decisions 4-6; owner admission, HMS read-only and publication separation remain"
date: 2026-10-09
provenance:
  - "discussion: 2026-10-09 HMS read-only compatibility and single-writer statement job completion"
code-anchors:
  - "novarocks/connector/iceberg/src/catalog/admission.rs (CatalogAdmissionRequest)"
  - "novarocks/connector/iceberg/src/catalog/mod.rs (NovaRocksCatalog::admit)"
  - "novarocks/connector/iceberg/src/catalog/hive.rs (NovaRocksHiveCatalog::admit_operation)"
  - "novarocks/connector/iceberg/src/catalog/hadoop.rs (NovaRocksHadoopCatalog::admit_initiation)"
  - "novarocks/frontend-application/src/query_execution/maintenance/rewrite_job/direct.rs (prepare_optimize)"
  - "novarocks/frontend-application/src/statistics/attempt_executor/statement.rs (StatementStatisticsAction)"
  - "novarocks/frontend-application/src/query_execution/maintenance/rewrite_job/direct.rs (DirectRewriteAction)"
---

## 问题

一个 catalog 是否允许本次 mutation，应由谁在什么时刻回答；单写者 catalog 如何保证用户顺序提交的语句不会被引擎内部异步作业变成并发写入？

## 背景与执行事实

本 ADR 延伸 [ADR-0118](ADR-0118-iceberg-provider-private-catalog-owner.md) 的唯一 provider-private catalog owner 与操作型准入，不取代其 transaction、publication 三态、读取错误或 catalog/filesystem 分工。

Connector 的能力槽说明 provider 实现了某类操作，不说明某个 catalog 接受某次请求。Iceberg 的 `NovaRocksCatalog::admit` 接收精确操作、表或命名空间目标与发起方式；先执行 `admit_operation`，再执行 `admit_initiation`。owner 的构造器和直接 mutation 方法复用同一操作规则；能看到完整请求的 provider 入口还执行发起方式规则。

| owner | 操作边界 | 发起方式边界 |
| --- | --- | --- |
| Hive Metastore | 永久只读兼容入口；读取、元数据表与时间旅行保留，所有 mutation 拒绝 | mutation 已由操作规则拒绝 |
| Hadoop | 单写者；保留空表创建与现有 mutation，CTAS、view、应用文档管理拒绝 | `Statement` 与 `JobAttempt` 放行；`Background` 拒绝；`StatementJob` 要求提交语句等待完成 |
| REST | 现有操作规则，包括 CTAS 对标准 staging 条件的要求 | `StatementJob` 可分离执行；其余发起方式沿用现有规则 |

`StatementJob` 是建立表作业前的询问，不是作业执行 attempt。SPI `ConnectorTableJobAdmission` 用 `Detached` 或 `AwaitTerminal` 向 FE 表达完成要求；具体 catalog 类型不出 provider。作业执行上下文是 `JobAttempt`，MV 调度、维护与启动续跑等真正后台来源是 `Background`。

零副作用点必须早于全部外部产物。写会话之后 BE 会写数据；metadata maintenance、cleanup 与 distributed rewrite 在规划阶段就可能写文件。只在最终 catalog commit 拒绝无法保护这些产物。HMS 的 metadata-location 读取、比较与普通 `alter_table` 也不能构成原子条件提交。

OPTIMIZE 在产品提交 gate 内，从一个 exact connector lease 同时获取 owner 准入、物理对象和引用事实，完成要求随 `OptimizeSubmission::Submitted` 返回。ANALYZE 在建立作业 root 和入队前取得准入。两个作业均属于进程运行态，FE 重启不会续跑。

## 考虑过的选项

**选项一：按 catalog 维护能力表，或按类型增减 Connector 能力槽。设计否决。** 调用方可以简单检查布尔值或槽是否存在，但会引入第二语义来源；同一 catalog 是否接受请求依赖操作、目标与发起方式，静态表无法表达。能力槽继续表达 provider 实现，准入由 owner 针对请求回答。

**选项二：允许规划和写文件，最终提交再拒绝不支持的 catalog。设计否决。** 改动入口较少，却不能兑现零副作用 `Unsupported`；写会话与维护规划已留下外部产物。提交前的防御性拒绝保留，但不能替代入口准入。

**选项三：为 HMS 增加锁、心跳与提交核查以恢复写入。设计否决。** 一套完整协议可以改善提交能力，但当前产品定位明确是永久只读兼容入口；它会重新引入本产品已退出的 HMS 写路径。上游获得新的提交 API 不会自动改变该产品定位。

**选项四：保留 Hadoop 异步作业，让用户自行避免与下一条语句重叠。设计否决。** 用户已经顺序提交，重叠由引擎自身制造，违反单写者下顺序语句的约定。对 Hadoop 全面禁用 ANALYZE 与 OPTIMIZE 同样不能满足保留这些操作的产品要求。

**选项五：异步作业持有表写锁，其他语句报忙或等待。待评估，当前未采用。** 它可允许提交语句先返回，但要新建跨入口互斥与 busy 语义。本裁决采用提交语句等待完成，保留顺序使用方式；用户主动并发写入不获得保证。

**选项六：统一 owner 准入与发起方式，单写者作业由提交语句等待真实退出。采纳。** 所有操作族在副作用之前询问同一个 owner，FE 按中性完成事实等待，不自行判断 catalog 类型。代价是维护长语句和取消等待时间变长。

## 裁决

1. **唯一准入规则。** 每个 mutation 族必须在第一次外部副作用之前调用 owner 的完整 `admit`。范围包括 catalog DDL/ref/view、CTAS、写会话、数据 mutation、metadata maintenance、cleanup、distributed rewrite、统计和维护作业、应用文档管理与更新。新增族必须接入，不得依赖 commit 阶段兜底。缓存计划重放也必须重新按本次发起方式准入。
2. **同源规则。** owner 构造器和 mutation 方法执行同一 `admit_operation`；入口额外携带发起方式。不得在工厂之外通过具体 catalog 类型、能力表或缺失槽重建判断。
3. **HMS 只读规则。** 一切 mutation 返回明确的 typed `Unsupported`，说明 HMS 是只读兼容入口、被拒绝的操作以及 REST/Hadoop 写入替代。vendored HMS `update_table` 保持上游不支持桩；不保留写入开关、旧实现或兼容提交路径。读取现有表不受这条拒绝规则影响。
4. **单写者来源规则。** Hadoop 拒绝后台 mutation，语句和已准入作业的 attempt 可执行；语句提交表作业时要求 `AwaitTerminal`。REST 仍返回 `Detached`。这些规则不构成多用户会话或外部引擎之间的锁。
5. **真实完成规则。** Hadoop 上 ANALYZE 与新提交的 OPTIMIZE 成功返回，必须晚于该作业实际执行退出和所属资源收敛。ANALYZE 等待终态及全部 convergence；OPTIMIZE worker 等实际任务 join，释放 job scope/root/permit 后才发布 terminal。业务终态、取消意图与实际退出不能互相替代。
6. **取消保责规则。** 等待中的语句取消或 deadline 到达，先向精确作业交付取消意图，再继续等待真实完成。ANALYZE 的入队前 permit 等待也观察取消/deadline；时钟读取错误不得阻断取消交付和收敛等待。已经派发的 OPTIMIZE 目前不能中止内部执行，取消只记录意图，提交语句仍等原作业跑完，之后报告取消。失败返回作业终态错误，不把失败伪装为提交成功。
7. **准入与 publication 分离规则。** `Unsupported` 必须证明未发生 mutation、规划产物写出或作业建立。已发生可能的 publication 后仍遵守 ADR-0110 的三态与核查规则，不得重新降为 `Unsupported`。

## 接受的妥协（诚实记录）

- HMS 的旧写能力被移除，用户必须通过 REST 或 Hadoop catalog 写入。没有自动迁移，也不会因上游新增锁支持而恢复；读取兼容是保留它的产品理由。
- Hadoop 的 ANALYZE、OPTIMIZE 成为长语句。作业失败会让等待语句失败；请求取消不能承诺立刻返回，已派发 OPTIMIZE 尤其可能持续到正常执行退出。这是保持单写者顺序与实际责任的成本。
- 本规则只防止引擎把顺序语句变成内部并发，不序列化主动并发的用户会话、其他 FE 或外部 Spark/Trino 写入。单写者的使用前提仍由部署和使用者满足。
- 发起方式由 host 在创建 request context 时准确赋值，provider 不能从调用栈猜测来源；新增后台调用点必须携带正确方式。现有 operation-specific 错误出口继续保留，SQL 侧尚未统一映射为一个错误码。
- OPTIMIZE 的 `AlreadyActive` 继续返回已有行为，不把重复提交者变成原作业的第二完成 owner。表锁和统一 busy 语义仍是独立议题。

## 何时重新评估

- 产品明确改变 HMS 永久只读定位时，必须用新 ADR 重新裁决完整提交、锁、失败核查与生命周期；不能以依赖升级或 vendor patch 顺带恢复写入。
- 用户需要主动并发写入，或多个 FE/外部 writer 共享 Hadoop 表时，评估跨入口互斥、表忙语义或具备原子条件提交的 catalog；对应选项五。当前单写者等待规则不能被宣传成并发保证。
- 长维护或取消等待时间超过使用者可接受范围时，评估派发后可观察的真实取消和分阶段执行；不得通过提前发布终态或遗弃作业缩短表观等待。
- 新增 mutation 族、后台维护来源或持久化可恢复作业时，重新审计零副作用点、发起方式、完成 owner 与实际收敛条件。
- 不同 SQL 入口的 Unsupported 表达阻碍客户端判断时，评估在现有 owner 事实之上统一边界错误编码，不引入文本嗅探或第二能力权威。
