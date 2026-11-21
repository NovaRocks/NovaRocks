---
id: ADR-0114
title: "Closed frontend state family manifest with manifest-owned persistent prefixes"
domain: [frontend-state-families]
status: active
supersedes: []
superseded-by: null
partially-superseded-by:
  - id: ADR-0243
    scope: "GC first-observation as a registered Accelerator family. The closed manifest and prefix ownership for remaining frontend state families remain active."
    link: "ADR-0243-statement-authorized-iceberg-metadata-maintenance.md"
date: 2026-08-26
provenance:
  - "PR: https://github.com/NovaRocks/NovaRocks/pull/984"
  - "discussion: 2026-08-24 / 2026-08-25, lake-native single source of truth and pluggable acceleration"
code-anchors:
  - "novarocks/frontend/src/state_family/manifest.rs (StateFamily)"
  - "novarocks/frontend/src/state_family/classification.rs (StateFamilyClassification, PersistentKeyPrefix)"
  - "novarocks/frontend/tests/state_family_conformance.rs"
---

## 问题

Frontend 进程里有几十种「状态」：外部 catalog 挂载、backend 期望成员、MV 定义投影、GC 观测、schema
缓存、在飞的 DML/维护/统计/刷新记账。哪一种可以持久化到 StateStore，持久化之后重启该不该恢复、克隆到另一个
deployment 该不该复制、版本不认识时该重建还是该报错？在此之前这些问题没有单一答案，只能逐个模块去读代码。

## 背景与执行事实

- 每个 owner 各自在自己的模块里定义 key 前缀与 schema version。新增一个持久 family 不需要声明分类、
  权威来源或 wipe 语义，也不会有任何检查失败。
- 唯一声明过保留/克隆策略的是 GC owned-ref observation，但那个策略类型没有任何 consumer，是一座孤岛。
- 更糟的是遗留：DML/维护/统计/MV 的运行态 owner cut 完成后，control-plane incarnation 与 lease 记录仍在
  每次 FE 打开时写入 StateStore，而生产代码里已经没有任何读取方。一个「没有 owner 的持久 family」能长期存在，
  正说明缺少闭合登记。
- 湖是用户数据与已发布语义的唯一共享真源。FE 本地状态因此只可能是三类之一：外部期望态的投影、只属于当前
  进程/attempt 的运行态、可从外部权威确定性重建的加速态。第四类（既非投影、又无外部真源、却要求持久）正是
  必须消灭的东西。

## 考虑过的选项

1. **文档化一张表 + 评审纪律**。零实现成本，但下一个新增 family 不会失败，等价于没有约束。
2. **运行期注册表 + 启动断言**。能发现未登记 family，但只在跑到那条路径时才发现，且注册表本身可被绕过。
3. **源码形状检查作为永久 guard**（例如「没有模块再出现这个前缀字面量」）。前缀字面量一移动就通过，保护的是
   源码长相而不是行为，与本仓库对永久 guard 的要求冲突。
4. **闭合分类 + manifest 独占前缀 + 行为门**（采纳）。

## 裁决

建立一个闭合的 frontend state family manifest：

1. 分类是编译期穷尽的三选一：`ExternalProjection`、`ProcessRuntime`、`Accelerator`。每个 family 恰好登记
   一次，并同时声明权威/重建来源、记录版本、retain/clone/wipe 策略。
2. **`ProcessRuntime` 在类型层面不可能携带持久前缀**：前缀只存在于另外两个变体的数据里。给运行态配一个持久
   前缀是编译错误，不是一条需要有人记得的约定。
3. **持久前缀与记录版本只在 manifest 中定义一次**，owner 模块从 manifest 取值。`PersistentKeyPrefix` 的构造
   函数是 `pub(super)`，因此 owner 可以读前缀但无法自己铸造一个——「第二个定义点」在结构上不可表达。
4. `ALL` 由穷尽链在 const-eval 期推导：登记了 family 却忘记挂进链条，构建直接失败。
5. 完成态的行为门是**扫描真实 store 内容**：打开 FE 不得写入任何 durable 记录；写入发生后，每个 key 必须归属
   于某个已登记且分类允许持久的 family。扫描从空 key 到全 `0xff`，因为退役的 coordination key 以 NUL 开头，
   锚在任何可打印前缀上的扫描都会假绿。
6. StateStore 因此不再是「FE 通用 metadata / attempt 权威」。它只承载 manifest 登记的投影与加速态。

## 接受的妥协（诚实记录）

- **manifest 是应用内部事实，不是 SPI 契约**。跨 crate 的状态（例如 provider 自己的 store 内部机制）不在
  它的管辖范围内，所以它只能保证「FE application 不偷偷长出第四类状态」，不能保证整个进程没有别的持久化。
- **前缀集中会一次性触及多个 owner 模块**。这是纯机械改动，但它把「未登记 family 不可表达」从纪律变成结构，
  因此接受这次集中带来的 diff 面积。
- **backend 期望态没有登记项，这是一次执行期修正**。它曾被登记为 `ExternalProjection`，因为当时 frontend
  还持有一条 durable membership 记录。BE 自注册（ADR 域 `cluster-membership`）删除了那条记录之后，backend
  期望态的唯一权威是外部 orchestrator，frontend 从不投影它，因此它根本不是一个 frontend 本地状态家族。
  代价是：manifest 现在**不覆盖**这份期望态，「集群该有几个 BE」的语义要去 membership 域找，不在这里。
  好处是「本地运行态没有持久 carrier」第一次真正成立，不再附带例外条款。
- **行为门覆盖不到完整语句面**。进程内测试驱动不了需要真实 BE 与湖的 DML/`ANALYZE`/`OPTIMIZE`/MV 刷新，
  这些交给产品形态验收。覆盖缺口是显式写在测试文档注释里的，因为一个覆盖不足的扫描和一个覆盖完整的扫描
  在输出上看起来一模一样。
- 这套结构**不阻止**有人把一个真正该是运行态的东西登记成 `Accelerator`。它只强制作者必须写下重建权威和
  wipe 策略，让错误的分类在评审中显形。

## 何时重新评估

- 当出现一个确实需要持久、又确实没有外部重建来源的状态需求时：那意味着三分类不够，必须重新讨论而不是偷偷
  加第四类或把它塞进 `Accelerator`。
- 当某个外部权威（orchestrator、controller）开始要求 frontend 持久化它的期望态投影时：那会重新引入一个
  `ExternalProjection` 条目，需要重新审视「谁的期望态值得被 frontend 投影」这条线。
- 当加速态需要跨 deployment clone 的真实运维需求落地时：`ClonePolicy` 目前只有声明，没有执行器。
- 当 manifest 需要跨 crate（例如 backend 也有自己的 family）时：现在的应用内部形态就不够了。
