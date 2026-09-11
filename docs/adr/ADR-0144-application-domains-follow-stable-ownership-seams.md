---
id: ADR-0144
title: "Application domains follow stable ownership seams and point dependencies toward policy"
domain: [crate-boundary, distributed-query-lifecycle]
status: active
supersedes: []
superseded-by: null
date: 2026-09-11
provenance:
  - "PR: <backfill after merge>"
  - "discussion: 2026-09-11 application domain ownership and dependency direction"
code-anchors:
  - "novarocks/query-application/src/lib.rs (query application boundary)"
  - "novarocks/execution-contract/src/lib.rs (immutable execution contract)"
  - "novarocks/worker/src/lib.rs (worker-local execution policy)"
  - "novarocks/workload-control/src/lib.rs (process-local work governance)"
  - "novarocks/catalog-application/src/lib.rs (long-lived catalog generations)"
  - "novarocks/frontend/src/application.rs (FrontendApplicationHost)"
  - "novarocks-server/src/composition.rs (role composition)"
---

## 问题

查询、Worker、Catalog、产品业务、协议适配与进程组合应如何划分物理边界，才能让依赖方向表达真实决策权，又不把仍在共同演化的查询内部接缝过早冻结成公共 crate 接口？

## 背景与执行事实

NovaRocks 的 native 生产形态有独立 FE 与 BE。FE 决定 SQL application 语义、查询准备、全局协调与业务产品状态；BE 决定本进程 Task 的准入、推进与回收；Server 选择配置和实现并组合角色。跨进程消息只是这些 owner 交换事实的载体，不是第三个业务 owner。

这些职责对依赖稳定性的要求并不相同：

| 边界 | 拥有的事实与变化 | 正常依赖方向 |
|---|---|---|
| `novarocks-execution-contract` | immutable identity、descriptor、operation、receipt 与纯分类 | Query Application、Worker 和 codec 依赖它；它不依赖角色实现或执行内核 |
| `novarocks-worker` | Worker 本地准入票据、Task/context 生命周期、租约、入站围栏与收敛政策 | Backend 适配依赖 Worker；Worker 不依赖 Frontend、Query Application 或 wire codec |
| `novarocks-query-application` | SQL application port、查询观察与准备、逻辑执行、attempt 协调、结果与恢复政策 | 产品服务和 Frontend 适配依赖它；它不依赖产品实现、角色实现或 wire codec |
| `novarocks-workload-control` | 进程本地工作责任、准入、资源和结果信用 | 应用 owner 依赖它；它不依赖查询、产品、角色、执行内核或 wire |
| `novarocks-catalog-application` | 长期 Catalog generation 与退休 | Frontend 组合依赖它；它不拥有查询局部 compiler 映射或 Connector execution instance |
| 产品应用 | MV、Statistics、Maintenance 各自的业务状态、外部效果与发布决定 | 产品依赖 Query Application 的通用执行和消费者契约；Query Application 不反向依赖产品实现 |
| Native/MySQL 适配 | wire decode/encode、连接与 transport 事实 | 适配层投影应用契约；领域 crate 不依赖适配实现 |
| Server | 配置验证、provider/role 选择、唯一 owner 构造与 shutdown 顺序 | Server 依赖并组合实现；它不直接解释领域或 wire 载荷 |

SQL application、查询观察、准备与协调会共享同一组 request-local identity、binding 和错误语义。它们的接缝仍会随 optimizer、Connector negotiation 和恢复策略一起演化。Rust 的模块私有性足以阻止它们在一个 crate 内越过受限构造入口；把每个阶段做成兄弟 crate，反而会使 `pub` surface、orphan rule 和 DTO 投影成为新的长期兼容负担。

Cargo 依赖图能强制 crate 之间不能命名未声明的实现；它不能表达同一 crate 内的模块矩阵，也不能证明运行时只走唯一生产入口。因此物理 crate 与模块私有性解决的是不同粒度的问题，生产组合和行为测试仍需证明真实路由。

## 考虑过的选项

**选项一：继续把所有 FE 应用职责放在一个角色 crate。** 一个 package 内移动类型最便宜，也不会产生公共 DTO。但 Catalog generation、工作治理、查询协调和产品业务会继续互相命名实现；依赖方向只能靠 review，Backend 与 Frontend 的共同契约也容易重新吸入角色状态。由于它无法结构性表达已经稳定的 owner 隔离，这是**设计否决**。

**选项二：按 SQL、观察、准备、协调的每一个阶段分别建立 crate。** 这能画出最细的依赖图，却要求尚在共同演化的 request-local 值跨越多个 `pub` 边界。每次调整冻结时机、错误分类或 ownership handoff 都会传播到适配 DTO；循环依赖最终只会被 callback trait 或镜像类型掩盖。这是**成本否决**：若这些接缝以后拥有独立消费者和稳定演进节奏，可以重新评估。

**选项三：稳定 owner 独立成 crate，查询内部阶段留在一个应用 crate 内。** Execution Contract、Worker、Workload Control、Catalog Application 与产品应用按真实决策权分离；SQL application、观察、准备和协调共同留在 Query Application，通过模块私有性和 move-only 构造限制误用。Native/MySQL 位于外侧，Server 只组合。采纳。

**选项四：建立一个中央 application facade 或 service locator，让所有领域互相通过动态接口调用。** 它能暂时消除 Cargo 环，却把依赖矩阵变成运行时注册顺序和可选实现；缺失能力会从编译错误退化为启动或请求期错误，也会形成第二套 owner。由于这破坏唯一组合权威和 fail-closed 构造，这是**设计否决**。

## 裁决

采用“稳定 owner 物理化、移动接缝模块化”的边界，并固定以下规则：

1. **Owner-first rule。** 先回答谁能改变状态、谁决定重试、谁释放资源，再决定 package。共享数据形状或代码量本身不构成共同 owner。
2. **Stable-seam rule。** 只有依赖方向明确、生命周期独立或需要 Cargo 闭包强制的边界才成为 crate。共同持有 request-local 语义且同步演化的职责留在 Query Application 内，用私有字段、私有构造和受限可见性隔离。
3. **Policy-inward dependency rule。** 产品应用依赖通用 Query Application；角色适配依赖领域契约；Server 依赖并组合全部实现。Query Application 不依赖产品、Frontend、Backend、adapter、wire codec 或 Server；Worker 也不依赖 Query Application。
4. **Contract-is-not-runtime rule。** Execution Contract 只包含不可变跨 owner 事实和纯验证。可变 registry、重试循环、资源 owner、transport client 和执行 kernel 不得因双方都使用而进入 contract。
5. **Thin-composition rule。** Server 负责显式配置、唯一 owner 构造、窄 handle 注入和逆序 shutdown，不直接 decode Task wire，也不执行查询或产品政策。
6. **No-facade rule。** 不以 optional port、全局 registry、万能 AppState、兼容 facade 或双向 callback 掩盖依赖环。出现环时重新核对 owner 或把 consumer port 定义在政策拥有者一侧。
7. **Graph-and-route rule。** Cargo normal dependency closure 是 crate 隔离的长期 guard；生产唯一调用路径、move-only handoff 与无旁路仍由构造 API 和运行测试证明，不能用静态图代替。

## 接受的妥协（诚实记录）

`novarocks-query-application` 会是一个相对大的 crate。观察、准备与协调共同编译，修改其中一个内部接口可能重编整个查询应用。这是为了保留同一请求事实的单一类型和私有 handoff，而不是认为大 crate 天然更清楚。

稳定 crate 边界会扩大一部分 `pub` surface，并受 Rust orphan rule 约束。我们接受在 adapter 侧实现窄投影，但不复制领域 DTO。若一个类型仅为穿过边界而存在且没有独立语义，应先怀疑切分位置，而不是继续增加转换层。

Cargo guard 只看解析后的 normal dependency graph。它不检查同一 crate 内的模块访问、dev/build 依赖、公开 API 的语义或运行时是否绕过了正确 owner；这些仍需要 compile-fail、构造测试和生产场景验证。

物理迁移可以分阶段落地，因此一段时间内旧角色 crate 仍可能承载尚未迁出的产品或协议代码。阶段检查点不能被解释为允许长期双权威：每个已经切换的能力只有一个生产 owner，未切换部分必须被明确点名。

## 何时重新评估

1. Query Application 内的观察、准备或协调出现第二个独立生产消费者，并且其公开契约能在多个版本中独立演进时，重新评估是否拆成新 crate。
2. 实测增量编译或链接成本主要由 Query Application 的共同编译单元造成，并且可通过稳定 owner seam 拆分而不复制 DTO 时，重评当前粒度。
3. 产品应用需要反向回调 Query Application 的新能力时，先检查消费者 port 是否能由 Query Application 定义并由 Server 注入；若不能闭合，重新裁决 owner，而不是直接新增反向依赖。
4. 引入多进程协调者、独立调度服务或可动态加载应用插件时，当前静态 Server 组合与 crate graph 可能不足，需要重新设计兼容、发现和生命周期协议。
5. Cargo 无法表达但反复被同 crate 模块越权破坏的边界出现时，评估编译期可见性或将该稳定 seam 物理化；不回到源码 token 扫描作为主要权威。
