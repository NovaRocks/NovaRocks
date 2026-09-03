---
id: ADR-0114
title: "Payload is the sole authority for participant roles"
domain: [distributed-query-lifecycle, runtime-filter]
status: superseded
supersedes: []
superseded-by: ADR-0134
date: 2026-08-26
provenance:
  - "mechanism: participant-role field removal with payload-derived participant classification on the native lifecycle wire"
  - "discussion: 2026-08-26 self-derived wire field classification following the message self-attestation removal"
code-anchors:
  - "idl/novarocks/service.proto (ParticipantManifest, QueryParticipantRole)"
  - "novarocks/proto/src/lifecycle/manifest.rs (ParticipantManifest::parse)"
  - "novarocks/frontend/src/query_execution/lifecycle_plan.rs (participant manifest assembly)"
  - "novarocks/backend/src/query_lifecycle/registry.rs (QueryLifecycleRegistry::init_query)"
---

## 问题

一条 wire 消息里的某个字段，其取值可以由**同一条消息的其它字段**完全推导出来。正确处理是「保留该字段，并让接收方
校验它与推导结果一致」，还是「一开始就不把它放上 wire」？`ParticipantManifest.participant_roles` 正是这个形状：它是
Frontend 从同消息另外两个字段机械推导出的投影，其一致性由跨两层的三处检查维持，而它本身在生产代码中几乎无人真正读取。

## 背景与执行事实

### 判据（承接 ADR-0113）

ADR-0113 建立了一条判据：**「消息 M 携带字段 d、d 的全部派生输入都在 M 内、接收方重算 d 再与携带值比对」这一形状，
接收方校验它不产生任何新事实**。据此该裁决删除了 `InitQueryRequest.init_digest`、`StageFragmentsRequest.stage_digest`
与 `RuntimeFilterContribution.contribution_digest` 三处消息自证摘要，并保留了全部跨消息内容引用——后者的派生输入不在
携带消息内，接收方无法独立重建，删掉就真的失去 fence。

该判据关心的是**信息结构**，与载体是哈希还是枚举集合无关。哈希只是「同消息派生值」最显眼的一种形态；一个由同消息其它
字段按固定规则推导出的**枚举集合**具有完全相同的结构，因此受同一条判据约束。本裁决把该判据从「消息自证摘要」推广到
**自证式派生字段**（self-derived field）这一更一般的类别。

### `participant_roles` 是同消息派生投影

`novarocks/frontend/src/query_execution/lifecycle_plan.rs` 在组装 `ParticipantManifest` 时，按两条固定规则填充 roles：

- `FragmentExecutor` ⟺ 该 participant 的 `expected_fragment_instance_ids` 非空；
- `RuntimeFilterService` ⟺ 该 participant 的 `runtime_filter` contribution 存在。

两条规则的输入（`expected_fragment_instance_ids` 是 field 4，`runtime_filter` 是 field 8）都在同一条
`ParticipantManifest` 内。接收方拿到消息后可以自行推导出完全相同的 roles 集合，携带值不提供任何它得不到的事实。
production 路径上不存在第二个 roles 生产者——没有任何调用方会构造出与载荷不一致的 roles。

### 一致性由跨两层的三处检查维持

- `novarocks/proto/src/lifecycle/manifest.rs`（Protocol 层，`ParticipantManifest::parse`）：`FragmentExecutor` 的**反向**
  检查——roles 不含 `FragmentExecutor` 但 instance 列表非空时拒绝（"service-only participant must not declare fragment
  instances"）。
- `novarocks/proto/src/lifecycle/manifest.rs`（Protocol 层，同一 `parse`）：`RuntimeFilterService` 的**双向**检查——
  contribution 的存在性与该 role 的存在性必须同真同假（"runtime filter contribution and participant role must be present
  together"）。
- `novarocks/backend/src/query_lifecycle/registry.rs`（Backend 层，`QueryLifecycleRegistry::init_query`）：
  `FragmentExecutor` 的**正向**检查——roles 含 `FragmentExecutor` 但 instance 列表为空时返回
  `QueryInitRejectedInvalidManifest`。

三处检查合起来，恰好把 roles 钉死为载荷的函数。也就是说：**这三处检查存在的全部目的，就是保证那个字段没有携带任何新
信息**。

### 生产代码中的读者

`ParticipantRole` 在 production 路径上只有上述三处读者。`RuntimeFilterService` 尤其极端：它在生产代码中**零读者**——
Backend 决定是否安装 runtime filter，只看 `ParticipantManifest.runtime_filter` contribution 是否存在
（`registry.rs` 的 `InitWorkspace::install_and_publish` 直接读 `manifest.runtime_filter()`），从不查询 roles。
`FragmentExecutor` 也只被上面那一处正向检查读到，fragment admission 本身用的是 `expected_fragment_instance_ids`。

### 与已冻结的 participant 语义一致

ADR-0008 在裁决三阶段启动时已经冻结：「每个 participant 都必须经历 Stage：service-only participant 使用**显式的空
fragment list**，禁止伪造 empty fragment 来占位」，其背景段也已把 service-only participant 定义为「合法地拥有空
fragment 集合、但仍承载 runtime filter/exchange 等 query-scoped 服务」的参与方。因此「参与方类别由载荷判定」并不是本
裁决新引入的概念——它是 ADR-0008 已有语义的直接读法，`participant_roles` 只是把同一件事又显式写了一遍。

### 那两处 Protocol 跨字段检查的出处

这两条跨字段一致性检查来自一份更早的设计裁决（2026-08-15），它列举 Protocol 层职责时把「跨字段一致性」与「digest 与
内容匹配」并列为同一类工作。**其中「digest 与内容匹配」已被 ADR-0113 删除。** 本裁决处理的是同一份清单里剩下的那一
条，因此是那次收窄的**延续而非否定**：Protocol「只表达 schema 表达不了的规则」这一原则继续成立，Protocol 依然拥有
required presence、enum 合法性、version、fixed-width identity 与各类上界（ADR-0106 划定的校验面）。本裁决主张的只是：
一个可由同消息其它字段完全推导的值，正确处理是**不上线**，而不是上线之后再花两层去校验。

## 考虑过的选项

1. **保留字段与三处检查不动。** 改动成本为零，且 wire 定义读起来更「自解释」。代价是：三处跨层检查会长期作为一种可
   复制的模式存在，让后来者以为「新增派生字段 + 接收方交叉校验」是本项目认可的 wire 设计形态；同时 `participant_roles`
   会持续制造一种它是权威的错觉——一旦有人真的按 roles 而不是按载荷分支，两者就可能在某次重构中分叉，而分叉时哪一边
   是真的并不明确。否决。

2. **保留字段，把两处 Protocol 检查迁到 Backend（或反之，把三处收敛到单层）。** 表面上解决了「跨两层」的分散问题。
   但迁移的结果是让 Backend 去校验一个**同消息派生值**——这正是 ADR-0113 判定为无信息增益的那个形态。把它从一层搬到
   另一层，既没有消除冗余，还把「这是一份可以校验的权威声明」这个错误印象固化在新位置上。否决。

3. **删除两个具体 role，保留枚举与字段作为未来扩展位（只剩 `UNSPECIFIED`）。** 诱人之处是不动 wire 形状、将来加新
   参与方类别时「已经有地方放」。但只剩 `UNSPECIFIED` 的枚举是一具残迹：它不表达任何事实，却会让 schema 读者继续把
   participant 分类当成一件 wire 上已有答案的事。真到需要新参与方类别时，正确做法是为它引入自己的载荷字段（其存在性
   即分类），而不是复活一个平行的声明维度。否决。

4. **删除字段与枚举，让载荷成为唯一权威表示，并把「roles 非空」等价替换为「载荷非空」。** 选择此方案。参与方分类的
   唯一表示回到它本来就在的地方，三处检查随字段一并消失，`participant_roles` 与实际载荷再也不可能分叉——因为只剩一份
   表示。

## 裁决

**载荷是参与方分类的唯一权威表示。** 冻结如下读法，不新增任何概念：

- 执行 fragment ⟺ `expected_fragment_instance_ids` 非空；
- 提供 runtime filter ⟺ `runtime_filter` contribution 存在；
- 纯服务参与方（service-only participant）⟺ instance 列表为空且 contribution 存在。

这与 ADR-0008 已冻结的「service-only participant 使用显式空 fragment list」完全一致。

**删除 `ParticipantManifest.participant_roles` 字段。** 按仓库既有约定（`RuntimeFilterContribution` 已 reserve 其
`contribution_digest` 的 tag 4 与字段名）在 IDL 中 reserve tag `3` 与名称 `participant_roles`，**永不复用**。

**整体删除 `QueryParticipantRole` 枚举**，包括 `FRAGMENT_EXECUTOR`、`RUNTIME_FILTER_SERVICE` 与
`QUERY_PARTICIPANT_ROLE_UNSPECIFIED`，以及 Protocol 侧的 `ParticipantRole` 别名与 `parse_role`。不保留只剩
`UNSPECIFIED` 的残迹。

**三处一致性检查随字段消失，不迁移。** `ParticipantManifest::parse` 中 `FragmentExecutor` 的反向检查与
`RuntimeFilterService` 的双向检查、以及 `QueryLifecycleRegistry::init_query` 中 `FragmentExecutor` 的正向检查一并删除。
删除的理由是它们校验的对象不复存在，而不是它们校验的事实变得不重要——载荷本身就是那个事实，无从不一致。

**「roles 非空」等价替换为「载荷非空」。** `ParticipantManifest::parse` 中原有的 `participant_roles must not be empty`
改为：`expected_fragment_instance_ids` 非空 **或** `runtime_filter` 存在，二者至少居其一，否则拒绝。这精确保留了今天
对「空参与方」的拒绝能力——今天该能力是靠「roles 非空」加上两处交叉检查**合力**挡住的（例如只声明
`RuntimeFilterService` 却不带 contribution 的消息，今天由双向检查拒绝；声明 `FragmentExecutor` 却不带 instance 的消息，
今天由 Backend 正向检查拒绝）。替换后单条规则即可覆盖全部这些情形。

该检查只涉及**同一条消息内两个字段的存在性**，不需要外部知识、不需要活状态、不需要跨消息引用，因此仍属 Protocol 层
（ADR-0106 划定的 format/enum/identity/version/cardinality/budget 校验面之内），不构成向 Protocol 回填领域权威。

**Protocol 其余校验面不变。** instance id 的非零与去重、exchange route 去重、`query_options` 解析、deadline 与
pre-start timeout 非零、以及 ADR-0113 保留的全部跨消息内容引用 fence，均不受影响。`ParticipantManifestDigest` 的身份
语义不变。

## 接受的妥协（诚实记录）

**诊断文本变化。** 三条现有错误文本（"service-only participant must not declare fragment instances"、"runtime filter
contribution and participant role must be present together"、以及 `QueryInitRejectedInvalidManifest` 中由空 instance 触发
的那一支）不再产生。畸形 manifest 改由新的「载荷非空」检查或 `expected_fragment_instance_ids` / `runtime_filter` 自身的
结构校验拒绝，错误路径与文本都不同。依赖旧错误文本的排障习惯需要重建。这与 ADR-0113 记录的同类妥协一致——那次删除
`contribution_digest` 时也发生了「拒绝**理由**变化而非拒绝**能力**丢失」的位移。

**失去「显式声明」的可读性。** 阅读 IDL 时不再能一眼看到「这条消息里有哪些参与方角色」，读者必须知道分类规则才能从
载荷推断。接受它的理由是：**该显式声明从未是权威**——它由载荷推导而来，三处检查的存在本身就是这一点的证明。保留它
反而更危险：一个看上去像声明、实际是投影的字段，会让后来者误以为可以依赖它来分支，而真正的权威在别处。以可读性换取
「唯一表示」，这里换得值。

**manifest digest 的字节值发生变化。** `ParticipantManifest::digest()` 已按 ADR-0113 改为 descriptor 全遍历
（`canonical::digest_message`），删除一个字段会改变规范化投影，因而改变 digest 字节值。这是**预期的一次性变更**：
验收要求 FE/BE 从同一 schema 同源计算、round-trip 一致、Init/Stage/Start 的跨消息回显链路全绿，**不要求** digest 字节
相对本次改动前的基线保持不变。任何把旧字节值写进 golden 的做法都是把实现细节误当契约。

**不承诺混版本 FE/BE 或 rolling upgrade。** 删除 wire 字段后，旧 FE 发出的 manifest 携带 tag 3，新 BE 会将其作为未知
字段忽略，但 canonical digest 的计算基准已改变，回显比对不再成立。本裁决要求 FE/BE **同批切换**。这继承 ADR-0113 的
同名约束，不能从 reserved tag 推断兼容性。

## 何时重新评估

1. **若出现一种参与方，其分类无法由载荷推导**（例如既无 fragment instance、也无 runtime filter contribution，只承担
   exchange 中转或其它 query-scoped 服务的第三类参与方），那时分类就不再是派生值、而是一项新事实。正确做法是为该参与方
   引入**它自己的载荷字段**，让该字段的存在性同时成为它的分类依据；**不得**以此为由恢复一个平行的 role 枚举。

2. **若 Protocol 层再次出现跨字段一致性检查**，先按 ADR-0113 的判据分类：其派生输入是否全部在同一条消息内？若是，
   正确反应是删除被校验的那个字段，而不是新增检查。本裁决与 ADR-0113 共同构成该判据的两个已落地实例（哈希载体与
   枚举集合载体），可直接引用。

3. **若需要独立发布 FE/BE、mixed-version 部署或 rolling upgrade**，先定义 schema/digest version negotiation 与
   compatibility window，再决定被 reserve 的 tag 与已删字段如何处理；不能从 reserved tag 推断兼容性。

4. **若 `ParticipantManifestDigest` 需要跨版本稳定的字节值**（例如外部审计或长期归档消费它），那时 descriptor 全遍历的
   规范化投影就成了对外契约，本裁决「字节值变化是可接受的一次性变更」这一前提失效，需要先裁决 versioned canonical
   projection 再动 schema。
