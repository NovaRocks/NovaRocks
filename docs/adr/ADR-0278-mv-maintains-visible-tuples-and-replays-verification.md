---
id: ADR-0278
title: "Non-aggregate MVs maintain visible tuples and replay exact verification obligations"
domain: [frontend-mv, pipeline-execution, provider-spi]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0222
    scope: "Ruling 2 production summary authors now include ContentApply for DeleteVerification."
    link: "ADR-0222-typed-task-summaries-and-per-kind-completion.md"
  - id: ADR-0267
    scope: "A new non-aggregate interpretation omits the apply key and branch storage roles."
    link: "ADR-0267-mv-publishes-bound-source-and-storage-facts.md"
  - id: ADR-0268
    scope: "The persistent cross-process takeover compromise for non-aggregate MVs becomes strict replay from Pending and conditional main publication."
    link: "ADR-0268-iceberg-mv-converges-from-the-original-commit.md"
  - id: ADR-0271
    scope: "Ruling 7 non-aggregate write-mode selection uses evidence, cost, and admission for multiple strategies."
    link: "ADR-0271-iceberg-change-window-facts-and-complete-enumeration.md"
date: 2026-11-28
provenance:
  - "discussion: 2026-11-27 accepted visible-tuple maintenance, strict recovery, startup grace, and coverage contract"
symptoms:
  - "物化视图按可见列维护时，怎么确认这次删对了，重启后还能继续？"
  - "ContentApply, DeleteVerification, PendingVerificationRequirement, NOVAROCKS_MV_NAME_STARTUP_GRACE"
code-anchors:
  - "novarocks/physical-plan/src/validation/content_apply.rs (validate_content_apply, routes_agree)"
  - "novarocks/sql/src/planner/distributed/build/contract_lowering.rs (lower_content_apply, check_maintenance_input)"
  - "novarocks/execution/src/exec/pipeline/builder/compiled_content_apply.rs (assemble)"
  - "novarocks/execution/src/exec/operators/content_apply/stages.rs (ContentApplyPhase)"
  - "novarocks/spi/src/connector/read_stack/split_source.rs (SplitPruningBasis, SplitPruningLedger)"
  - "novarocks/mv-application/src/persistence/resolution.rs (ApplicablePublication, FrozenMvMarkerKeys)"
  - "novarocks/mv-application/src/persistence/codec/markers.rs (Marker, PendingVerificationRequirement)"
  - "novarocks/frontend-application/src/mv/domain/refresh/strategy.rs (RefreshStrategy)"
  - "novarocks/frontend-application/src/mv/domain/refresh/decision.rs (verify_native_refresh)"
  - "novarocks/frontend-application/src/mv/domain/refresh/recovery.rs (prove_strict_recovery)"
  - "novarocks/mv-application/src/management/name_lifecycle.rs (NameStartupGrace, NameLifecycle)"
  - "novarocks/connector/iceberg/src/commit/mv_dispatch.rs (FullRefreshStageWitness, require_full_refresh_marker_keys)"
  - "novarocks/connector/iceberg/src/commit/retirement.rs (RetirementPreparer)"
---

## 问题

非聚合物化视图怎样按可见元组维护行袋，证明删除已经完成，并在发布结果未知或 FE 重启后恢复同一项核验义务？

## 背景与执行事实

非聚合 MV 的目标存储表 S 保存查询结果的可见列。内容相同的行可以有多个副本，所有副本都计入行袋。
一次增量维护在冻结的源窗口上得到带符号的输出元组。负贡献需要在 S 的实际可见副本中定位，写入仍由 Iceberg
按准确文件与行位置的原协议发布。源窗口的增量能力、净零依据、负权重可能性和分区影响继续由 Provider 签发。
[ADR-0271](ADR-0271-iceberg-change-window-facts-and-complete-enumeration.md)

| 事实 | 作者与用途 |
|---|---|
| D、L、M、B、P | D 定义查询，L 冻结解释与存储角色，M 记录目标映射，B 绑定准确源事实及 D/L/M，P 在快照上绑定 B 与源水位。历史 P 沿自己的 B 解码。 |
| 元组与删除核验 | SQL 冻结投影后的完整内容、比较类型和两侧路由。Execution 按规范内容归并有符号计数，定位目标可见行，独立完成 `DeleteVerification`。 |
| 持久义务与资格 | MV Application 的解析作者按 S 的当前载体判断 `Bound`、`RetiredPublication`、`NeverPublished` 或 `Unattributed`，再以标记决定服务与维护资格。 |
| 发布效果 | FE 消费完整计算证据并授予原提交。Iceberg 保管准确对象、main 条件、派发证据与清理。StateStore 的 Accelerator 从湖上观察重建。投影 v7 增加资格字段，v5 与 v6 继续解码，缺少的资格由新的湖上观察补全。 |

## 考虑过的选项

- **在非聚合 S 继续写入隐藏源行身份。设计否决。** 行身份可以协助简单行定位。可见投影、过滤、重复值、UNION ALL
  和 join 的结果正确性需要按内容和次数证明。持久布局还会把内部辅助列暴露给直接读取 S 的引擎。
- **重新扫描并以全量重建结束未完成核验。设计否决。** 自动全量可以生成正确的新结果，同时抹去旧删除是否得到验证这一事实。
  需要重建时由显式领域操作授权，并把原义务写入恢复记录。
- **为每个非聚合目标保留跨进程未结效果屏障。设计否决。** 已有的带 main 条件提交可以限制旧快照效果，持久 Pending
  可以保留待核验内容。名字在同一存储路径之间可复用，需要独立保护。
- **在全部增量 join 计划上开放运行时过滤。成本否决。** 这会减少部分扫描，前提是每条差集与投递边都能归属裁剪，
  同时保留正负贡献。阶段性范围保留增量 join 的既有关闭设置。
- **用分区谓词支持未对齐来源或 join 的局部重算。待评估。** 切片代价可能低于全量，正确性依赖对所有贡献和目标文件
  的完整分区归属。

## 裁决

1. **可见存储与完整内容比较。** 新建非聚合 L 的 `apply_key` 为空，`branches` 为空，目标映射的列按可见输出声明。
   兼容解码允许缺省键，聚合 L 继续要求其组行键及原存储角色。旧的非聚合解释按定义、内容类型与布局资格作重建判定。
   非聚合源可以按能力准入 Iceberg v2、v3。内容比较保留顺序、类型、NULL 与规范值身份。相同内容的计数按带符号贡献净额计算，
   只为净负次数从实际 S 可见行选择待删位置，只为净正次数发出插入。
2. **数据与核验各走原端口。** 首个输入捕获全量带符号元组并归并，再探测按同一内容路由的目标可见行。
   `ContentApply` 以 capture、finalize、probe、tail 组装本地阶段，按实际参与位置安装贡献单位。
   数据端口进入原 Router、writer，写入计数继续由 Router 出具。节点以 `SummaryAuthor::Node` 另行交付
   `DeleteVerification`，安装前冻结 tail 单位，零贡献的单位也正常闭合。FE 核对准确来源、种类、集合和次数，独立消费
   requested、matched、不足内容与指纹。只有完整核验能授权数据发布或退役。
3. **全策略共享结构覆盖证据。** 覆盖由同一次尝试的 Provider 枚举、逐输入的排空合同、Root End 和准确参与者完成组成。
   Provider 枚举必须自然交出末批，每份未读文件归入静态谓词、分区选择、动态过滤、空需求或不可满足谓词。
   回执按依据记录文件数与身份摘要。冻结 Fragment 的本地输入策略和跨 Fragment 的数据边均使用 `MustDrain`。
   白名单的 Scan 及有限 Values 走自然结束，Filter、Project、Unpivot、Repeat 与 ChangeEventExpand 消费全部输入。
   Sort、TopN、Window、HashAggregate、SetOp 与 Redistribute 消费至上游结束再按各自语义产出或完成。
   HashJoin、NestLoopJoin 即使 build 为空也排空 probe，probe 先结束时仍等待 build 完成。
   ContentApply 排空两侧后收尾，其余节点以类型化错误拒绝。
   净零 occurrence 沿冻结窗口依据闭合。全量、首刷、AppendOnly、PartitionReplace 和 ContentLocate 的数据计划共同执行
   这套检查。需求运行时过滤与内容等值同源，需求中的 NULL 必须放行 NULL。维护计划仍记录实际裁剪依据。
4. **资格与策略沿准确观察决定。** 存储可读性、管理文档可解码性和发布可服务性分别取证。改写使用可服务的 `Bound(P)`。
   直接读取沿现有规则读取当前快照，零文件退役继续引用原数据文件。核验不完整的产物留在原清理作用域。
   未归属时现存 Invalid 阻断，适用的未解除 Pending 进入严格恢复。没有阻断的未归属状态可重算全量。
   AppendOnly、ContentLocate、PartitionReplace、FullReplace 与 MetadataOnly 各自要求相应证据，走共用成本评估。
   差集用冻结的事件体量，快照扫描用 manifest 文件体量，精确切片沿同一准备回执得到切片体量。
   Iceberg 按原观察签发的快照与 schema 投影读取历史统计，仍以独立的 Current 观察约束写入。
   PartitionReplace 限定对齐的非 join 形状，要求精确分区影响且源窗口无行级删除。提交复用过滤删除与追加的两阶段见证，
   校验选区内的新增文件和固定 main，基准变化返回 52。全量的每一轮比较准入时冻结的所有 MV 标记键。
5. **先估计和准入，再建立持久义务。** ContentLocate 使用源事件体量、存储域行宽与关系放大估计状态规模。
   变长列宽来自统计，估计可能低于实际。部署声明的单查询内存预算同时用于用户刷新和后台刷新，
   预算和估计一同写入 Pending，实际每参与方上限沿同一预算安装。观察到准确 Pending 生效才派发计算。
   位置删除从这一版本取得完整写入基准。写入后的文档投影与目标声明来自同一观察。
   可回退的 `NotStarted` 截止于原装配安装。装配前终止，或完整通过后数据发布确定未生效，才可写同一义务的解除标记。
   覆盖缺失、结果未定及退役遇 52 保留 Pending。确定核验不足通过零文件退役提交原子写入退役记录和 Invalid。
6. **严格恢复信任冻结基准的净零窗口。** 读取 Pending 指定的目标基准 X、原 P/B、映射 M、源窗口和预算。
   对 X 到当前 main 的窗口复用 Provider 的 `SameSnapshot`、`ReplaceOnly` 或 `EqualEndpoints` 净零证明。
   内容变化、父链缺失或历史不可读会阻断。重放复用原 Pending 和刷新身份，重新计算和验证，以当前 main 为提交条件。
   退役也只取 X 上准确原 P，当前载体允许缺少 P 或携带同一 P，校验后按当前 main 提交。
   暂时短缺沿原有界重试，确定性不足或旧语义不可用保留义务。显式重建在新数据与新 P 的同一提交写恢复记录，
   每轮按当轮观察移除 Invalid、Pending 与解除标记。旧提交的清理仍需原 Provider 证明。
7. **管理接续与名字保护各有准确范围。** 非聚合结果未知由原语句继续观察，跨进程由准确槽登记、持久标记和固定 main 收束。
   CREATE、DROP 的名字效果在派发前按 catalog、namespace、name 登记，确认结果或声明寿命到期才释放名字。
   启动发现完成前暂停建删。发现本部署的旧写者、发现不完整或目标不可解码会触发宽限。
   恢复建删需要发现完整且无重启证据，或者宽限因全部相关远端效果寿命到期或部署级声明而结束。
   声明绑定当前 launch、覆盖本部署其余全部 incarnation、断言它们的旧效果已收束。聚合仍沿原逐目标屏障与管理接续。
   读取与刷新按各自的发布资格和串行写入准入运行。

## 接受的妥协（诚实记录）

- 内容匹配需要原目标中的可见副本。需求过滤只能按已证明的内容与 NULL 规则缩小扫描。暂时无法比较的键，或缺少
  变长宽度证据时，尚未建立义务的刷新可选择其他合格策略，包括全量。估计低于派发后的实际状态量时，确定性资源失败保留 Pending。
- 继续信任 Iceberg 写方遵守 REPLACE 的行袋等价合同。外部回滚及缺失历史会限制重放证明。跨重启未知的产物需要
  经过现有准确引用与时间窗口的维护清理。
- 启动发现可能看不到已被外部删除的旧目标。缺少重启证据时，迟到的按名 DROP 仍可能击中后来同名创建的目标。
  名字保护只覆盖 NovaRocks 的 MV 操作。聚合接续与非聚合接续在过渡期并存。
- 增量 join 的运行时过滤仍关闭，PartitionReplace 先覆盖可证明对齐的切片。可见存储、删除核验、严格恢复及其选路
  作为一个 PR 进行组合验收。最终候选的原生、跨重启和全工作区验收仍待独立取证。

## 何时重新评估

- 源及目标扫描能按同一次尝试和准确关系报告裁剪，并证明差集正负贡献闭合后，评估开放增量 join 的运行时过滤。
- 有跨 join 的分区影响证明和目标切片归属后，评估未对齐或多来源的局部重算。
- 观测到估计误差影响准入或经常触发确定性不足时，校正行宽统计与算子放大估计，核对冻结预算与实际停机前沿。
- 存储对象具备唯一命名，或 Catalog 可以按对象身份执行 DROP 后，重评名字保护、启动声明与剩余迟到风险。
- 快照保留开始主动保护当前发布与未解 Pending 的基准链时，复核老义务重放与安全过期的交界。
