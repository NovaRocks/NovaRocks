---
id: ADR-0219
title: "Iceberg v3 lineage facts and planner-declared duplicate-match errors"
domain: [provider-spi, frontend-dml, frontend-mv, error-contracts]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0055
    scope: "Historical write.row-lineage-dependent eligibility and the v2 plus property write limitation described in compromise 1; Provider-owned strategy, format-version-only DELETE routing, and SQL predicate ownership remain active"
    link: "ADR-0055-row-dml-strategy-consumer-closeout.md"
date: 2026-11-06
provenance:
  - "discussion: 2026-11-06 format-version lineage, selected-snapshot facts and planner-declared duplicate-match violations"
  - "reference: Trino 89d09f46c68a6ba43dee2c734c733e6fc54453ab, QueryPlanner.plan(Merge), FlatHashStrategyCompiler and StandardErrorCode"
  - "reference: iceberg-rust 0.11 and Iceberg Java v3 row-lineage inheritance"
code-anchors:
  - "novarocks/connector/iceberg/src/schema_facts.rs (row_lineage_enabled)"
  - "novarocks/connector/iceberg/src/row_lineage_synth.rs (lineage synthesis)"
  - "novarocks/connector/iceberg/src/storage_inspector.rs (source snapshot observations)"
  - "novarocks/spi/src/connector/mv_storage_observation.rs (MvSourceSchemaObservation)"
  - "novarocks/frontend-application/src/mv/domain/refresh/target_apply.rs (base eligibility)"
  - "novarocks/frontend-application/src/query_execution/dml/mutation_flow.rs (duplicate assertions)"
  - "idl/novarocks/physical_package_v2.proto (PerKeyRowCountAssertion)"
  - "novarocks/execution/src/exec/operators/assert_num_rows_processor.rs (keyed assertion failure)"
  - "novarocks/trino-adapter/src/error_mapping.rs (execution_code)"
---

## 问题

跨引擎创建或升级的 Iceberg 表，如何一致决定行血统能力、MV 当前所选快照的资格，以及MOR UPDATE/MERGE重复匹配的检查与客户端错误身份？

## 背景与执行事实

表格式能力、所选快照事实、文件继承值和 SQL 语义属于不同层。Iceberg v3 规定 row lineage；NovaRocks 的 `write.row-lineage` 只是普通表属性，其他引擎不依靠它声明能力。v2 升级后的旧文件可暂时没有 `first_row_id`，所以“表已是 v3”并不保证当前所选快照已经分配了行范围。

| 对象 | 原作者与事实 | 不可替代它的推断 |
|---|---|---|
| lineage 列与行级写入准入 | Iceberg Provider 读取冻结 metadata 的格式版本 | 自定义开关、文件名、当前数据是否非空 |
| MV 基表资格 | Provider 观察准确所选 snapshot 的 `row_range`，MV 领域解释资格 | 表属性、另一个 snapshot、当前表头的范围 |
| 旧文件的 lineage 值 | 原文件的 `first_row_id`、存储列与数据序列号 | 用当前表版本补造每一行的 id 或序列号 |
| 目标行是否已匹配 | join 后该目标行的 `_file` 存在 | 可为 NULL 的 `_row_id` |
| 重复检查与违约身份 | FE 规划断言的键、适用范围与 类型化违约原因 | 执行层重新解释 SQL，或适配层按语句种类回猜 |

Trino 的 MERGE 规划对 `(unique_id, case_number)` 做 MarkDistinct：已匹配行使用目标行身份；未匹配行使用源行唯一身份；未命中任何子句时 `case_number` 为 NULL，两个 NULL 在分组中相等。没有命中子句的行在判重之后才被消费为无动作。这里采用其规则，而不是按“实际修改次数”判重。

## 考虑过的选项

1. **保留逐表 lineage 开关：设计否决。** 它可以表达产品私有开关，却与 v3 的格式事实和其他引擎创建的表脱节，也会把属性为 true 的 v2 误报为具有 v3 能力。格式能力只由 Provider 的真实格式版本决定。
2. **MV 逐文件扫描 manifest 后判断：成本否决。** 可提供更细的文件诊断，但资格检查的成本随表规模增长。所选快照的 row range 已是所需的 O(1) 元数据事实；更细诊断没有成为当前资格合同。
3. **以 `_row_id` 判匹配并判重：设计否决。** 升级前文件可以合法缺少这个值。使用物理位置 `(_file,_pos)` 识别被匹配行，不让尚未分配的 lineage 值决定 SQL 匹配事实。
4. **只计实际修改，或只按目标行匹配次数：设计否决。** 两者各有业界实现，但都改变已选择的 Trino 语义：前者忽略未命中子句的重复，后者合并不同子句。这里保留目标行与命中子句的组合；未命中也自成一组。
5. **FE 按“通用断言错误加语句种类”事后映射：设计否决。** 它依靠某种语句只有一种断言的偶然结构，其他断言会被误报。计划在拥有完整 SQL 语义时声明原因，执行侧只传递事实。
6. **升级前序列号返回数据序列号：设计否决。** Trino 的相应列采用这个行为；这里采用读取栈 iceberg-rust 与 Iceberg Java 的 NULL 行为，与没有 `first_row_id` 时的 `_row_id` 一致。该分歧明确保留。

## 裁决

1. **格式能力规则。** Iceberg row lineage 只由格式版本是否至少 v3 决定。无属性或属性为 false 的 v3 具有能力；属性为 true 的 v2 仍无能力。UPDATE 与 MERGE 仅准入 v3，v2 保留类型化拒绝；DELETE 的 Provider 策略保持按格式版本签发。
2. **原文件继承规则。** 没有 `first_row_id` 的文件，其 `_row_id` 和 `_last_updated_sequence_number` 都为 SQL NULL。有 `first_row_id` 时沿原存储值与数据序列号的继承规则合成，算术保持受检，不补造 id。
3. **快照事实规则。** 源观察只陈述准确所选 snapshot 是否有 row range，包含 SnapshotTo 观察。MV 基表要求 v3 且所选快照有范围；拒绝说明所选 snapshot 与执行一次 v3 提交（例如 OPTIMIZE）的恢复方法。这个字段不命名为永久 MV 能力。目标表只要求 v3；新建 MV 目标不注入属性，CTAS 只确保 v3。用户显式给出的属性原样作为普通属性保存。
4. **MOR物理匹配规则。** MERGE 以 `_file IS NOT NULL` 表示已匹配。UPDATE/UPDATE FROM 对每行按 `(_file,_pos)` 判重；MERGE 对全部已匹配行按 `(_file,_pos,命中子句)` 判重，包括未命中子句的行，以及只有 WHEN NOT MATCHED 的语句。未匹配插入行不参与目标行断言。
5. **MOR先断言后无动作过滤规则。** 未命中子句的已匹配行必须先参与断言，再过滤无动作行。当前最多一个 WHEN MATCHED，动作码与该子句一一对应，0 表示未命中；若扩展多个子句，第三键必须采用实际子句序号，不能用动作种类合并子句。
6. **规划声明规则。** failure-contract 定义 SQL 域违约原因；FE 规划在准确断言上声明，依次经物理计划、package 与 LocalCompiler 传到 BE。键控断言失败沿 Data 类返回所声明原因；没有声明的断言仍返回原中立原因 7:317。执行侧不解释 SQL 语句种类，不解析消息以决定原因。
7. **协议投影规则。** Trino 边界只按声明原因投影 `MERGE_TARGET_ROW_MULTIPLE_MATCHES`、119 与 USER_ERROR。MERGE 文本为 `One MERGE target table row matched more than one source row`；UPDATE FROM 使用 UPDATE 替换 MERGE。文本不加内部 code 前缀或发布终态后缀，诊断继续保留原失败位置和发布事实。其他失败映射不变。
8. **精确版本规则。** package 新增声明身份是计划合同变化，随 plan contract revision 与原生 exact-island 身份更新。无旧格式透明读取、跨版本降级或 statement-text fallback。断言成功、写入提交与实际退出仍分别取证。
9. **阶段范围。** 用户在RWM-5执行中明确选择：本次119、多键与NoAction对齐限定MOR。仍可达的COW UPDATE/UPDATE FROM保留独立consumer，普通重复拒绝尚无计划119身份。完整FE effects把matched UPDATE的MERGE也计入Delete，当前所有SQL MERGE均由Provider路由MOR，属于本次合同；保留COW MERGE helper的旧NoAction过滤不可误报为现产品缺口。RWM-7切换UPDATE、RWM-8删除遗留COW后收敛，不另造临时COW机制或事后统一映射Connector错误；共享metadata声明仍须逐列准确。

本条仅替代 ADR-0055 中依赖 `write.row-lineage` 的历史准入描述与其妥协 1 的 v2 加属性不可写限制；Provider 策略唯一、DELETE 按格式版本、SQL 谓词合法性与其余有效裁决保留。

## 接受的妥协（诚实记录）

- MOR选择 Trino 子句分组语义，意味着把条件改严也可能产生新的 119：两条都未命中、或仅有 NOT MATCHED 时，匹配同一目标行仍会失败。它比按实际修改计数更严格，且这两种精确情形只有上游源码推导，没有相应上游测试背书。
- MOR无动作行在过滤前也需参加键交换与断言，增加原 join 结果的一次分布和状态成本；不以提前过滤改变语义。
- O(1) 快照资格检查无法列出每个尚无 lineage 的文件。只给准确 snapshot 和恢复办法；若以后按内容维护 MV，仍须重新评估是否需要基表 lineage。
- 升级前文件的序列号采用 NULL，与 Trino 的对应列存在明确差异。表头已升级不能证明旧文件有完整 lineage。
- MOR 对升级前文件的更新使用物理位置并分配新身份；现有 COW 收集仍拒绝没有 lineage 的旧文件，不能把 MOR 的资格与成功解释为 COW 已支持。
- 规划声明使 package 承载新的共享语义，增加编解码和版本验证义务。它不改变 Task/Worker 失败事实结构，也不能借此扩大其他错误映射。

## 何时重新评估

- RWM-7切换默认UPDATE到MOR时，用无显式MOR属性的入口确认相同计划身份和判重范围；RWM-8删除COW时移除过渡差异。当前不能以MOR验证称全策略已对齐。

- 支持多个 WHEN MATCHED/WHEN NOT MATCHED 子句时，将第三键落实为子句序号，并重新核对跨动作效果；不能只扩展 parser。
- MV 改为按可见内容维护、无需保存基表 `_row_id` 时，重新评估基表 lineage 资格；观察字段继续只陈述快照事实。
- 需要逐文件诊断，且 metadata 级事实不足以满足已接受的产品合同或独立证据推翻其充分性时，重评 manifest 扫描的成本取舍。
- Iceberg 规范与实际读取栈统一旧文件序列号规则，或产品明确改变跨引擎兼容目标时，重新裁定 NULL 分歧。
- package 或 failure-contract 扩展声明错误时，保持有限登记与准确域原因，不引入任意文本驱动的执行语义。
