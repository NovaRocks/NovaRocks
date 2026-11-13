---
id: ADR-0231
title: "Iceberg mutations use merge-on-read and signed write assignments"
domain: [provider-spi, frontend-dml]
status: active
supersedes: [ADR-0063]
superseded-by: null
partially-supersedes:
  - id: ADR-0049
    scope: "COW selection, activation routes and cohorts retire; Provider strategy and identity ownership, exact preparation and remaining write-stack routes remain active"
    link: "ADR-0049-provider-row-mutation-strategy-identity-routes-and-cohorts.md"
  - id: ADR-0055
    scope: "Property-selected COW UPDATE/MERGE and COW source/activation consumers retire; Provider strategy ownership, SQL predicate legality and MOR source facts remain active"
    link: "ADR-0055-row-dml-strategy-consumer-closeout.md"
  - id: ADR-0191
    scope: "COW-only root declaration, materialized Complete rows, selection conversion/requiredness handoff and FE signed-layout normalization retire; shared folding, value conversion, ordinary CAST semantics and typed failure preservation remain active"
    link: "ADR-0191-frozen-fold-facts-and-shared-compiled-evaluation.md"
  - id: ADR-0221
    scope: "COW result purpose, CSA2/CowSelectionRecipe, COW encoding custody and COW record limits retire; BatchRecipe, NRU1, logical requiredness, ordinary root IPC and shared resource/exit rules remain active"
    link: "ADR-0221-schema-free-online-units-and-prepared-layout-recipes.md"
date: 2026-11-13
provenance:
  - "discussion: 2026-11-12 Iceberg merge-on-read-only mutations, reachable fixed-width writes and copy-on-write retirement"
code-anchors:
  - "novarocks/connector/iceberg/src/commit/validation.rs (row_mutation_strategy_from_metadata)"
  - "novarocks/connector/iceberg/src/commit/write_shared.rs (canonical_data_write_fields)"
  - "novarocks/connector/iceberg/src/commit/row_mutation_preparation.rs (prepare_row_mutation)"
  - "novarocks/frontend-application/src/dml/insert/shaping.rs (source_assignment_before_common_type)"
  - "novarocks/sql/src/analyzer/write_assignment.rs (bind_write_source_query, register_literal_assignments)"
  - "novarocks/type-contract/src/write_input_projection.rs (CheckedWriteInputProjection, WriteInputDeclaration)"
  - "novarocks/execution/src/exec/node/table_writer.rs (materialize_target_value, validate_target_requiredness)"
  - "novarocks/execution/src/exec/operators/compiled_change_events.rs (materialize_target_value)"
  - "novarocks/connector/iceberg/src/commit/write_adaptation.rs (WriteAdaptationError)"
  - "novarocks/version/src/lib.rs (NATIVE_COMPAT_EPOCH)"
---

## 问题

统一行级写入路径时，谁签发真实来源、规范输入与存储目标之间的关系，如何保留字节和必填义务，并彻底移除已退出算法的表达能力？

## 背景与执行事实

| 事实 | 原作者与消费边界 |
|---|---|
| UPDATE / MERGE 策略 | Provider 在原通用可写、v3、类型和 ref 准入后签发 MOR；所有 MERGE 效果形状使用同一规则 |
| DELETE | v3 部分覆盖使用 deletion vector，完整覆盖沿原物化规则移除数据文件及独占旧位置项；非 v3 保留 position delete，不增加笼统行级拒绝 |
| 匹配读取 | 原 admitted AST、Provider 签署的 match contract、target ref/base、planning pin 贯穿同一句；不混入后来的 main 或 generation |
| S 与 V | S 为表达式真实源类型；一般 source query 完成 DISTINCT、UNION、ORDER/LIMIT 等语义后，SQL 原 value-conversion 作者执行普通赋值转换得到真实 V；原已支持的 VALUES、常量 SELECT / UNION ALL 保留逐值目标上下文，先普通赋值再共同类型推导，避免丢失 decimal 精度 |
| W 与 T | Provider 同一声明作者签发规范输入 W 和可选完整存储 T；Exact 不授予表示差异，显式许可仅在 T 的 plain physical Fixed 叶允许 W Binary，W 保留 T 各层 required 与其余完整事实 |
| 赋值投影 | CheckedWriteInputProjection 证明 V/W/T 的合法关系，不授予值、容量或 NULL 承诺；在实际选中 event/writer assignment 上检查 required，再用原 scoped hydrate 构造 W |
| 存储适配 | Provider 按 T 对可达非 NULL 原 bytes 检查 fixed(N) 宽度；data 和 equality-delete append 保留 typed Data 失败 |
| 统计与完成 | 写入后的 W 有独立 projected ValueId，统计不复用 V 的身份；逻辑 affected rows、writer 报告、最终 publication 和真实退出分别保留原 owner |

W/T 的许可属于准确字段和完整签名。nominal、字段名/顺序、decimal/timezone、offset、Map 排序、Union 等保持精确；typed binding 的规范 namespace 与 Provider 原 Field/metadata 的精确比较各守原边界，不从 Arrow carrier 或已被规范化移除的注释推领域。metadata planning、prepare、begin、shared maintenance、seal 与 public recipe 消费同一声明，不能各自重建近似 W/T。

MOR 的更新效果删除旧物理行并写 after-image。`_file`、`_pos`、原分区/spec 仍是真实删除事实；`_row_id` 与 `_last_updated_sequence_number` 保留 lineage。替换行保留源 ID，新版本继承真实 data sequence；新行的 ID 由实际提交分配。

DELETE 与 MOR UPDATE/MERGE 的 v3 删除效果保留 [ADR-0228](ADR-0228-iceberg-position-delete-materialization.md) 的逐文件物化规则：本次与旧删除位置的并集未覆盖整个数据文件时，保留数据文件并写最终 DV；完整覆盖时逻辑移除数据文件及它独占的旧位置删除项（含旧 DV），不新增 DV。完整覆盖移除属于同一 MOR 路径，不恢复 COW，也不授权物理删除仍有引用的共享对象。

共享 `ConnectorScopedBeginCause`、`RewriteBeginScope`、`IcebergSharedTargetSchemaBacking`、`read_snapshot/scoped_capture`、`CheckedDataFiles` / `ScopedDataFileProjection` 继续服务重写、统计、扫描和 MV 窗口；`analyzer/declaration_widening` 继续服务视图等真实消费者。中性改名不扩大能力或改变最后持有者责任。

## 考虑过的选项

1. **保留属性选择 COW/MOR：成本否决。** 部分修改比例可能受益于整文件重写，但两条路径维持两套选择、物化、重读和 activation 接缝；当前选择一条已接受的行级路径，不作普遍性能结论。
2. **只停调用而保留 COW 接口：设计否决。** 无生产作者的 selection、激活、结果用途与线路仍可表达退出语义；退役须覆盖定义、实现和消费者。
3. **普通 CAST 先转成 fixed(N)：设计否决。** 坏宽度可能变为成功 SQL NULL，丢失原 bytes；普通嵌套 CAST 也没有本赋值投影能力，不能扩大它来冒充存储合同。
4. **只比较 carrier 或只签宽化 W：设计否决。** carrier 不证明 nominal；丢失 T 或把 W required 全部宽化会丢掉实际赋值义务。V 保留 source NULL，W 独立保留 T required。
5. **完整 V/W/T 与选行后投影：采用。** 普通 S→V 与写入 V→W 分开；required 在 Execution 原 owner，字节宽度在 Provider 原 owner。两者复用唯一可达行作者，代价是真实受管工作与必要构造空间。
6. **全子数组验证：设计否决。** 父 NULL 或偏移未引用的子槽不是该逻辑值，检查它们会误拒绝合法输入。

## 裁决

1. **单一策略。** UPDATE 和所有 MERGE 固定 MOR。`novarocks.update.mode`、`write.update.mode` 及其他 `write.*.mode` 的已有值、COW 字面值或非法值均不影响算法，也不绕过原 v3、类型与 ref 准入。DELETE 与其他写入 flavor 保留原规则。
2. **同一来源。** match 与 begin 消费同一 preparation 的准确 base/ref；保留 MOR 非空 identity、binding、ordinal、分区与统计事实。COW uniqueness tuple、activation 和 frozen reread source 退出，不删除仍活跃的身份字段。
3. **三声明。** 一般 source query 的 S→V 复用普通 SQL conversion 作者，转换放在完整查询外层。原已支持的 VALUES、无 FROM 的 literal SELECT 及仅由这些分支组成的 UNION ALL 保留逐单元目标上下文：SELECT 不含 DISTINCT、过滤、分组或窗口，容器不含 WITH、ORDER/LIMIT/OFFSET/FETCH；原分析作者先对原 literal 单元执行普通赋值，再共同类型推导，保留 decimal 系数与精度。该上下文不进入显式 CAST 或 typed ARRAY 的内部，也不扩展到一般 SELECT、UNION DISTINCT 或排序/限量查询。V 保留真实 NULL 与获准 Fixed(m) 字节表示；W/T 关系来自 Provider 原完整声明，不借 COW RootPurpose 或表达式 intrinsic 推许可。NULL/空 literal 的 expected context 属原分析作者，已有 checked constant 保持原 pool/ordinal。
4. **选行后义务。** 普通 writer 与 change-event assignment 共用 CheckedWriteInputProjection；MERGE 未选中的 assignment 不提前执行 required/bytes normalization。原 Execution owner 在 Provider I/O 前把可达嵌套 required NULL 作为 Data 拒绝，再由 scoped hydrate 精确构造 W。父 NULL/未引用子槽不误拒绝；不可达槽使用合法布局占位。全局 event placeholder 可空不修改独立 W required。
5. **原字节存储。** 只有显式 T plain Fixed(n)→W Binary 叶许可 V 的 Binary 或 Fixed(m)，m 不与 n 比较。Provider 消费原 bytes，逐个检查可达非 NULL 值的宽度；坏宽度为 typed Data，不截断或填充有效值。Provider 收到 required NULL 的兜底仍为 Internal；控制和契约错误保留原分类。
6. **独立结果身份。** V 与构造后的 W 有独立 ValueId，统计消费实际写入投影。逻辑计数取原 Session Root/WriteCounts，不以物理行数或零占位替代。publication、取消、terminal 与实际退出不能互相替代。
7. **完整退役。** COW 内部读、Complete 物化、根声明、ExternalOwner 计数、Root 用途、SPI activation、Provider 实现和专属 profile 项全部退出。删除的 wire 编号及名字永久 reserved，不提供旧格式回退；普通 Expressions、视图宽化、共享 NRU1/BatchRecipe 与重写继续保留。
8. **兼容身份。** 完整 W/T 与 COW 词汇收缩必须进入原 descriptor、closed manifest、plan/Provider revision 和类型表/codec 摘要。版本 owner 的当前规则仅对这些材料无法表达的 execution-only 变化额外推进 epoch；不得因旧 epoch 注释提到 COW 而机械推进。

## 接受的妥协（诚实记录）

- 放弃通过表属性选择 COW 的策略空间；MOR 的删除文件、读合并和维护成本仍存在。只有独立负载证据能支持性能判断。
- 完整来源/目标合同已接受，普通 nested CAST/MAP/ROW 能力、UUID/Variant 等既有 Provider 不可写限制不会因此扩大。不能把本合同理解为任意来源转换都获准。
- 可达检查与规范构造需要真实工作和空间，仍消费原 scoped owner/creator/control；没有新配额、默认钱包、第三方补丁或额外流水线阶段。
- 旧 ADR 的历史正文保持原样，只修订状态、部分取代范围和索引，保留仍有效的折叠、Provider、codec 与生命周期规则。
- 版本 owner 已将 plan 24→25、Iceberg 9→10，生产 epoch 保留 15、test-only 保留 13：新 descriptor/closed manifest/revision 表达 W/T 与 COW 删除，没有另一个不可表达的 execution-only 改动。版本值已修改；旧输入严格拒绝与原生兼容仍待实际验证，源码版本值不能替代运行证据。
- 本设计的最终 package、SQL、System、native 与组合验证仍未完成；已有原始失败、跳过与 NOT_RUN/UNKNOWN 不因设计接受或局部通过而升级。

## 何时重新评估

- 独立负载证据证明修改比例、文件几何或维护成本使 MOR 不满足已接受目标时，重新评估双策略的成本；须重新裁决完整算法、类型、资源、计数和生命周期，不能恢复旧属性入口。
- 新 Provider 类型或转换获得原 owner 支持时，沿同一 V/W/T 与 ordinary conversion 作者评审，提供真实可达值、宽度、NULL 和分类证据。
- descriptor、closed manifest 或版本规则不能表达新的 execution 语义时，原版本 owner 单独评估 epoch；额外版本判定不能代替实际不同岛拒绝与兼容测试。
- 共享构造、资源或生命周期作者变化时，独立重核取消、最后持有者与真实退出，不以终态替代关闭。
