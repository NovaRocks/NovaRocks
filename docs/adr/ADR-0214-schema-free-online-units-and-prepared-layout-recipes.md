---
id: ADR-0214
title: "Schema-free online units and prepared layout recipes"
domain: [pipeline-execution, sql-compiler]
status: active
supersedes: []
superseded-by: null
date: 2026-11-03
provenance:
  - "discussion: 2026-11-03 preparation-bound Arrow units, logical requiredness and evidence-based capacity limits"
code-anchors:
  - "novarocks/arrow-batch-codec/src/recipe.rs (BatchRecipe)"
  - "novarocks/arrow-batch-codec/src/unit.rs (encode_unit, decode_unit)"
  - "novarocks/execution/src/runtime/exchange/online_unit.rs (ExchangeRecipe)"
  - "novarocks/native-adapter/src/root_cow_selection_codec.rs (CowSelectionRecipe)"
  - "novarocks/native-adapter/src/root_result_session.rs (CowFlight)"
  - "novarocks/type-contract/src/schema.rs (arrow_data_types_exact_with_profile_and_nullability_observed)"
---

## 问题

已经共享完整冻结布局的执行端，如何传送独立的数据单元，同时保持类型权威、实际资源责任和取消后的真实退出？

## 背景与执行事实

布局与数据有不同的来源和生命周期。完整语义由已冻结的 FunctionValueType / TypedValueBinding 持有；实际 Arrow 编码、切片、父 NULL、字典值 NULL 和 buffer 别名属于本次载体。字段展示名、首个数据包和物理 nullable 标记均不能重新定义逻辑域或 requiredness。

| 实体 | 权威和承载 |
|---|---|
| 中性 `BatchRecipe` | 完整语义声明、规范字段顺序、有限表示政策、确定字典关联与摘要；没有 Task、Connector 或字节授予权 |
| `ExchangeRecipe` | 从实际 LocalProgram 输出/导入布局准备；准确边、来源集合及接收准入仍由原角色持有 |
| NRU1 单元 | 摘要及表示 tag，随后为本单元完整 DictionaryBatch 和一个 RecordBatch；没有 Schema、delta 或跨单元字典状态 |
| `CowSelectionRecipe` / CSA2 | FE 与 BE 各自从准确 root 声明准备，record 绑定原摘要；segment、ACK、重放和 End 保留原责任 |
| 构造 owner | 在实际分配前向原预算请求，分别保管结构、数组和临时工作；metadata 保管不产生字节授权 |
| 原 driver / native CPU / root producer | 拥有同步调用和实际销毁；停止观察、取消或超时均不能代替这些工作真实退出 |

Arrow IPC Schema 消息包含递归 Field/type 表。arrow-ipc 60.1 的普通 stream reader 使用其固定的消息校验入口，Schema 结构会把类型包装展开为更多 FlatBuffer 层。RecordBatch 则是 FieldNode/Buffer 的扁平列表，DictionaryBatch 引用该列表；它们的 FlatBuffer 表嵌套与 SQL 类型深度无关。去掉重复 Schema 消息，保留标准批次消息即可消除这条传输边界，毋须修改第三方库。

业界对照是机制参照，不是相同格式的证明：Velox 的 [PrestoSerializer.h，bfd81c220604](https://github.com/facebookincubator/velox/blob/bfd81c220604acb0d10c8b7bc53831f5092b4c2f/velox/serializers/PrestoSerializer.h) 在反序列化接口接收调用方 RowType；Trino 的 [PagesSerdeUtil.java，f7532f393ec5](https://github.com/trinodb/trino/blob/f7532f393ec5a538dcc6634d9eb35f30795af5c2/core/trino-main/src/main/java/io/trino/execution/buffer/PagesSerdeUtil.java) 由行数与 BlockEncodingSerde 读取物理 blocks，接口不接收完整 SQL RowType。二者都不意味着应把 SQL 逻辑类型权威交给每页的数据描述。

## 考虑过的选项

1. **继续逐单元发送完整 Schema：设计否决。** 接收端已有准确布局，重复 Schema 不能增加语义权威，却继续把数据传输限制在递归 metadata 表的边界；首包学习还建立第二个 schema 来源。
2. **修改或 fork Arrow 的默认校验器：设计否决。** 公开批次编码、消息校验与解码接口已经足够。为重复 metadata 放宽校验或维护补丁，不解决重复类型来源及 owner 分工。
3. **自建完整列式数据格式：成本否决。** 可以统一全部编码细节，但会重新承担 Arrow 类型、字典、slice 与 kernel 互操作的维护。本方案复用标准 Arrow 批次，自有 COW 记录仅保留现有支持集。
4. **准备期 recipe 与无 Schema 独立单元：采用。** 完整声明只冻结一次，每单元仍完整、可独立校验；资源与生命周期留在实际角色。
5. **为包装层、节点或 record 大小增加本地配额：设计否决。** 这些配额不能替代原内存预算和背压。表示位宽、实际库边界或实测约束才可成为拒绝依据，必须同时说明保护对象与拒绝必要性。

## 裁决

1. **准备期唯一布局。** 发送与接收各自从同一冻结布局准备 recipe。摘要包含完整逻辑声明及对应载体政策；本地 slot ID 和根展示名由角色映射。不得从首包、字段注解或当前数据猜测声明。
2. **独立标准批次。** 每个 NRU1 单元重新建立 Arrow DictionaryTracker / IpcWriteContext，发送本次完整字典及一个 batch。字典 ID 按同一确定遍历重建。校验摘要、表示 tag、消息种类、dictionary 来源、FieldNode/Buffer 几何及尾部；不接受 Schema、delta、额外 batch 或跨单元缺省状态。零列批次保留真实非零行数。
3. **逻辑 requiredness 与物理 NULL 分开。** wire carrier 允许较宽的物理 nullable 标记；Map entries/key 与 run ends 保持 required。完整类型的名字、metadata、encoding、参数仍精确，宽化不授予新的 Dictionary/REE kernel 能力。编译表达式输入按原完整 channel 声明验证 active rows，复用借用式父 NULL、offset 与 selected-row 作者，不构造 depth×rows mask，也不把载体收紧成会拒绝合法 unused child NULL 的声明。
4. **一个中性机制 owner。** Schema/stream/reader/递归几何在 `arrow-batch-codec` 中共用；functions、plan-codec、Execution 使用其机制。常量的单字段、ConstantPool 准入及完整 stream 生命周期归原 owner。类型 owner 显式选择 raw Physical 或完整值的 profile；完整值不能误送回 raw 的物理计深政策。
5. **按实际构造向原预算请求。** references、nodes、buffers、dictionary、alias 放大和算术均在调用前核对；重叠 buffer、对齐修复与 synthetic logical-null bitmap 的实际分配不能只按 wire 长度估计。所有输出数组在发布前续接原来源责任，最后裸 buffer 别名退出后再释放对应持有。独立 Schema lifetime 不持有数组预算。继承的通用退款及类型准入政策仍由各自 owner 复审，不在 codec 另建钱包或次数配额。
6. **容量与表示边界分开。** COW record 不设 256 MiB 或派生节点硬上限；长度与目录数受真实 wire 位宽和受检算术约束，内存由原结果预算承担，生产端沿原分批和背压推进。64 KiB / 1024 工作轮次用于让出和取消响应，不是总输入拒绝阈值。Map 的 entries 包装算实际节点与工作，推导的物理深度只作栈测试几何，不是新增拒绝条件。
7. **原线程与真实退出。** driver 明确请求 2 MiB；native blocking 与 FE 规划/计算 CPU 使用各自原 16 MiB owner；root producer 默认 1 MiB，并沿既有配置。COW 编码游标只由原 producer 持有，借用输入和 generation 后在 Mutex 外构造、推进、销毁，再回锁发布；迟到结果不能覆盖取消。测试使用原 owner 的线程和 join，不以 RUST_MIN_STACK 或测试专设大栈证明生产安全。dev/release、输入形状与实际退出分别取证；本 ADR 不把默认栈数值当作最小栈峰值测量。
8. **格式与版本明确隔离。** 普通 root 结果保留 NRX1 / 带 Schema IPC，不能通过修改共享编码入口一并切换。生产 Native compatibility epoch 为 14，test-only 岛 13 保留；旧格式没有透明解码回退。授予、Installed、同序号不重复解码、End、Consumed、normal-close 与 retirement 仍按 ADR-0209，不因 payload 格式替换而重建流控。
9. **后续复用入口。** 在线单元的分片、重组与压紧消费 `BatchRecipe`、`encode_unit` / `decode_unit` 及原单元 custody，不另造带 Schema 单元或重复语义目录。字节、位置、计算完成与真实退出保持各自 owner。

## 接受的妥协（诚实记录）

- 本格式依赖先准备的准确布局，不能拿一段未知 NRU1 字节作为独立的 SQL schema 文档。摘要是已冻结身份的一致性校验，不是可替代原 Task/来源授权的能力。
- 完整字典在每个单元重发，保留清晰的重放和独立解码代价。当前不引入跨单元状态或 dictionary delta。
- arrow-ipc 60.1 读取字典仍依赖已弃用的 Field dict_id 关联。使用点显式标注弃用理由，这不是长期冻结该上游接口的承诺。
- COW 保留其现有闭集和下游 row mutation 边界；格式独立不代表 Provider、函数目录或所有业务路径已经支持每一种深类型。未实现能力与实际错误、未运行和未知清理分别报告。
- 私有 Arrow 构造几何使用固定依赖下的保守预算模型，不等同于精确 allocator 发票、RSS 上界或性能证明。更一般的临时峰值退款、继承类型上限及下游 COW 上限仍需在原 owner 中独立审视。
- 请求的线程栈与成功完成某组输入，只证明该构建和 owner 下的功能范围；不证明未覆盖算子、全部形状或最小物理栈需求。

## 何时重新评估

- Arrow 升级移除 dict_id，或改变 DictionaryTracker 的关联顺序、decoder 分配/构造行为时，更新唯一几何与字典接口；无可用公开替代时先重新裁决，不保留猜测式兼容。
- 需要跨单元字典、压缩、分片或多 batch 单元时，先明确原单元重放、完整性和资源责任，修订格式及兼容身份。
- 标准批次接口不能满足已接受的类型/载体能力，且自建格式的收益与维护成本有实际依据时，重新评估完整自有列式格式的成本否决。
- 类型合同或函数/Provider 能力扩展时，在实际 owner 上增加最大合法输入的 dev/release 往返与取消退出证据；不据已有样本推导新的拒绝阈值。
- 资源模型与真实构造不符，或预算产生不可接受的保守拒绝时，修正原 owner 的估算、配置和退款时点；不以追加 codec 配额替代修复。
