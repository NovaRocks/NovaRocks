---
id: ADR-0180
title: "Inline keyed documents separate immutable semantics, ordered slots and compact snapshot records"
domain: [frontend-mv, provider-spi]
status: active
supersedes: []
superseded-by: null
partially-superseded-by:
  - id: ADR-0224
    scope: "Ruling 1 document-version, aggregate-private-property and snapshot-record capacity quotas, and the first reevaluation trigger; inline storage, identity, request shapes, lifecycle, evidence and external rollback boundaries remain active"
    link: "ADR-0224-mv-document-capacity-comes-from-real-boundaries.md"
partially-supersedes:
  - id: ADR-0154
    scope: "Document storage and attachment rules: inline semantic versions, binding documents, slots and snapshot records; lake authority and domain ownership remain unchanged"
  - id: ADR-0171
    scope: "Request-shape table for document storage: marker-only MetadataOnly, zero-file SnapshotProducing slot changes and all-observed-ref expiry; immutable operations, dependency validation and artifact ownership remain unchanged"
date: 2026-10-15
provenance:
  - "discussion: 2026-10-14 accepted inline keyed document storage, snapshot ordering, outcome evidence and external rollback boundary"
  - "discussion: 2026-10-15 accepted implementation contract including conflict termination and dangling-root repair"
code-anchors:
  - "novarocks/spi/src/connector/document_storage/mod.rs (document identity and keyed storage contracts)"
  - "novarocks/connector/iceberg/src/document_storage/keyed/mod.rs (keyed codec, views, resolution and retention)"
  - "novarocks/connector/iceberg/src/commit/model/request.rs (RequestShape)"
  - "novarocks/connector/iceberg/src/commit/attempt.rs (run, RetryPolicy)"
  - "novarocks/connector/iceberg/src/commit/snapshot_lifecycle_helpers/rewrite_manifests.rs (snapshot record carry)"
  - "novarocks/mv-application/src/persistence/keyed_documents.rs (MV version, slot and snapshot-record mapping)"
  - "novarocks/frontend-application/src/mv/domain/iceberg_refresh.rs (configuration slot changes)"
---

## 问题

湖上的应用文档怎样同时支持迟到提交、准确的快照解释与历史回收，而不依赖外置私有文件或本地恢复日志？

## 背景与执行事实

表属性随每次 Iceberg metadata 提交整体保留；快照 summary 随快照保存。REST 条件提交可以断言表 UUID
和某个 ref 的快照，却不能断言属性值、完整 ref 集合或 metadata 版本。客户端失去响应不代表请求没有
生效；服务端可能在客户端结束后才检查条件。因此，不能用读后写整份文档清单来模拟局部更新，也不能
用进程内串行保证一个旧请求在重启后不再生效。

本裁决记录已接受的设计。落盘时新合同仍待产品集成及验证；`code-anchors` 标出承重 owner 与接线位置，
不表示这些位置已全部实现新语义，也不提供测试成功或生产资格证明。

| 对象 | 保存的位置与身份 | 解释责任 |
|---|---|---|
| 文档版本 | 表属性；按完整语义摘要寻址，不可变 | 应用定义格式、内容与引用；SPI 定义共同身份；provider 编码、校验与保存 |
| 绑定文档 | 一种文档版本；集中保存稳定的解释事实及其引用 | MV 当前引用定义 D 与解释 L；未来稳定绑定仍在这里演进 |
| 槽 | 表属性；按 owner 与 name 定位，值含最后写入的预选快照 ID | 配置及合并后的管理身份各为一个槽；初值没有写入快照 ID |
| 按键标记 | 表属性；按 owner、name 与绑定身份定位，同键同值 | 应用决定发布或 attempt 的事实及何时过期 |
| 快照记录 | 快照 summary；所在快照就是准确附着 | provider 只解释帧与绑定引用，应用解释紧凑载荷 |
| 根键 | 表属性；每个 owner 一个键，指向其当前绑定文档 | 是回收根，不是当前数据的发布证明 |

MV 的计算定义、物化解释与发布语义仍属于 MV Application；provider 拥有物理表、快照/ref、manifest、
原子提交及保留事实。FE ManagementEntrance 按目标串行拥有业务写准入；本地 StateStore 仍只是可重建的
Accelerator，不能补齐湖上缺失的事实或授权重新派发。

## 考虑过的选项

1. **整包清单与 latest 发布属性。设计否决。** 写入方便，但迟到的局部变更会恢复其他文档的旧版本；
   latest 属性在回滚之后也不能证明对应当前数据。只保留最新 revision 同样会让历史记录失去解释。
2. **表目录或仓库中的私有外置文件。设计否决。** 普通 Iceberg 元数据不会把它们视为被引用的对象；
   当孤儿清理的扫描范围覆盖其所在目录时，可能删除正确性所需的字节。把它们登记为 statistics 也不成立：
   统计槽可被其他引擎替换，生命周期不属于应用文档。
3. **引擎侧持久存储。设计否决。** 湖上数据无法独立恢复其解释，破坏唯一湖上权威。
4. **伴生 Iceberg 表。成本否决。** 文档作为数据行可以取得标准文件引用保护，但每个 MV 增加一个对象，
   并需重新定义名字生命周期、隔离与跨对象原子性。当前接受内联上限；真实文档超出支持规模时重新评估。
5. **metadata-only 槽写入加进程冻结或本地恢复日志。设计否决。** 冻结跨不过重启；本地日志跨不过 FE 换机。
   即使请求带 main 条件，只要不推进 main，旧值仍能在较新槽值之后落地。跨进程写入顺序应由湖上提交保证。
6. **每次发布扫描完整历史并回收。成本否决。** 能求完整存活集，但普通刷新大多不改变绑定，却为全部历史付费。
   选择生命周期过期时回收、发布时重写引用闭包；只有后者无法满足真实规模时再评估回收调度。
7. **四类按键内联、快照顺序与生命周期回收。采用。** 同键同值负责无条件迟到写入，推进 main 负责可变槽顺序，
   快照记录负责准确附着，完整加载的过期提交负责版本回收。
8. **额外代际 tag 防止外部回滚 ABA。成本否决。** 它可提供额外围栏，但新增 ref 与协议责任；当前采用
   Iceberg 同类元数据清理的风险边界，不在存储表上建立代际 tag。若产品要求抵御外部回滚，再重新裁决。

## 裁决

### 1. 按键保存完整语义，全部内联

保留前缀为 `novarocks.doc.`，本格式为 `novarocks.doc.v2.`。文档版本使用 `rev.<identity>`，槽使用
`slot.<owner>~<name>`，标记使用 `mark.<owner>~<name>~<binding>`，根键使用 `root.<owner>`；
快照 summary 中使用 `record.<owner>~<name>`。身份文本为 64 位小写十六进制。owner、name 与 binding
各匹配 `[a-z0-9][a-z0-9._-]{0,63}`；分隔符不会出现在 token 内。通用属性修改和建表属性入口拒绝保留前缀
与旧文档键，只有文档 owner 的 typed 通道能写这些键。

文档身份由 SPI 共同定义，对存储版本、owner、name、格式 owner/name/version、有序的关系名与目标身份、
内容求 SHA-256。编码使用版本化域分隔和长度前缀；关系按名字升序且不得重复。相同内容换引用或格式
就成为不同版本。快照 ID、存放位置等非语义事实不进入摘要。

版本、槽与标记采用确定性 JSON 外壳，内容是无填充 base64url；版本值含身份摘要，槽和标记含完整性摘要。
槽的写入 ID 参与完整性校验，由 provider 填入，调用方不能指定。读取须重算摘要并重新编码，与原字节
逐字节比较；未知版本/类别、错误位置、非法 token、非规范字节或键值不符均报含具体键的 CorruptData。
不截断，不分块，不回退到外置载体。

| 上限 | 计量范围 | 派发前检查时机 |
|---|---|---|
| 64 KiB | 单个版本外壳值 | 产生新文档版本 |
| 256 KiB | 提交后所有 `novarocks.doc.` 表属性的键和值 | 新版本、写槽或写标记，包括发布同时写标记 |
| 4 KiB | 单条记录键和值 | 记录结构变化；按绑定声明的最大版本值长度推算 |

普通刷新只引用已有绑定且不写标记时，不重新触发版本与合计预算检查。超限错误列出类别、名字、实际大小
与上限；属性合计还说明被保留快照引用的字节。历史变长不会单独触发定义预算；这三个值也不证明整份
metadata.json 有界。

### 2. 稳定绑定只存一份，记录只保存变化事实

绑定文档 B 引用 D/L，保存各 occurrence 的数据版本格式及最大值长度。P 的记录帧为
`2.<binding-identity-or-dash>.<base64url-payload>`；`-` 允许无绑定记录。帧不重复 owner、格式字符串、
引用列表或摘要。所在快照提供准确附着，不再使用 TableMetadata、CommitOutput、ExactOutput 三类附着。

MV 的 P 当前只保存发布 ID、绑定引用、冻结时间、按 D occurrence 顺序排列的数据版本值和逻辑结果行数。
对象身份来自 D，格式来自 B，D/L revision 经 B 引用；刷新种类、输出身份、是否为空和处理行数不持久化。
`total-records` 是文件记录数，不扣除删除文件/DV，不能替代逻辑结果行数。将来标准 `refresh-state` 接管
源快照与刷新时间时，与私有记录各用自己的 summary 键，不把私有字段混入标准格式。

D/L/B 的 revision 是完整语义身份；配置 revision 只取配置内容摘要，不随同值槽写入的 writer 改变；
P revision 取记录帧字节摘要。普通刷新复用 B，不为每个快照复制稳定绑定事实。

### 3. 写入形状由副作用决定

| 通道 | 冻结条件 | 可以做的事 |
|---|---|---|
| Create | assert-create | 原子写入版本、初始槽及根键，不制造文档快照记录 |
| MetadataOnly | 表 UUID | 仅增加本次绑定的按键标记，不删键、不写槽/版本/根键 |
| SnapshotProducing 发布 | 表 UUID、main 与所需 schema/spec | 写自己的记录、版本、根键与标记，重写闭包，可删除过期标记 |
| SnapshotProducing 槽变更 | 表 UUID、冻结的 main；无快照时断言 main 不存在 | 零文件快照，写槽、带上父记录、重写闭包及修复根键，可删除过期标记 |
| SnapshotProducing 维护 | 表 UUID、冻结的 main 与所需 schema/spec | 不改逻辑数据，带上父记录、重写闭包及修复根键 |
| 文档过期 | 表 UUID、完整加载中每一个 ref 的准确 snapshot ID | 只移除选定快照及失去引用的 `rev.*` 属性，不产生快照、不写属性、不删文件 |

这是对 ADR-0171 请求形状表在文档存储中的补充，不把普通统计等 MetadataOnly 操作限制为标记。
不可变 intent、依赖验证、冻结后不补条件以及原有 artifact ownership 规则继续成立。

### 4. 每次用户槽变更都推进 main，冲突即结束

配置修改与管理交接都必须提交；即使值相同、当前 owner 相同，也不提前返回。槽变更沿用父 manifest，
只写一份新 manifest list，原样复制父快照全部记录。有父快照时标 `replace`；表上没有快照时为空
`append` 且无记录。槽值记入本次预选快照 ID。已有父快照但没有私有记录时仍为 `replace`、记录为空。

依赖为 RefUnchanged；确定冲突报告未生效，不在新 main 上自动重新准备。这样一次用户请求不会越过
已完成的新操作而重新覆盖槽。任何后续推进 main 的提交都会挡住此前在途的请求，跨重启不需要槽专属
冻结、恢复日志或效果时限。按名字的建表/删表隔离及原有管理重新准入约束仍由其 owner 负责。

NovaRocks 不改变逻辑数据的维护同样原样带上全部父记录；发布及退役写自己的记录，零文件时为
`append`，不能伪装为 `replace`。外部维护没有私有记录时如实返回“当前快照没有记录”，应用不从历史
拼出当前可服务性。将来新增标准记录时，应同步加入带上记录的集合。

### 5. 回收属于生命周期提交，发布重写自己的闭包

MV 生命周期 owner 决定何时及移除哪些快照；provider 在同一次 `snapshots=all` 完整加载上求可删版本。
存活集等于各 owner 可解析根键的闭包，加全部保留快照记录引用的闭包。表属性版本减去存活集才可删除。
不移除任何已观察 ref 的头。没有快照要移除但存在孤立版本时，允许只删版本。

过期以完整观察令牌的加载为基线，不重新加载、不得自动重试；冲突返回生命周期 owner 重新规划。
updates 只允许 remove-snapshots 和删除 `rev.*` 的 remove-properties，逐个断言全部已观察 branch/tag/main。
槽不删除；标记由应用确认过期，再随带 main 条件的提交删除。文档版本回收默认开启。

每次写记录或根键的提交，都通过 set-properties 原样重写提交后根键及新记录所引用的版本闭包。回收先
落地时，新提交把重新引用的键写回；新提交先落地时 main 前进，旧回收被拒绝。此重写同键同值，只增
请求体，不增加相同版本属性。

发布、槽变更及维护使用 `snapshots=refs`；正常 MV 应用路径不扫描历史记录。过期、诊断及需要完整证据
的收束使用 `snapshots=all`。服务端忽略 refs 参数仍保持正确，只是多传数据。Iceberg 本身仍须序列化
完整 metadata.json，不能把应用不扫描历史表述为提交成本不随历史增长。

### 6. 证据分四种结论，收束不授予删除权

| 结论 | 必需证据与行为 |
|---|---|
| 已生效 | 某次加载 main 等于预选快照；槽 writer 等于它；可见预选快照且记录相符；保留快照父指针指向它；或保留快照带有完全相同的记录。结束观察 |
| 确定未生效 | 服务端明确拒绝；或完整加载中有完整祖先链反证：main 从断言父快照经另一后继前进，预选快照从未出现。后者以没有外部回滚为前提。结束观察 |
| 已失效、结果未定 | main 已越过断言父快照，旧请求在没有外部回滚时不能再生效，但过去是否生效的证据已消失。结束观察，报告“未确认，已被后续提交取代” |
| 仍未知 | main 仍等于断言父快照，迟到请求仍可能生效。继续观察 |

只见 refs 且正证不足时，必须完整加载再判断。断言父快照为空时，反证要求 main 祖先链完整到首个快照；
祖先链缺口不构成未提交证明。“预选快照不存在且 main 已前进”本身不能证明未生效。

收束器只交付结论。只有服务端明确拒绝，attempt 才立即清理真实属于自己的产物。完整反证与“已失效、
结果未定”都不能立即删文件，只让产物按既有年龄门、重新检查存活集及准确对象身份成为普通 GC 候选。
仍未知不重发、不中止外部效果、不删除。清理与提交结论保持独立，不能把观察超时转换为未提交。

这套结论适用于共同的存储收束器；接线时 FE 先用于槽变更。发布的应用收束、重启屏障退出以及未知产物
的跨进程隔离是独立演进项，不能从本裁决推断已经投入生产。

### 7. 外部回滚边界只损害相关历史

不增加代际 tag。外部回滚若让 main 回到旧请求断言的父快照，迟到的发布或槽变更可能重新生效。
迟到回收在“版本重新被引用、外部回滚、回收落地”时也可能删掉被回滚掉的历史所需版本。这是采用
Iceberg 同类 schema/spec 清理风险模型的边界，不以本地时限或更宽松的错误推断掩盖。

读取严格解码所有已加载的私有键，严格校验当前快照引用闭包；当前闭包缺失失败关闭。非当前快照仅仅
缺少引用版本时，诊断为“历史无法解释”，不使当前 MV 不可用。根键悬空视为过时，不作为当前发布证明；
下次槽变更或维护若带上的记录有可解析绑定，则根键改指它并重写闭包，否则保持原根、不把悬空部分
纳入闭包。后续发布用新绑定修复根键。非规范编码或摘要损坏不属于历史缺引用的降级例外。

全部已观察 ref 都受条件保护，但规划之后新建且指向待删快照的 ref 不受保护，catalog 可能随快照删除它。
REST 缺少完整 ref 集合与 metadata 版本条件；不能把逐 ref 条件宣传为完整的 ref 集合围栏。

### 8. 部分替代与实施边界

本 ADR 部分替代 [ADR-0154](ADR-0154-mv-documents-are-the-lake-authority.md) 的存放与附着规则，
保留其湖上唯一权威、领域语义/provider 物理事实分工及 Accelerator 边界；部分替代
[ADR-0171](ADR-0171-commit-operation-model.md) 的文档请求形状，保留不可变操作、依赖验证、staging 与
owned artifact 生命周期。旧格式不迁移、不双读；识别为 LegacyFormat 后仅允许 DROP TABLE。

文档过期通道不等于生命周期服务已经开启。保留窗口、下游和读者窗口、自动维护开启、文件删除以及旧
metadata 文件清理仍由真实生命周期 owner 交付；本裁决不承诺它们已完成或已经限制历史总量。

## 接受的妥协（诚实记录）

- 全内联拒绝超过支持规模的定义或绑定；每次提交仍重写完整 metadata.json，版本池与保留窗口会影响成本。
  选择伴生表需要额外对象与跨对象生命周期设计，当前为此接受内联上限。
- 每次配置或管理变更增加快照与 manifest list；同值操作也付费。读取这张 MV 的下游可能沿依赖链各做一次
  无变化刷新，这是跨进程顺序保证的实际成本。
- 稳定绑定增加一次引用解析；闭包重写增加请求字节。将回收放在生命周期路径，使普通应用发布无需扫描历史，
  同时把删除延迟到生命周期 owner 下一次运行。
- 外部回滚 ABA 和规划之后新增 ref 是明确边界；接受相关历史可能无法解释，不承诺抵御任意第三方 metadata
  修改。当前缺少 metadata 级条件，逐 ref 比较无法补齐该能力。
- 证据可能被外部过期删除，因此有些请求永远无法确认过去是否生效；诚实结束观察并保留 GC 规则，会延迟空间回收。
  没有把失效证明升级成删除授权，也没有宣称实现未知产物的持久隔离。

## 何时重新评估

- 真实文档超过 64 KiB、绑定规模导致记录超过 4 KiB，或保留窗口内版本池频繁逼近 256 KiB 时，重新讨论伴生
  Iceberg 表；必须同时解决对象生命周期、隔离与跨对象原子性，不直接恢复私有外置文件。
- 闭包重写或生命周期完整加载成为实际瓶颈时，按负载证据重新比较回收调度与引用表示，保持完整存活证明和准确附着。
- REST 提供可依赖的 metadata 版本或完整 ref 集合条件时，用它取代目前逐个已观察 ref 的条件，重新检查新增 ref 窗口。
- 产品要求抵御外部回滚造成的迟到请求 ABA，或要求所有历史永久可解释时，重新评估代际 tag/更强外部围栏及其生命周期。
- 需要更长保留、跨 FE 接续或持久未知产物隔离时，由相应 owner 单独交付；不得用本地 Accelerator 扩大本 ADR 的授权。
