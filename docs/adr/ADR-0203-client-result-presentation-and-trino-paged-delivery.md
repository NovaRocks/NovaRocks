---
id: ADR-0203
title: "Frozen client presentation and acknowledged Trino pages have separate owners"
domain: [distributed-query-lifecycle, runtime-role, sql-language]
status: active
supersedes: []
superseded-by: null
date: 2026-10-28
implementation-status: implemented
provenance:
  - "discussion: 2026-10-27 and 2026-10-28 client result presentation, session values and acknowledged HTTP pages"
  - "reference: Trino 483, commit 50b0b50b75abd47f830b7805ee1b51716eb4065e (annotated tag object 32d4f28e8311ea6f67edca209df59a0493d869fa)"
code-anchors:
  - "novarocks/result-contract/src/render_schema.rs (ClientPresentationFamily, TrinoJsonFacts, ClientRenderSchema)"
  - "idl/novarocks/result.proto (ClientRenderSchema family and RenderPresentation)"
  - "novarocks/sql/src/compiler/root_render_type.rs (client_render_schema_from_port)"
  - "novarocks/result-render/src/ (bounded client row encoders)"
  - "novarocks/native-adapter/src/root_result_session.rs (root result encoding)"
  - "novarocks/query-application/src/session.rs (QuerySessionFactory and QuerySessionStatement)"
  - "novarocks/query-application/src/session_control.rs (ClientSessionKey, ClientQueryId and SessionOrigin)"
  - "novarocks/query-application/src/query_control.rs (QueryApplicationControl)"
  - "novarocks/query-application/src/protocol_delivery.rs (GovernedProtocolOwner and StreamingStatementResult)"
  - "novarocks/trino-adapter/src/ (HTTP pages, replay, client observation and listener lifecycle)"
  - "novarocks/frontend-application/src/server.rs (FE listener composition and drain)"
  - "novarocks-server/library/src/app_config.rs (StandaloneServerConfig)"
---

## 问题

分布式查询如何在 BE 编码结果、在 FE 投递无连接的客户端分页，同时准确区分结果计算、页送达、语句结算和客户端会话变化？

## 背景与执行事实

本条的呈现族、语句级会话与分页生命周期已实现。2026-10-28 完成本机规范定向 Rust、原生 1FE+3BE 协议/生命周期、兼容专项、MySQL session、doctest 与严格 Clippy 验证；本机追加的全工作区 Rust 仍记录未关闭的进程监护不一致，实际结果与处置分别保留。用户将 Linux 全量和 Trino 483 / Spark / 官方客户端对照留待手工执行，尚不具备这些验收结论。实现状态不代表正式准入或全部验证通过。

一条查询的原生输出是有类型的根结果契约，BE 按它编码，FE 消费根段。Trino 直连协议把若干完整 JSON 行放在一个 HTTP 响应中，客户端沿 `nextUri` 继续请求。一个 HTTP 请求的结束不能表达整条语句的生命周期；同一 URI 还可能因传输失败而被重试。

| 责任或事实 | owner | 不能由它推出的结论 |
|---|---|---|
| 原生类型、呈现族与呈现事实 | FE 规划作者冻结到 `ClientRenderSchema`，随 Native 根输出契约传递 | 不从展示类型、函数名或 Arrow 载体猜测另一种类型 |
| 编码后的行负载 | BE 根任务的 `result-render` 编码器；FE 本地结果使用同一编码器 | FE 转发端不获得重新解释单元格的语义权 |
| 行边界与 FE/BE 根段确认 | 既有行跨度游标、根结果投递 owner | 根段确认不证明客户端已经收到页 |
| 保留页、token、slug、墓碑与放弃计时 | FE Trino 适配器 | 单次 HTTP 写出成功或失败不结算语句 |
| 语句准入、取消、成功可见性与协议终结 | Query Application 的原语句 owner | seal 不证明客户端收到结果，也不证明仍在运行的工作已经退出 |
| 会话值及会话变化的语义 | Query Application；执行 `USE` / `SET` 的一方产出变化 | 适配器不能通过执行前后快照比较重建变化 |
| 客户端查询 ID 与中立会话键 | Query Application | 适配器不重建身份，MySQL 连接位置不成为 HTTP 查询配额 |
| 本机监听、Host 检查和 HTTP 头投影 | Trino 适配器，由 FE 服务 owner 监督 | 本机监听不等于任意浏览器来源都可信；Native JWT/TLS 不保护客户端 HTTP |

MySQL 仍保留自己的连接、写出和终结模型。ADR-0172 的原查询期限规则同时约束新载体；其累计活跃写出与 Closing 规则仍针对 MySQL，本条不将它标记为被取代。

## 考虑过的选项

**在 FE 接收 Arrow 后统一编码客户端 JSON。设计否决。** 该结构把所有客户端结果的编码 CPU、暂存与数据转写集中到单一 FE，破坏根任务编码、FE 转发的分工。FE 本地结果仍由同一个有界编码器处理，不据此把分布式结果编码移回 FE。

**不保留页，或依赖 BE 重发根段实现 HTTP 重放。设计否决。** 客户端会重试相同 URI，缺少原页无法提供稳定响应。一页又可能跨越多于常驻窗口的根段；延迟根段确认会阻塞 BE 产出，并把客户端重放责任混入 FE/BE 投递。

**由 HTTP 写出结果结算语句。设计否决。** 写出成功只说明字节进入传输栈；写出失败可能紧接着发生同 token 重试。两者都不是页送达证据，不能据此完成或杀死语句。

**对比执行前后的会话快照产生响应头，或在服务端按 HTTP 连接缓存会话。设计否决。** 客户端携带会话值，语句 owner 才知道显式设置及其类型化结果。快照比较和连接缓存都会增加第二个会话语义作者，也无法直接承接未来的预编译语句等会话变化。

**每条语句先检查请求头中的 catalog/schema 是否存在。设计否决。** 名称不是已解析对象；预查增加外部往返及准入，而且当前 schema 被删除后会妨碍客户端执行 `USE` 切走。

**让 Trino 查询和墓碑使用 MySQL 连接 id 与连接位置。设计否决。** 这些位置属于真实 MySQL 连接及其认证、协议尾部。HTTP 查询没有对应连接，墓碑占位会在高频短查询下挤占普通和控制位置。

**为重放另开结果预算，以维持原生行的全部大小范围。待评估。** 本条复用语句原有结果窗口，接受更窄的单行页上限；尚无独立容量需求支持第二份预算。若有真实工作负载要求，再由原结果资源 owner 评估窗口能力。

**忽略 Host，只核对配置用户名。设计否决。** DNS rebinding 可以把攻击域名解析到本机，使浏览器向本机 HTTP 端口提交并读取查询。用户名检查不能替代本机名称检查。

## 裁决

1. **冻结呈现规则。** 协议入口把呈现族作为语句准入输入；FE 规划时冻结 MySQL 文本族或 Trino JSON 族。Trino 族只携带影响编码的 `PARAMETRIC_DATETIME`、`VARIANT`、`VARIANT_BINARY` 事实。会话时区属于会话值，不进入呈现事实；带时区值按列类型自带的时区编码。Native 解码必须识别明确的族，缺失、未知值或跨族混用直接拒绝，不选择默认族。
2. **根任务编码规则。** BE 根任务按冻结规格以有界步进编码；每行仍是四字节小端长度前缀加行负载，行可以跨根段。FE 只识别行边界、移除帧前缀并套协议信封，不解析或重编码单元格。Trino 族的类型签名与文本规则以 Trino 483 为准；未映射的宽整数、无符号整数、opaque 与精度大于 38 的 decimal 在规划期具名拒绝，不换成近似类型。map 键遵循 Trino 原类型对象的 `toString()`：时间键保留原精度，二进制键用 base64；不能将标量的时间舍入规则套给 map 键。
3. **首次拉取准入规则。** `POST /v1/statement` 只校验 HTTP 输入、取得核心分配的查询 ID 并登记。首次 `GET` 才以会话值构造语句会话、准入并执行。Query Application 以不复用的 `ClientSessionKey` 保存所有会话，MySQL 连接另经索引供 `KILL` 查找。客户端查询 ID 与内部执行尝试 ID 保持不同职责；MySQL 的 `KILL <connection id>` 只寻址 MySQL 会话，服务排空及全体取消仍覆盖全部来源。
4. **页确认规则。** 请求 token n+1 才确认第 n 页。相同 token 返回保留的同一份响应字节；更旧 token 返回 410，未知位置返回 404。所有带数据的页都提供 `nextUri`，包括结果读完时的最后一个数据页。正常结束页只在数据都已确认后生成，不带 `data` 和 `nextUri`；无数据的语句可直接结束。根段一经复制进页缓冲便按原投递协议确认，不能为 HTTP 重放扣住 BE 根段。
5. **唯一结算规则。** 结果读到末尾时封存成功可见性；客户端请求结束页时才 `complete`。执行失败、客户端显式取消、放弃、结束页请求和服务取消之间只允许一次结算。`DELETE` 以 `ClientCancelRequested` 取消并返回 204，后续错误页为 `USER_CANCELED`；放弃以 `ClientDisconnected` 结算，后续为 `ABANDONED_QUERY`。已封存后的取消或放弃记为送达未完成，不推翻已决定的语句成功或已发生的外部效果。
6. **结果窗口规则。** 保留页与正在组装的页共用该语句已经领取的结果窗口，Client 根段路径扣除两个常驻根段；Local / Internal 本地结果路径扣除原图 buffer 容量、schema backing、编码器暂存与适配器输出块，再扣列描述与两个页信封并进行静态总额核对。构造阶段原有 93 MiB 峰值检查保持不变。达到页目标或总额不足时使用背压；没有保留页可释放、单行仍无法容纳时，以 `GENERIC_INSUFFICIENT_RESOURCES` 拒绝。结算后窗口随原语句释放，墓碑只保留结束页响应；登记与墓碑按放弃期限回收，不另设查询数量配额。资源退款仍以实际持有者退出为准。
7. **三类时间规则。** 执行等待只观察原查询期限与取消。每个 HTTP 响应 body 从首次 poll 起有 30 秒写出期限；写出失败或到期只中止这个响应，页继续保留，不向语句驱动发送取消。两次客户端请求间由独立的放弃期限约束，默认 300000 ms，针对查询的 `HEAD` 也刷新它；长轮询最多等待一秒新的完整行或状态。结算墓碑沿同一放弃期限移除。
8. **会话值与变化规则。** catalog/schema 请求头只写入规范化名称，在真正解析或执行 `USE` 时检查存在性。请求头中的属性由原属性语义作者校验，时区校验后保存在会话值中。`USE` / `SET` 的实际执行者产出类型化变化，Trino 适配器投影 `X-Trino-Set-Catalog`、`Set-Schema`、`Set-Session`、`Clear-Session`；MySQL 连接会话在内部应用同一变化。查询返回的语句种类决定 `updateType`，不能从 SQL 前缀重猜；没有真实逻辑写计数时不填 `updateCount`。`USE` 同时清除旧 catalog 伪属性，`SET` 写回规范属性名并清理旧别名，不增加 SQL `RESET SESSION`。错误沿类型化映射投影，保留 NovaRocks 稳定错误码，不按错误文本分类。Trino 适配器只截断展示 detail，消息的 JSON 线上表示（含引号）最多 16 KiB；核心原始错误保持完整，截断时 FE 日志记录原文与客户端查询 ID。
9. **统计事实规则。** 查询状态、排队和经过时间来自适配器实际生命周期；原语句 owner 尚未暴露 stage 调度遥测，因此 `scheduled` 为 `false`，不由驱动启动或非排队状态推断，其他无来源的数值保持 0。
10. **本机服务规则。** Trino 监听固定绑定 `127.0.0.1`，只接受主机名为 `localhost`、`127.0.0.1` 或 `[::1]` 的 Host，允许可选数字端口；其余 Host 返回 403。语句用户须等于同一 `[standalone_server].user`，不匹配返回 401。通过检查后生成 URI，保留原本机名称和是否显式指定端口，数字端口规范化，例如 `localhost:0000008080` 生成 `localhost:8080`。该规范化界定输出信封，不能反向成为请求头长度限制。FE 先绑定 Trino 监听，再发出整体就绪标记；排空拒绝新的提交及尚未执行查询的首次拉取，已执行查询继续服务。最终清空登记并等待监听实际退出，不以发送关闭信号代替 Join。

## 接受的妥协（诚实记录）

带数据的结果多一次请求来确认最后一个数据页，查询的并发许可与结果窗口也会持有到这一确认；这是稳定重放和真实页送达证据的代价。放弃期限提供最终释放，客户端可以用 `HEAD` 保持仍在使用的查询。

页预算复用原窗口，使一行在原生根传输中合法，却可能在 Trino 页中因空间不足而失败；调大原生行上限不能补足这个窗口。保留页与一次重试中的旧响应可能短暂共享同一字节 backing，不能按 HTTP 请求数重复计费或提前退款。墓碑按时间保留，不以 MySQL 位置数量限制它。

本机入口使用配置中的单个用户名，当前没有客户端 TLS、非空认证或可配置的远程 Host 白名单；Native trust 的安全层只用于 FE/BE Native 传输。扩展访问面需要新的客户端安全契约。本条也不交付 spooling、多语句事务、预编译语句、查询详情页或新的 SQL 方言语义。会话时区被保存，但其 SQL 语义需由相应时间语义 owner 明确定义。

本条没有替代 MySQL 投递模型。两种呈现族与适配器暂时共存，MySQL 连接仍按自身协议终态结算；HTTP 页确认模型不能改写 MySQL 的连接行为。协议实现、源码或局部通过不能替代 Trino 同类型编码对照、官方客户端、1FE+3BE 及目标提交的完整验证事实。

## 何时重新评估

- 真实工作负载需要超出当前页窗口的合法单行时，由原结果资源 owner 评估窗口或新的投递模式；不能给适配器私开第二份预算。
- 引入 spooling 时，重新定义页/对象的送达确认、重放、访问凭据、保留与实际释放；不能把直连页墓碑直接当作对象生命周期。
- 引入远程访问、TLS、多用户认证或网关时，一起裁决身份、Host、公开 URI 与授权边界；不能仅把监听地址改为 wildcard 或扩充白名单。
- 引入新的 SQL 方言、时间语义、预编译语句或按查询 ID 的控制面时，扩展原会话值、类型化变化与核心身份接口；不重新建立会话快照差分、HTTP 连接会话或第二查询身份来源。
- 删除 MySQL 载体时，统一评估本条与 ADR-0172 的关系及仍需保留的查询期限规则；不能因新增 HTTP 入口提前宣称 MySQL 裁决已失效。
