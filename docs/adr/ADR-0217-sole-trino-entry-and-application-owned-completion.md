---
id: ADR-0217
title: "Trino is the sole client entry and completion retains application authors"
domain: [distributed-query-lifecycle, runtime-role, sql-language, error-contracts, frontend-dml]
status: active
supersedes: []
superseded-by: null
partially-supersedes:
  - id: ADR-0102
    scope: "The live MySQL connection registry, connection-token control API and connection-addressed KILL surface; application authorization, original cancellation identity, protocol/application ownership separation and actual-exit rules remain active"
    link: "ADR-0102-mysql-kill-connection-lifecycle-ownership.md"
  - id: ADR-0172
    scope: "The MySQL writer-specific cumulative active-delivery budget, writer receipts, Closing phases and startup projection; original query deadlines, typed cancellation/failure and actual resource-exit rules remain active"
    link: "ADR-0172-mysql-query-deadlines-and-active-delivery-work-budgets.md"
  - id: ADR-0095
    scope: "The MySQL ErrorKind, errno and SQLSTATE projection; analyzer-owned code/span, transport-neutral UserError and the existing ADR-0187 partial supersede remain active"
    link: "ADR-0095-typed-sql-analyze-errors-and-wire-mapping.md"
  - id: ADR-0203
    scope: "The dual MySQL/Trino client-family transition, MySQL connection-index references and incomplete command-result projection; explicit Trino facts, root framing, page replay/acknowledgement, settlement, session effects and listener security remain active"
    link: "ADR-0203-client-result-presentation-and-trino-paged-delivery.md"
date: 2026-11-05
provenance:
  - "discussion: sole statement-scoped client entry, static completion shape and application-owned logical counts"
  - "reference: Trino 483, commit 50b0b50b75abd47f830b7805ee1b51716eb4065e"
code-anchors:
  - "novarocks/query-application/src/session.rs (StatementAdmission, StatementOutputShape)"
  - "novarocks/query-application/src/session_control/statement_action.rs (StatementCommandCompletion, StatementCompletionProjection, prepare_completion_rows)"
  - "novarocks/query-application/src/sql/admission.rs (admit_statement_session)"
  - "novarocks/query-application/src/sql/session.rs (SessionSqlState)"
  - "novarocks/frontend-application/src/query/statement_action.rs (execute_observed_statement_action)"
  - "novarocks/frontend-application/src/server.rs (Trino listener composition and readiness)"
  - "novarocks/sql/src/compiler/root_render_type.rs (client_render_schema_from_port)"
  - "novarocks/result-contract/src/render_schema.rs (ClientPresentationFamily, TrinoJsonFacts)"
  - "novarocks/result-render/src/trino_json.rs (ArrowTrinoJsonEncoder)"
  - "novarocks/trino-adapter/src/driver.rs (static columns and command projection)"
  - "novarocks/trino-adapter/src/registry.rs (retained pages and terminal settlement)"
  - "novarocks/trino-adapter/src/request.rs (QueryTextLimit, ParsedStatementRequest)"
  - "novarocks/trino-adapter/src/error_mapping.rs (typed client errors)"
---

## 问题

只保留一种客户端协议时，怎样让查询、命令完成结果与取消保持相同的应用语义，并让协议形状、逻辑计数、送达确认与实际资源退出各有明确作者？

## 背景与执行事实

原双入口把MySQL连接身份、writer终结和Trino无连接查询同时组合进FE；同一领域完成值因载体不同而出现不同列/计数形状。若以连接位置代替语句身份、从命令是否有计数反推输出列，或把最后的数据页当作客户端已确认，会使权限、JDBC/CLI对命令的识别以及资源保留互相影响。

| 实体 | 唯一作者与职责 | 不可替代的事实 |
|---|---|---|
| 语句静态输出形状 | Query Application从原AST产生StatementAdmission | 普通CREATE与CTAS同为CREATE TABLE类别，但前者空列、后者rows bigint；不能只看StatementKind或执行后的值 |
| 有无计数与计数值 | 原写入/刷新/统计领域产生StatementCommandCompletion | 真零、真实非零、没有计数是三个不同事实；物理writer/profile/Task退出数不代替逻辑计数 |
| 客户端完成投影 | Trino适配器消费上述原类型事实 | columns、data、updateType/updateCount来自已冻结事实，不从SQL前缀、错误文本或HTTP状态猜测 |
| 原Root、Source与工作退出 | Query Application与各业务/Native owner | Ready、成功seal、HTTP响应返回和取消请求都不证明原工作已退出 |
| 根结果编码与页确认 | 原BE根编码器；FE保留不可变页与token | 原Native根段ACK不证明HTTP页送达；HTTP写出成功也不等于语句complete |

客户端呈现仅为显式TrinoJson及原TrinoJsonFacts。Internal请求可以没有客户端族；Scalar、CountOnly与typed Internal artifact保留各自的准确领域，不转成HTTP JSON。ClientRows缺呈现事实、退役wire词汇或不支持的原生类型沿原typed作者拒绝，不选择缺省族或近似类型。

本条固化长期owner边界，不记录某个局部PASS作为全链验证。旧ADR的执行事实、既有失败和后续未运行范围仍由其原记录保留。

## 考虑过的选项

1. **保留仅供System使用的MySQL listener或兼容client facade：设计否决。** 可以少改旧actor，但测试继续经过产品不再提供的连接/编码/终结路径，必须永久维护两套session与容量。替代性质应沿真实Trino/Native owner保护。
2. **全部命令只返回updateType，缺计数就省略rows列或填0：设计否决。** 响应更短，却使静态结果形状随运行结果变化，混淆未知与零；与Trino客户端按columns/updateType判别完成结果的模型不符。
3. **让Trino adapter从SQL文本、物理writer数或profile推导计数：设计否决。** 减少跨层类型改动，却把逻辑业务归属交给载体或退出时序，破坏ADR-0208的独立逻辑计数合同。
4. **单一Trino入口，保留三个作者和原分页结算：采用。** 客户端、测试和依赖只维护一条路径；命令也使用原治理结果图与不可变分页。代价是计数命令仍多一次终页请求，并须为受影响的共享wire/descriptor重做对应验证。
5. **将所有Internal结果也统一成JSON或另开计数结果窗口：设计否决。** 会引入无客户端事实的呈现和第二容量来源；没有必要改变Scalar/CountOnly/typed artifact，计数图沿原statement/window保管即可。
6. **放开远程HTTP、客户端TLS、多用户管理SQL或spooling：待评估。** 这些需要自己的身份、授权、送达和退出合同，删除一个协议不构成扩大访问面的授权。

## 裁决

1. **唯一客户端入口。** FE仅组合Trino HTTP statement入口；旧MySQL listener、connection registry/token控制port、专属encoder/呈现、容量advertisement、feature与同步client依赖在无真实消费者后退出。旧mysql_port等配置明确拒绝，不静默忽略或转换。语法层的MySQL兼容、SQL类型/算术/CAST/时间语义不由本条删除；外部MySQL StateStore、mysql_async及其fixture/测试保留，外部StarRocks比较端按原owner保留。

2. **身份、授权与生命周期不迁给socket。** 客户端查询ID与内部attempt身份仍由原应用作者分配，语句会话以不复用的中立session key保管；HTTP连接不是查询身份，也不是容量票据。依赖已退役连接身份的KILL、KILL QUERY、KILL CONNECTION、SHOW PROCESSLIST在原准入阶段typed拒绝，不伪造ID、不返回空成功。此拒绝不提前实现新的按query-id管理SQL。现有准确continuation DELETE、服务drain/取消、当前principal/Host校验和有限测试授权继续通过原owner；不引入管理员豁免、隐藏控制listener或通用观测授权。

3. **错误沿原类型投影。** SQL作者保留稳定code与有来源的span；已知执行/Connector/资源/控制/Representation事实保留原typed cause和外部效果。Trino边界按登记映射输出errorCode/errorName/errorType/location，不以message匹配分类。未知来源不能借客户端迁移变成已知失败，success seal后的取消或送达失败也不能抹去已发生的提交。ADR-0095的Analyze规则及ADR-0187已有部分替代关系继续有效；只退出旧MySQL ErrorKind/errno/SQLSTATE映射。

4. **完成结果固定三个作者。**
   - Query Application在准入时从原AST冻结静态形状：普通结果集；无输出列的命令；计数类rows bigint；CALL只有原业务路由选择具体载体后才确定，不能猜测。CTAS与普通CREATE TABLE不可因updateType相同而共用形状。尚未实现的语法/业务不因表中列出形状就视为已开放。
   - 领域返回Empty、Rows(Counted(n))或Rows(NoCount)。普通写/MOR、COW原selection、最终CREATE等分别沿原作者给出真实值；没有真实计数时rows那一行值为NULL，不填0。u64→bigint显式检查范围，超界沿原typed错误拒绝；adapter不修正、截断或重数。ADR-0208关于逻辑贡献、覆盖、严格消费及提交的规则不被本条替代。
   - Trino适配器仅投影：成功完成路径从语句已准入后的首个响应起，各完成结果页携带同一updateType；空命令提供columns: []，计数类提供唯一rows bigint列和一行数据。真实计数的updateCount从承载该行的数据页起保持到成功终页；NoCount行仍为NULL、全程不出现updateCount。错误终页清除完成投影，保留原typed错误。相同token重放同一份字节。POST尚未执行/未得到原AST准入事实时不伪造类别。

5. **计数页也服从原页确认。** 计数行在原statement的Local结果图/窗口内，经同一有界Trino编码器生成；不另建计数钱包或改写writer产物。包含该行的数据页仍带nextUri。请求后继token确认这一页，然后生成无data/无nextUri的终页并结算原语句。空列、无数据命令可沿原协议直接结束；不强行添加一个虚构数据页。客户端共享库以updateType区分完成结果与结果集，SQL runner只为结果集产生golden块，不能因为计数命令也有columns而改写历史golden。

6. **期限与实际退出承接原owner。** producer等待只观察同一原查询deadline、取消与失败；HTTP adapter不得续期、重放时重新准入或以写出时钟判定query timeout。Trino单response body写出期限与客户端放弃期限仍沿ADR-0203，各自的30秒/放弃时间不等同于已退出的MySQL累计writer预算或绝对Closing。取消/响应Drop必须由原稳定Source保管实际future、输入/结果别名、权限与Join；Ready、页封存、单次HTTP完成均不能提前退款。Root、窗口、工作与transport资源分别以原实际持有者退出为准。

7. **大小限制沿固定上游语义。** `[standalone_server].trino_query_max_length`采用Trino483的query.max-length默认1,000,000，按 Java 字符串长度等价的 UTF-16 code units 计数：BMP为一单位，补充平面字符为两单位。超限在执行准入前返回QUERY_TEXT_TOO_LARGE（既有映射code35/USER_ERROR），不启动语句工作。读取HTTP body使用同一限制推导的有界UTF8字节上界：合法UTF8每UTF16单位最多三字节，所以3L仅是读取保管上界，最终仍以UTF16单位精确裁决，不把字节数当语句语义长度。本条不创设新的硬阈值，也不恢复MySQL command_bytes。

8. **冻结与兼容不靠旧名字缺省。** sole Trino family与呈现词汇由原FE/BE codec/descriptor作者同步；退役tag/name保留reserved，缺族、退役枚举和未知值明确拒绝。客户端呈现变化不改变Internal结果域，也不以旧fixture/旧binary receipt证明新compatibility岛。规范构建、实际程序来源、原生行为、运行/清理和准入资格分别核验。

## 接受的妥协（诚实记录）

**计数命令比旧MySQL OK多一次终页往返。** 即使逻辑计算与publication已经完成，承载count的数据页仍有nextUri，客户端还须请求终页确认最后一页；原query许可与窗口因此继续保留。这是ADR-0203“最后数据页仍带nextUri”的直接延伸，以额外一次RTT换取不可变重放和明确送达证据。不读到底的客户端占用到原放弃期限；不以“计数只有一行”为由提前complete或设置特例。

**没有真实计数仍返回NULL行。** 这保证静态形状与 Trino 客户端模型一致，却诚实暴露领域尚未提供计数的边界。NULL不代表零，也不由profile/writer物理计数补齐；未来由对应业务作者在原完成值内补齐，不能改静态形状。

**唯一入口减少协议分支，但仍保留多个实际退出owner。** Native根段、保留HTTP页、本地结果图、业务Source与残余Work的结束时刻可以不同。用一个done位统一它们更容易写，却不能证明归还原资源；本条接受这些独立事实与额外终页请求，不宣称所有外部/private矩阵已验证。

**本机安全范围没有因退役扩大。** 保留现有loopback/Host与配置用户名规则；本条不提供客户端TLS、远程listener、跨用户管理权限或spooling。Native JWT/TLS也不会因此保护HTTP入口。这些限制是未交付能力，不是删除MySQL后自动满足的安全结论。

上游长度规则的固定来源：[QueryManagerConfig](https://github.com/trinodb/trino/blob/50b0b50b75abd47f830b7805ee1b51716eb4065e/core/trino-main/src/main/java/io/trino/execution/QueryManagerConfig.java)、[DispatchManager](https://github.com/trinodb/trino/blob/50b0b50b75abd47f830b7805ee1b51716eb4065e/core/trino-main/src/main/java/io/trino/dispatcher/DispatchManager.java)。逻辑计数规则见 [ADR-0208](ADR-0208-logical-write-counts-and-strict-stream-delivery.md)，分页确认规则见 [ADR-0203](ADR-0203-client-result-presentation-and-trino-paged-delivery.md)。

## 何时重新评估

- 需要远程入口、TLS、多用户认证/管理员控制时，先定义client identity、URI/Host、认证授权与原query owner的目标规则；不能只放宽bind或恢复connection-id旁路。
- 引入spooling或新交付协议时，重新定义不可变结果、送达确认、凭据、保留与实际退出；是否省去终页RTT必须由新的可验证送达合同决定，不能在现分页模型上偷省。
- 某类领域补齐逻辑计数时，保留静态shape与原逻辑贡献/提交规则，只扩展该领域的Counted事实和对应验证；若新增语法形状，先由Query Application/SQL作者与上游目标版本冻结。
- 升级上游Trino基准或改变statement text限制时，核对UTF16语义、默认值、配置范围与HTTP读取几何，不沿迁移便利自定额度。
- 结果窗口确实无法容纳合法单行时，由原资源owner评估窗口或交付模式；不增加adapter私有预算或把Internal强制JSON化。
