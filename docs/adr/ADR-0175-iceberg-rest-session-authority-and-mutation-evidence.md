---
id: ADR-0175
title: "Iceberg REST keeps one upstream session per generation and turns each NovaRocks-issued mutation into attributable evidence"
domain: [provider-spi, distributed-query-lifecycle, crate-boundary]
status: active
supersedes: []
superseded-by: null
partially-superseded-by:
  - id: ADR-0224
    scope: "Ruling 23 retention of requested property values and the definite-rejection set in ruling 16; authentication, routing, complete JSON validation, attribution and redaction remain active"
    link: "ADR-0224-mv-document-capacity-comes-from-real-boundaries.md"
  - id: ADR-0179
    scope: "SDK session and generic mutation facade ownership, caller deadline after dispatch and FE staged-result projection; authentication, header provenance, frozen routing and redaction rules remain active"
    link: "ADR-0179-iceberg-publication-lifecycle-and-evidence.md"
  - id: ADR-0176
    scope: "Compromise that the REST-only core patches (staged-create initialization updates, AddSchema.last_column_id, view API) remain and that stage and view requests use their types: the patches are deleted and the Connector owns that vocabulary"
    link: "ADR-0176-iceberg-rest-request-vocabulary-and-supported-server-floor.md"
partially-supersedes:
  - id: ADR-0151
    scope: "Meaning of the announced acquisition paths: both now carry the frozen public routing context (complete address and public routing headers); the execution node never opens its own catalog session; a vended read is admitted only when execution-node first acquisition can be formed. Consumer-driven renewal, role-local material and the ratio windows remain effective"
  - id: ADR-0174
    scope: "REST crate patch inventory (U1r, R1-R3, R5-R8, R10) and its deletion; ruling 4 temporary REST outcome narrowing; the compromise that authentication uses the SDK session and merged properties. Baseline reproducibility, consumer-owned deletion of the core patches and the remaining rulings stay effective"
date: 2026-10-14
provenance:
  - "discussion: 2026-10-12 REST mutation evidence mapping, session authority, routing context and execution-node first acquisition review"
  - "discussion: 2026-10-13 ruling to extend the vendored upstream REST client instead of replacing it; streaming minimal views vs read-then-decode complete documents"
  - "discussion: 2026-10-14 drop success also accepts a 200 with an empty body"
  - "PR: (回填)"
code-anchors:
  - "novarocks/connector/iceberg/src/catalog_runtime.rs (assemble_rest_generation)"
  - "novarocks/connector/iceberg/src/catalog_config.rs (REST_SERVER_ROUTING_HEADERS_PROPERTY, rest_persistent_definition)"
  - "novarocks/connector/iceberg/src/rest_protocol/auth.rs (RestAuthOwner, PersistentRestConfig)"
  - "novarocks/connector/iceberg/src/rest_protocol/binding.rs (SessionBinding, is_authentication_header, is_protocol_header)"
  - "novarocks/connector/iceberg/src/rest_protocol/attempt.rs (AttemptCell, ensure_session)"
  - "novarocks/connector/iceberg/src/rest_protocol/boundary.rs (sanitize_upstream_error)"
  - "novarocks/connector/iceberg/src/rest_protocol/http.rs (RestHttpClient, classify_send_error)"
  - "novarocks/connector/iceberg/src/rest_protocol/evidence.rs (classify, MutationEndpoint, AttributionExpectation)"
  - "novarocks/connector/iceberg/src/rest_protocol/mutation.rs (read_view, read_empty)"
  - "novarocks/connector/iceberg/src/rest_protocol/json.rs (JsonScanner)"
  - "novarocks/connector/iceberg/src/rest_protocol/endpoints.rs (RestEndpoints, acquire_routed)"
  - "novarocks/connector/iceberg/src/commit/attempt/observation.rs (PublicationJournal)"
  - "novarocks/connector/iceberg/src/rest_protocol/routing.rs (route_for_lease, preflight_route)"
  - "novarocks/connector/iceberg/src/metadata_context.rs (vended_renewal_path)"
  - "novarocks/connector/iceberg/src/execution_authority.rs (ExecutionNodeCredentialsEndpointRefresher)"
  - "novarocks/connector/iceberg/src/catalog/error.rs (CatalogOutcome, preparation_failure_kind)"
  - "novarocks/spi/src/connector/credential_lease.rs (CredentialRoutingContext, CredentialRenewalPath)"
---

## 问题

访问 Iceberg REST catalog 时，会话、认证与路由由谁唯一决定；一次变更请求的 HTTP 结果在什么条件下才能当作「已生效」或「确定未生效」的证据？

## 背景与执行事实

> **当前适用范围：** [ADR-0179](ADR-0179-iceberg-publication-lifecycle-and-evidence.md) 已替代上游会话建立、generic mutation facade、裁决 21 的错误回推，以及裁决 24 中调用方取消/deadline直接结束证据等待的规则。Provider 自有会话冻结一份成功 binding；外发后使用独立证据期限，journal 先固化结果再收尾。下文“直接路径迁移之前”与“CTAS stage Unknown 在 FE 报确定未提交”是历史妥协，已经退出；FE 现在投影同一 Provider 事实。认证、header 来源、冻结路由、状态码分类与脱敏约束继续有效。

REST crate 来自上游 iceberg-rust，留在 vendor 中且不带 NovaRocks 补丁（vendor 政策见 [ADR-0170](ADR-0170-third-party-crates-are-bounded-by-public-configuration-not-forked.md) 与 [ADR-0174](ADR-0174-vendored-iceberg-baseline-and-patch-policy.md)）。上游客户端（`RestCatalog`）在首次操作时请求 `/v1/config`，按 defaults < 用户配置 < warehouse < overrides 合并配置，`overrides.uri` 替换 uri，合并结果还决定 prefix；成功后在客户端内缓存会话。NovaRocks 不替换这个客户端，而是经它公开的两个扩展点接入：`with_auth_manager` 注入认证 owner，`with_client` 注入 HTTP 客户端。需要 NovaRocks 自身语义的请求，用从这份会话取得的绑定由 NovaRocks 自己发出。

**请求分工。**

| 经上游（先经过建立入口） | 由 NovaRocks 用会话绑定发出 |
|---|---|
| `/v1/config` 握手与配置合并（令牌由认证 owner 换取） | 全部变更请求：建删 namespace、建表、stage、表提交（含 CTAS 最终提交）、删表、建/替换/删 view |
| 会话探针（请求被认证否决，不发出） | 单页列举 namespace、表、view |
| namespace 的存在性 | 委派模式的加载表；两种模式下提交前的基线重读 |
| 静态模式下表的加载 | 两种模式下表的存在性：声明了 HEAD 用 HEAD，否则用不带委派头的 GET，只看状态码 |
| 过渡适配器中的上述几类读取 | 凭证端点；view 的读取与变更；BE 的取得（不经上游，BE 没有上游客户端） |

**实体与各自扛什么。**

| 实体 | 拥有 | 不拥有 |
|---|---|---|
| 上游 `RestCatalog`（每个 FE generation 一个） | 握手、配置合并、会话缓存、左列的读请求 | 变更与证据；认证材料来源 |
| `RestAuthOwner`（FE、BE 各一，同一实现） | 本角色身份、令牌换取与缓存、独占写 `Authorization`。FE 的同时以 `RestAuthManager` 注入上游：`init_session` 给握手会话，`catalog_session` 校验合并配置并取得绑定，`authenticate` 写令牌或否决探针 | 路由 |
| `SessionBinding` | 生效 uri、prefix、按来源区分的 `header.*`（持久定义 / 服务端下发）、HEAD 端点声明；地址拼接 | 任何表状态与认证材料 |
| `SessionGate` = `AttemptCell` | 一次建立尝试的共享结果 | 重试、失败缓存 |
| `RestHttpClient` | 不跟随重定向、关闭 reqwest 重试、只用 HTTP/1.1；一次写出并记录派发阶段 | 解释状态码 |
| `evidence::classify` | 观察 + 最小视图 + 归属期望 → 证据与失败类别（纯函数） | I/O |
| `JsonScanner` + 最小视图 sink | 分段校验整个 JSON，只保留视图要的字段，保留量有显式上限 | 证据判断 |
| `RestTransitionalAdapter` | 为尚未迁移到 owner 的直接路径实现 `iceberg::Catalog`；变更错误附带类型化证据 | 直接路径自己的分类器 |
| `routing::route_for_lease` | 从绑定冻结 BE 的路由上下文；形不成时在派发前拒绝 | 认证材料 |
| `CredentialRoutingContext`（SPI） | 跨 FE/BE 的公开路由事实：完整地址 + 公开路由请求头 | 令牌、secret、principal |

**容易踩的事实。**

- **上游的会话缓存只共享成功。** 它基于 `OnceCell::get_or_try_init`：初始化失败之后，等待中的调用会一个接一个地重新初始化。一个冷 generation 上 N 个并发调用遇到 `/v1/config` 超时 T，最后一个要等约 N×T（32 个调用、30 秒超时约 16 分钟）。
- **注入 `AuthManager` 之后，上游不再读 `rest.auth.type`。** 上游在认证之后以替换方式合并 `header.*`，`header.authorization` 会覆盖令牌；上游的 OAuth2 实现还会采用服务端经 `/v1/config` 下发的 `token`。这两条优先级是客户端库的附带结果：Iceberg Java 1.9 之前令牌优先、之后 `header.*` 优先，转换时没有讨论优先级。
- **两份各自初始化的会话可能落在不同的 catalog。** 服务端在两次 `/v1/config` 之间改 overrides，读和写就分到不同的 prefix 甚至服务。表 UUID 检查发现不了：同一份 metadata 可以注册到两个 catalog。
- **默认 HTTP 行为会改变请求含义。** reqwest 默认最多跟随 10 次重定向；301/302 把 POST 改成 GET，303 一律改 GET，307/308 把同一请求体（包括令牌请求的 client secret）发往新地址。最终收到的 200 不一定是对这次提交的回应。
- **Java 参考服务端先发布、后重读。** 提交、建表、建 namespace、建 view 与 view 提交都在发布之后重读；重读时表被并发改名或删除，客户端收到 404（校验失败则 400），而变更已经生效。
- **上游把非 2xx 的响应体与服务端消息放进错误上下文**；`disable-header-redaction` 还能由服务端下发，令错误带出响应头的值。上游的公开 wire 类型（例如 `LoadTableResult`）派生 `Debug`，会打印凭证配置。
- **上游没有公开的「只建立会话」或「查询端点声明」接口**（`supports_endpoint` 是 crate 私有），但它在发送之前调用 `AuthSession::authenticate`，而 `HttpRequest` 公开方法与地址——会话探针只依赖这两条公开契约。
- **「未写出」可以可靠判定的前提**：reqwest 的连接错误只来自连接器与取连接阶段（DNS、TCP、TLS、代理隧道、连接超时）；hyper 只在一个字节都没写出时重试失效的复用连接；限定 HTTP/1.1 后没有 HTTP/2 的协议级拒绝与重试。reqwest 的读取超时从请求开始计时、包含建连，所以连接超时必须短于读取超时，否则卡住的握手会报成读取超时——`RestHttpClient::with_timeouts` 拒绝这种配置。
- **删除的成功状态在实现之间不一致。** REST 规范为删表、删 namespace、删 view 只定义 204；Iceberg Java 参考实现在 1.11.0 之前返回 200 加空响应体；网关可能返回 `200 text/html`。`PURGE` 删表的成功会触发提交后的清理请求。
- **BE 从不带种子。** 分布式执行中 BE 第一次读数据文件就要自己取得凭证；租约不带取得路径时第一次读取直接失败（[ADR-0151](ADR-0151-credential-renewal-is-driven-by-the-consumer.md)）。

**正例锚点。** `AttemptCell` 是「同一尝试的参与者共享成功与失败、失败不缓存」的唯一实现，令牌换取与会话建立共用它；`evidence::classify` 是不做 I/O 的纯函数，映射表可以不经网络穷举测试。改动这两类逻辑前先读它们。

## 考虑过的选项

**会话与客户端**

- **A. NovaRocks 实现完整 REST 客户端，上游 REST crate 退出。成本否决。** 能完全掌控每个请求，但要替换基础设施，失去上游在握手、配置合并与读请求上的后续修复，并长期自担与规范和参考实现的对齐。上游扩展点失效时回头（见「何时重新评估」）。
- **B. 把 REST crate 迁出 vendor 自有化后改造。设计否决。** 分叉之后无法再跟随上游，与 vendor 只删不增的政策冲突。
- **C. 保留移植的 REST 补丁，在其上加证据。设计否决。** 补丁中的状态映射与整块缓冲正是证据不可靠与响应无界的来源；vendor 不再新增改动。
- **D. 只接管提交，提交另建会话。设计否决。** 提交与其余请求各有一份会话，正是上文「两份会话」的错位。
- **E. NovaRocks 自己请求 `/v1/config` 取得绑定。设计否决。** 形成第二份会话。
- **F. 用一次真实的目录请求触发会话建立。成本否决。** 可行，但每个 generation 多一次目录请求，还要挑一个所有服务端都接受的请求；认证否决的探针不产生目录请求。
- **G. 读取上游私有状态（字段、`Debug` 输出）得到绑定或端点集合。设计否决。** 不是稳定接口。
- **H. 依赖上游 `OnceCell` 串起并发的会话建立。设计否决。** 它只共享成功（上文）。
- **I. 全部读请求也由 NovaRocks 发出。成本否决。** 能统一流式读取与错误形状，但 namespace 存在性与静态加载在上游行为正确，留在上游才能继续获得修复；代价是它们整块读入、错误要在边界脱敏。
- **J. 缓存建立失败一段时间，或失败后自动重试。待评估。** 与「同一批调用共享一次尝试」是不同的问题，本条不裁决。

**认证与路由**

- **K. 复用上游 `OAuth2Manager` 作为 FE 认证。设计否决。** 它会采用服务端下发的 `token`；BE 没有上游会话、用不了它；FE 与 BE 应当是同一份实现。
- **L. 保留 `rest.auth.type` / `header.authorization` 作为认证入口。设计否决。** 注入认证 owner 后 `rest.auth.type` 不再起作用，保留只会误导；`header.*` 随 catalog 定义投影到 BE，会让 FE 与 BE 共用一个身份，并让授权身份键中记录的 principal 与远端实际看到的身份分离。
- **M. 服务端下发的 `token` 替换角色令牌（上游与 Java 的行为）。设计否决。** 认证只来自角色本地身份。
- **N. BE 继续自己请求 `/v1/config`，再与公告比对。设计否决。** BE 仍是第二份会话，只是把错位从静默变成报错。
- **O. 不公告 BE 的取得路径。设计否决。** BE 从不带种子，等于让它首读失败。
- **P. 所有 `header.*` 自动视为公开交给 BE（业界整包下发）。设计否决。** 服务端下发的私有认证性质请求头会被静默带到 BE。
- **Q. 只接受持久定义中的请求头，服务端下发的一律拒绝。设计否决。** 等于放弃服务端路由头这一能力；显式声明的代价只是一项配置。
- **R. 按名称判断服务端下发的请求头是否秘密。设计否决。** 名称不可靠；名称检查只用于对认证类名称快速失败。

**证据与资源**

- **S. 400、404 授权清理（Java 默认 `CleanableFailure`）。设计否决。** 参考服务端会在发布之后返回它们。
- **T. 证据规则只用于表提交，DDL 沿用旧映射。设计否决。** 建表、建 namespace、view 提交同样会在发布之后重读；CTAS 的 saga 也把派发之后的 stage 视为可能已有副作用。
- **U. 读到所需字段就判定已生效。设计否决。** 截断或混入的内容会被当成成功。
- **V. 为提交回应设字节上限，或为 REST 请求设进程级信号量。设计否决。** 确认的保留量已与快照数无关，上限只会把合法的大 metadata 变成结果未知；不另设连接器专属额度（ADR-0156）。
- **W. 完整文档也边读边解。成本否决。** 依赖图中的 JSON 解析器（`serde_json`、`sonic-rs`）都是拉取式，只能在完整输入或可阻塞的 `Read` 上工作；内存归属只作用于当前线程（ADR-0167），在阻塞线程上边读边解会失去调用方的归属，并让每个在途读取占一个阻塞线程。
- **X. 删除只接受 204。成本否决。** 符合规范，但 Java 1.11 之前的服务端返回 200 加空响应体，会把每次删除都报成结果未知。
- **Y. 删除接受任何 200（上游行为）。设计否决。** 网关返回的 `200 text/html` 会被当成删除成功，`PURGE` 随即触发清理。
- **Z. 读请求与凭证续期改用更精确的错误类别（401、419 判为拒绝）。设计否决。** 续期会在令牌过期时关闭授权身份、丢弃仍有效的存储凭证。
- **AA. 过渡适配器在错误类型中编码全部证据。设计否决。** 现有错误类型区分不了 401、403 与冲突；直接路径的分类器会从 `TableNotFound`、`DataInvalid` 等推出错误的「确定未提交」。
- **AB. 直接路径的已有表提交没有冻结 UUID 时跳过 UUID 检查，或在适配器里重新加载表补上。设计否决。** 前者会把提交归属到被删除重建、恰好同 schema 与 spec 的另一张表；后者重新加载到的可能已是另一张表。
- **AC. 发送 `Idempotency-Key`、令牌在过期前刷新。待评估。** 前者需要单独设计结果未知之后的重发语义（规范规定 5xx 不定案）；后者不比既有行为差，本条不裁决。

## 裁决

上游客户端负责会话与列明的读请求；NovaRocks 在它之上扩展认证、绑定、派发、证据与 BE 取得。固化为以下规则。

### 开发规则

**会话与请求分工**

1. **一个 generation 一份会话。** 一个 FE catalog generation 只有一个上游客户端，REST owner、过渡适配器、FE 续期与全部 NovaRocks 请求都从它派生。NovaRocks 从不自己请求 `/v1/config`；绑定在 `catalog_session` 返回成功后于 generation 内不变；catalog 定义变化才随 generation 重建。
2. **经上游的调用也先过建立入口。** 任何需要会话的调用（包括经上游的读）先调用 `ensure_session`；上游客户端只在会话已经经入口建立之后才交给调用方（`RestEndpoints::upstream`），上游自己的初始化不会被别的路径触发。
3. **建立尝试共享整个结果。** 同一时刻至多一次尝试，期间到达的调用都是它的参与者，成功得到同一份绑定、失败得到同一个失败步骤；失败不缓存，尝试结束之后到达的调用才发起下一次；不增加重试。参与者共同驱动尝试：发起者被取消时尝试继续，全部参与者放弃时尝试随之放弃。令牌换取遵循同一规则。
4. **探针被否决而不发出。** 建立尝试调用上游对一张随机命名探针表的存在性检查；认证 owner 在 `authenticate` 中按完整地址认出探针，返回私有来源标记的否决错误。收到否决即确认绑定已取得，并从探针请求的方法得知服务端是否声明了表的 HEAD 端点；收到其他错误时以绑定是否已取得为准，HEAD 按未声明处理。
5. **请求分工按上表，不按方便。** 需要单页、委派、证据、流式或 view 语义的请求由 NovaRocks 发出；上游行为正确、不需要这些语义的读留在上游。表的存在性是目录语义，不取决于读数据的能力：不构造 FileIO 与表对象，不发委派头。
6. **地址与协议头与上游一致。** 按上游算法原样拼接 `uri`、`v1`、prefix 与路径段，多级 namespace 以 U+001F 连接，不做额外规范化；`Content-Type`、`X-Client-Version`、`User-Agent` 取上游的值。访问委派头按调用点发送：委派模式的加载、两种模式下的基线重读与 stage、load-table 取得；存在性检查与建表不发送。

**认证**

7. **`Authorization` 只由本角色的认证 owner 写。** FE 用 catalog 定义中 `CatalogControl` 绑定所指的身份，BE 用 `DataCredentialVending` 绑定所指的身份，材料只在角色本地注册表；FE 没有身份时请求不带 `Authorization`。支持 OAuth2 client credentials 与 bearer 两种身份。
8. **认证配置只有一个入口。** 持久定义带 `rest.auth.type`、`token`、`credential`，或 `header.*` 使用认证类名称（Authorization、Proxy-Authorization、Cookie、Set-Cookie，或名称含 token、secret、password、credential、access_key，不分大小写）时，建 catalog 与构造 generation 都失败。名称检查是快速失败的防线，不是「是否公开」的判据。
9. **协议头由客户端写。** `header.*` 使用 Content-Type、Accept、User-Agent、X-Client-Version、X-Iceberg-Access-Delegation 时配置失败；服务端下发的同名项不进入 NovaRocks 的请求，只记入诊断，在经上游的读请求上按上游行为生效。
10. **拒绝服务端下发的认证材料。** 合并配置中出现 `token`、`credential` 或认证类请求头时，`catalog_session` 返回错误，会话建立失败。令牌端点（`oauth2-server-uri`，缺省 `{uri}/v1/oauth/tokens`）、scope（缺省 `catalog`）、audience、resource 只取持久定义；服务端下发的不同值被忽略并记入诊断。令牌换取一次后缓存、不刷新；收到 401 或 419 不换令牌重发。

**路由上下文与准入**

11. **路由上下文是公开事实，不是认证。** 两种取得路径都携带 `CredentialRoutingContext`：完整请求地址 + 公开路由请求头；load-table 路径另带表身份与期望 UUID。公开 = 持久定义的 `header.*`，加上 `iceberg.rest.server-routing-headers` 中显式声明过的服务端下发请求头；名称与值都等于持久定义的才算持久定义，其余都按服务端下发处理。地址须为 http/https、有主机、无用户信息/查询串/片段，且已是 HTTP 客户端会发送的规范形式，不超过 2 KiB；路由请求头至多 16 个，名称与值合计不超过 4 KiB。路由上下文按值参与 BE 授权身份的键；`Debug` 只打印请求头名称。
12. **准入先于派发。** FE 为 BE 形成 vended 租约时，catalog 定义必须带 `DataCredentialVending` 绑定，且路由上下文能完整形成；任一不成立就在派发之前拒绝查询，错误写明原因与修正方法（例如在 `iceberg.rest.server-routing-headers` 中声明某个请求头）。vended catalog 上的 CTAS 在 stage 之前做同样的配置检查（`preflight_route`），配置错误不产生任何副作用。
13. **BE 不开会话。** BE 没有上游客户端，不请求 `/v1/config`，不拼地址：地址与路由头取自路由上下文，协议头由客户端写，`Authorization` 由 BE 的认证 owner 写。load-table 路径只解码表 UUID 与 `storage-credentials`，UUID 不一致即拒绝。请求层失败（连接、状态码、读取、解码）带「进展之前可重试」标记、不关闭授权身份；身份与范围校验失败沿用拒绝语义。

**派发与证据**

14. **每个请求最多写出一次。** 任何请求都不跟随重定向（包括 `/v1/config`、令牌端点与经上游的读）；关闭 reqwest 重试；NovaRocks 不重试；只保留 hyper 对「复用连接在写出前已失效」的重试；只用 HTTP/1.1。
15. **「未写出」只有两种来源。** 调用 HTTP 客户端之前的失败（取得绑定、拼地址、序列化、构造请求），以及 HTTP 客户端返回的连接错误；其余错误都是「已写出、无状态行」。无法确定时按结果未知处理。
16. **证据映射（变更请求）。**

| 观察 | 证据 | 失败类别 |
|---|---|---|
| 未写出：令牌端点以 4xx 拒绝 | 未派发 | `Unauthenticated` |
| 未写出：令牌端点其他失败、`/v1/config` 失败 | 未派发 | `Unavailable` |
| 未写出：合并配置带服务端认证材料 | 未派发 | `Unsupported` |
| 未写出：拼地址、序列化、构造请求失败 | 未派发 | `Internal` |
| 未写出：连接失败 | 未派发 | `Unavailable` |
| 已有表提交没有冻结 UUID（过渡适配器在派发前拒绝） | 未派发 | `Internal` |
| 409 | 被拒绝 | 建类请求 `AlreadyExists`，其余 `Conflict` |
| 401、419 | 被拒绝 | `Unauthenticated` |
| 403 | 被拒绝 | `PermissionDenied` |
| 成功状态且可归属 | 已生效 | — |
| 成功状态但归属失败或文档不完整 | 结果未知 | 诊断：协议异常 / 无法归属 |
| 400、404 | 结果未知 | 诊断：可能发生在发布之后 |
| 其他 4xx、408、429、5xx、非成功的 2xx、任何 3xx | 结果未知 | — |
| 已写出、无状态行 | 结果未知 | 诊断：读取超时 / 连接关闭 |

   能授权清理的拒绝只有 409、401、403、419。不从错误类型或错误文本推断证据。stage 再映射到 CTAS saga 的分支：未派发与 401/403/419 为确定未生效，409 为冲突，可归属的成功为已 stage，其余（含 400、404）为结果未知。
17. **删除的成功。** 删表、删 namespace、删 view 收到 204，或收到**响应体为空的 200**，为已生效；带任何字节的 200 是结果未知（协议异常）。drop 的响应体只计字节、不保留。
18. **归属：成功回应当且仅当以下各项成立才算已生效。** ①是直接回应（不跟随重定向）；②整个响应体是一个完整、合法的 JSON 文档（尾部多余内容、截断、嵌套超限都不算）；③最小视图可解码：表提交与建表要非空 `metadata-location` 与 `metadata.table-uuid`，stage 要求 `metadata-location` 为空，view 要求 `metadata-location` 与 `view-uuid`，建 namespace 要求回应中的 namespace；④身份一致：已有表与 view 等于请求冻结的 UUID，CTAS 等于 stage 冻结的 UUID，建 namespace 等于请求的 namespace，建表、stage 与建 view 跳过；⑤产生快照的提交：本次快照出现在 `snapshots` 中，目标 ref 指向哪里不参与判断（回应可能是刷新后的版本），ref 关系只进诊断；⑥CTAS 另检查 NovaRocks 命名的发布标记与文档属性；⑦只改 metadata 的提交只检查 ①至④。②至④不成立报告协议异常，⑤⑥不成立报告无法归属，二者都是结果未知。视图中用到的字段重复出现是协议异常，不取第一个或最后一个。
19. **已有表提交必须冻结 UUID。** 每个对已有表的提交都以生成 updates 所用的那份 metadata 加入 `assert-table-uuid`；没有它的已有表提交不派发，也不靠重新加载表来补。冻结的 UUID 同时是归属期望；表被删除重建时服务端以 409 拒绝，而不是提交到新表上。
20. **回执与表对象分离。** 经 owner 的提交只交出本次快照与表 UUID，不构造上游 metadata 对象、不物化表对象；返回的 metadata 位置只用于诊断，刷新后的回应中它可能属于之后的版本。
21. **过渡适配器只给两种错误形状。** 收到的 409 取专门类型（提交 `CatalogCommitConflicts`、建表 `TableAlreadyExists`、建 namespace `NamespaceAlreadyExists`）；其余一律是显式构造、附带类型化证据的 `Unexpected`，不经 `?` 从 serde、url 等错误自动转换。共用的结果判定（`proves_uncommitted`、`uncommitted_failure_kind`）先读类型化证据，没有证据时才按错误类型推断（非 REST catalog）。

**响应读取与资源**

22. **非成功状态不读响应体。** 令牌端点的失败也只按状态码分类。
23. **两类解码。** 变更确认、凭证端点与 BE 取得只解码最小视图：`JsonScanner` 逐段校验完整 RFC 8259 语法，只保留视图需要的字段，其余按字节流跳过；保留的每个值、每个集合都有显式上限，保留量与快照数、schema 数无关，不积累原始字节。结果本身就是完整文档的读取（委派加载、基线重读、stage 回应、过渡适配器的成功回应、加载 view、列举页）分段读完后一次解码，原始字节只在解码期间存在，不另设上限。经上游的读按上游行为整块读入。
24. **派发之后的失败一律是结果未知。** 连接中断、读取超时、截断、语法错误、尾部多余内容、嵌套超限、调用方取消或 deadline，对变更请求都是结果未知；对读请求是错误，类别与既有规则相同。每读一段都经过 Tokio 协作预算检查点与 deadline 检查。解码在调用方的异步上下文中进行，沿用调用方的内存归属；不设字节额度与进程级信号量。

**脱敏**

25. **上游边界只留类别与固定文本。** 经上游的每个调用，错误在边界转成只含 `ErrorKind` 与按操作给出的固定文本的错误，不透传上游错误的文本、上下文与来源。
26. **服务端控制的内容不出连接器。** 响应体、响应头的值、服务端错误消息、令牌、credential、principal、请求头的值（包括路由请求头）都不进入错误、日志与 `Debug`；被拒绝时报给用户的是按状态码给出的固定文本。认证 owner、REST owner、绑定、请求、响应体与最小视图都手写 `Debug`，只打印名称、计数与存在性，不打印上游客户端与上游 wire 类型；委派凭证立即转成不可序列化、没有原始映射访问器的封闭类型。

### 调试规则

- **先看派发阶段，再看状态码。** 每个变更结果的诊断带派发阶段、状态码、响应头名称、归属结论（含 ref 与本次快照的关系）以及 NovaRocks 自己持有的目标、UUID 与期望快照；会话建立失败带失败步骤（令牌被拒、令牌不可用、`/v1/config`、服务端配置带认证材料）。
- **结果未知不是「失败」。** 400、404 与归属失败都可能发生在发布之后：先按诊断中的 UUID 与期望快照核查 catalog，再决定是否重做；不要手工清理产物。
- **准入错误点名的请求头要么声明、要么让服务端停止下发。** 不要把它改成持久定义的 `header.*` 来绕过：那会让该值随 catalog 定义固定下来，而服务端下发的值可能变化。
- **BE 首读失败而 FE 准入通过**，说明 BE 的角色本地注册表缺少 `DataCredentialVending` 所引用的身份：FE 无法核对 BE 的注册表。

### 对 ADR-0151 与 ADR-0174 的部分替代

| 被替代的内容 | 本 ADR 的处理 | 仍然有效 |
|---|---|---|
| ADR-0151：公告的取得路径只说「沿哪条路径取」，load-table 路径由执行节点按自己的 catalog 配置拼出 | 两种路径都携带 FE 冻结的公开路由上下文；执行节点不建会话、不请求 `/v1/config`；vended 读取的准入要求能形成 BE 的首次取得 | 消费侧驱动续期、材料不跨进程、一个 principal 一条能力路径一个 authority、按比例的时间窗口、一次操作一个取得窗口、四类取得失败可区分 |
| ADR-0174 裁决 3 中 REST crate 补丁（U1r、R1、R2、R3、R5、R6、R7、R8、R10）的保留与删除条件 | 全部退出，REST crate 与上游发布包逐字一致 | core 补丁的消费方删除责任；基线可重建；其余裁决 |
| ADR-0174 裁决 4 的临时结果收窄（只有收到的冲突证明确定未提交） | 由证据映射取代；REST 结果先读类型化证据 | 「不能授权清理就不清理」 |
| ADR-0174 妥协「认证仍使用 SDK 的 session/merged properties」 | 认证 owner 接管，服务端下发的认证材料使会话建立失败 | — |

## 接受的妥协（诚实记录）

- **真实的参数错误、表不存在也报告为结果未知。** 400、404 不再授权清理，会留下孤儿文件并要求用户核查；这是为了不在发布之后误删，不是因为这些状态在多数情况下有歧义。
- **直接路径在迁到 owner 之前仍保守。** 经过渡适配器的直接路径（分区演进、schema 演进、ref 操作、重写 manifest、过期快照、属性提交等）只能从 409 得出确定结论，401、403、419 与未派发也报告为结果未知；委派模式下它们的成功提交仍报告为结果未知，因为它们需要完整物化 `Table`。DDL 的语句层仍按错误类别推断，类别为 `Unavailable` 或 `Internal` 的未派发在语句层报告为结果未知。这些都比证据映射保守，不影响清理安全，但不是目标形态。
- **CTAS stage 的结果未知在语句层报告为确定未提交。** stage 收到 400、404 等时连接器的证据是结果未知、不授权清理；而 staged create 不产生可见表，FE 语句层沿用既有行为，把 stage 阶段的结果未知报告为确定未提交。两层的结论并不一致，是有意保留的既有语句语义。
- **删除接受空响应体的 200 是对规范的偏离。** 为兼容 Iceberg Java 1.11 之前的服务端；空 200 与「网关吞掉响应体」无法区分，接受了这一小风险。
- **依赖上游扩展点的未成文契约。** 探针与绑定依赖：`AuthManager` 的回调时机与 `catalog_session` 拿到的合并属性、认证先于发送、`HttpRequest` 公开方法与地址、`with_client` 注入的客户端用于上游全部请求、HEAD 端点声明与缺省回退。协议头的取值（`x-client-version: 0.14.1`、`user-agent: iceberg-rs/0.11.0`）是上游私有常量的拷贝。每次升级上游都要逐项复核。探针本身是借用认证否决的手法，不是上游设计的用途。
- **经上游的读整块读入**，保留量与上游行为相同；换来的是继续获得上游的修复。
- **依赖重定向的部署不再可用。** catalog 的 `uri` 与令牌端点必须配置为最终地址；读请求与会话建立遇到 3xx 失败，变更请求为结果未知。
- **认证方式只有 OAuth2 client credentials 与 bearer。** API key、cookie、SigV4 等暂不支持，以后只能作为显式的认证能力接入，不经 `header.*`。令牌在 generation 内不刷新：令牌过期后 FE 的请求失败，直到 generation 重建（catalog 定义变化或进程重启）。
- **服务端下发路由请求头时要多一项配置**（`iceberg.rest.server-routing-headers`）；不声明则分布式 vended 读取在准入时被拒绝。
- **FE 无法核对 BE 的角色本地注册表。** 某个 BE 缺少所引用的身份时，它的第一次读取明确失败，而不是在准入时拒绝。
- **凭证端点的地址来自加载回应，只检查语法，不要求与 catalog 同源**，而 NovaRocks 会把 catalog 令牌发往这个地址。是否要求同源留给凭证与安全的 owner（待评估）。
- **SPI 的通用持久属性检查仍放过 `header.authorization`、`header.x-api-key` 一类键。** 本 ADR 只在 Iceberg REST 中拒绝认证类名称；SPI 的通用规则留给它的 owner（待评估）。
- **core 中只为 REST 服务的补丁（staged create 初始化 updates、`AddSchema.last_column_id`、view API）仍在**，NovaRocks 的 stage 与 view 请求暂时使用它们的类型，退出由 REST 协议 owner 另行完成。

## 何时重新评估

- **上游发布新版本**：逐项复核上面列出的扩展点契约与协议头取值；任何一项失效（例如认证不再先于发送、`catalog_session` 拿不到合并配置），探针与绑定即不可行，回到选项 A 或 F 重新比较。上游提供公开的「建立会话 / 查询端点声明」接口时，用它替换探针。
- **reqwest 或 hyper 升级后无法可靠区分「未写出」**：回到证据映射重新裁决「未派发」的边界。
- **某个受支持的 REST 服务用规范以外的状态码表示确定的拒绝，而用户依赖这类拒绝**：重新评估被拒绝状态集。
- **受支持的服务端全部不早于 Iceberg Java 1.11 的删除语义**：重新评估选项 X，收窄回只接受 204。
- **某个受支持的部署必须经过重定向才能到达 catalog 或令牌端点**，或要求 OAuth2、bearer 之外的认证方式：重新评估重定向规则与认证入口。
- **某个受支持的服务端经 `/v1/config` 下发的路由请求头名称无法预先声明**（例如随请求变化）：重新评估选项 Q 与显式声明规则。
- **出现能在异步上下文中边读边解完整文档、且保持内存归属的解析器**，或内存归属可以跨线程传递：重新评估选项 W。
- **经上游的读出现需要 NovaRocks 语义的需求**（流式、单页、委派），或上游在这些读上的行为与规范偏离：重新评估选项 I。
- **直接路径全部迁到 catalog owner**：删除过渡适配器与它的两种错误形状，DDL 语句层改按证据分支报告，CTAS stage 的语句层结论随之复核。
- **令牌在长生命周期 generation 中过期成为实际问题**，或建立失败的突发重复成为负载问题：评估令牌刷新与选项 J。
- **结果未知的人工核查成本变得不可接受**：评估 `Idempotency-Key` 与重发语义（选项 AC）。
- **凭证与安全 owner 裁决凭证端点的同源要求，或 SPI owner 收紧通用持久属性检查**：同步本 ADR 的对应妥协。
