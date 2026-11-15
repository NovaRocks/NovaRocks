# Trino 显式互操作资产

本模块落实 TRC-1 P11。它不进入默认 System 集合，也不以本地比较器测试代替真 Trino、Spark 或 1FE+3BE 的运行。

- `trino-interop/encoding-conformance`：Spark SQL 写普通 Iceberg 类型；同一原 Spark JVM 内的 Iceberg 1.11 GenericRecord 作者补写 Spark 3.5 SQL 无法表达的 TIME、Variant。两端读取同一张表、相同逐列 SQL，保留双方每次 HTTP 头、原始 JSON body、输入 SQL、能力组合和命令退出。时间能力声明与不声明各一次；Variant 的三组能力在双方实际读取能力成立时运行。
- `trino-interop/official-cli`：镜像内 `/usr/bin/trino` 走 host 网络访问原 FE，验证超过 1 MiB 的 96 行结果、`--catalog`/`--schema`、USE 和 `sql_mode` 在下一条语句生效、DDL `updateType`、可从 SQL 构造的 LargeInt/宽 Decimal D13 拒绝，以及对原 CLI PID 的 SIGINT。中断必须同时观察原查询活跃数回落和 `client_cancel` 结算恰好增加一次；原 FE/BE 全量结果资源收束由 System 原有 `await_idle` 另行确认。
- `trino-interop/jdbc-probe`：使用声明的 JDBC 驱动，以只读方式挂载到镜像的 `/probe/trino-jdbc.jar`，通过镜像 Java 的源码模式运行探针。连接启用 `validateConnection=true`，让建立连接与 `isValid` 真正验证 `HEAD /v1/statement`；随后通过 `TrinoConnection.getServerVersion()` 要求 `/v1/info` 返回非空版本，不接受 SQL 回退。成功必须输出 `JDBC_PROBE_OK validated=true server_version=<值>`。

G1a 对签名相同的列逐值比较：JSON 数字用十进制解析，字符串字节不做规范化，浮点 JSON 必须带小数点或指数。map 键按精确字符串比较，array/row/map/Variant 递归检查 JSON 类型。签名相同而值不同不能登记成未来任务。G1b 只接收场景内 `DIFFERENCE_OWNERS` 明确登记的类型差异或拒绝；该表目前仅允许 TRC-6c 的时间类型边界。

参照由 `docker/fixture-inputs/versions.json` 唯一声明。场景通过 `reference_inputs()` 公布所需构件：编码与 CLI 只需要镜像，JDBC 另外需要驱动。System runner 在选中场景之后、任何集群启动之前，按构件并集解析一次；解析结果随场景结算记录的 `reference` 保存，只包含该场景使用的构件。缺失输入由 fixture builder 按声明创建，已有镜像别名直接复用，创建时才核对版本；builder 失败在入口报出，不转为 BLOCKED。

探针只消费入口传入的 `--trino-image` 精确 image ID 与（JDBC）`--trino-jdbc-jar` 宿主路径，不查候选镜像或核对版本。脱离 runner 手工运行时，先以 `docker/fixture-inputs/provision.sh --consumer image:trino --result <file>` 取得引用；驱动按声明的 artifact 取得。缺少引用或驱动直接报错。探针的 Docker 客户端与 Linux host 网络条件继续保留 BLOCKED / 退出 75；实际客户端操作的失败与清理失败保留原始失败。

外部资源在首个副作用前由原生 `resource_registration` 登记。Python 作者保留原 RunBudget、命名空间和 exact container ID；私有原期限 guard 随容器运行。CLI SIGINT 发到作者在 exec 前记录的原 PID，不发给 PID 1 的期限 guard，不猜 JVM PID。清理先移除 Catalog 表，再删除原命名空间的对象前缀并复查为空，最后等待 exact-ID 容器实际消失。未知创建结果不能按可变名称删除。Native teardown 可用原期限重试清理，原失败报告保留为 `prior-*`。

维护 owner 为 Trino 协议/结果呈现模块。相关编码、类型冻结、会话效果或官方客户端接线变化时显式复跑；成本为额外 Trino 镜像、Spark 和 REST fixture，不进常规准入。测试输入和环境由本模块的原生声明及 Cargo runtime-inputs 公布。

运行方式（需先准备原生程序与已有 fixture；selector 由原生 System CLI 消费）：

```text
--only trino-interop/encoding-conformance
--only trino-interop/official-cli
--only trino-interop/jdbc-probe
```

`g1a-comparison.json`、`g1b-differences.json`、`official-cli.json`、`jdbc-probe.json` 是本次派生结果；`pages/`、`command-*/`、`resource-intents.json` 与 `cleanup.json` 是判断实际范围和退出的原始材料。Variant 的适用性结果必须随最终报告逐项保留；JDBC 缺驱动是入口失败，不能记为通过。
