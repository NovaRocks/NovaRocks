# Iceberg 删除适用性夹具

此目录由 Java Iceberg 1.11.0 生成真实 Parquet、Puffin、manifest 与 metadata，
为 NovaRocks 的删除闭包提供独立 oracle。`run.sh` 复用仓库 Spark shell，
不启动服务、不更改共享 Docker 配置、不修改 NovaRocks 产品代码。

```bash
NOVA_ENV_REST_ENV_FILE=/absolute/path/to/resolved/env.sh \
  tests/sql/fixtures/iceberg-delete-applicability/run.sh \
  --mode anomalies --output reports/uea4g/<unique-run>
```

环境入口优先使用调用者传入的 `NOVA_ENV_REST_ENV_FILE`，否则解析工作树的
`docker/iceberg-rest/runtime/current/env.sh`。脚本先固定其真实路径，再透传给
`spark-shell.sh`；隔离 suite 的入口不会被 `runtime/current` 覆盖。环境须先由
既有 fixture owner 准备。每次默认生成一个 UUID namespace；也可显式传入
`--namespace`、`--prefix`，但该名称不得已存在。

`--mode positive` 仅运行同次 `RowDelta.addRows + addDeletes` 的 position、
equality、DV 正向控制，供 `spark_rest_delete_applicability.sql` 使用。
数据文件和删除文件来自官方 writer，真实 manifest data sequence 必须表明：
position/DV 在相等序号下生效，equality 删除旧文件的 key 并保留同提交的新 key。
SQL 黄金行由固定输入手算，不使用 NovaRocks record 模式生成。

`--mode corpus` 运行更多官方 writer 的合法组合：position/equality、DV/equality、
共享 position、同容器多 DV blob、升级保留 legacy position、累积 DV 替换、
保留 data sequence 的 rewrite、global/partition 字段组，以及不可变 Puffin 的
端点替换。端点 oracle 按实际可见行袋分别核对 removed/added，保留重复行重数。
多 row-group position 夹具另存物理 footer 布局，区分 full、缺失与长前缀统计。

`--mode promotion` 是合法类型提升的诊断模式，分别比较无分区与 identity(p)
表在 p INT→LONG 前后的历史 snapshot 投影及显式当前 schema 投影。它保存物理
Parquet schema、table/scan schema、partition 常量 Java 类及实际行 Java 类。
独立预期不一致与 reader 异常分开；此模式只输出 `UEA4G_OBSERVATION_OK`，
不能用作 fixture 或产品通过声明。

`--mode anomalies` 先运行正向控制，再运行低层异常矩阵。异常用真实 writer 的
不可变内容和 Avro schema 构造独立 metadata；保留同 manifest 重复、跨 manifest
重复、manifest-list 重复引用，以及同地址异 sequence/count/scope 的原始重数。
position 和 DV 分别测试跨 partition/spec；position 区分 bounds 推导路径与
显式 referenced path；DV 另测精确目标 X<D、多 DV 同目标及引用变化。
异常矩阵不预填全拒绝，Java 实际成功或失败都是观察结果。

每轮输出：

- `fixture.scala`、`spark.log`：准确执行输入和原始输出。
- `receipt.json`：运行版本、每例构造路线、原始 entries/list、完整 Java
  `planFiles().deletes()` 列表及重数、真实 `IcebergGenerics` 行读取结果或异常阶段。
  独立行袋比较单独存储在 `independent_oracle_matched`，不将不匹配伪装成读取异常。
- `artifacts/`：从实际对象读取并校验 SHA-256 的 metadata、manifest、Parquet、
  Puffin。原始对象路径与本地文件的映射保留在 receipt 中。
- `registrations`：对低层异常 metadata 实际调用 REST `registerTable` 的能力收据。
  客户端 API 存在不等于服务端注册成功。

`fixture-generation` 与 Java planning、Java row read 相互独立：writer 拒绝不能
冒充 reader 拒绝，planning 成功不能冒充内容读取成功。脚本要求准确完成 marker、
非零且完整的 case 集合、正控制的独立行袋和真实序号关系、完整的 artifact 哈希；
Scala 编译失败即使 spark-shell 返回 0，也会使脚本失败。

异常表的输入不是合规表的预期；产品策略应按批准的 spec 消费 Java 实测。
Java 与 NovaRocks 采用相同统计策略时逐文件闭包完全相等；显式省略安全 equality
统计的策略差集必须另外给出独立排除见证，不能以任意超集或相同行数通过。

对象均位于本轮创建的表位置。实验默认保留它们以便重放和 native 验证，脚本不
清理共享 namespace。验收 owner 完成消费后仅删除 receipt 指定的本轮表与对象；
注册的异常表与源表共享内容，必须在全部读者退出后统一清理。
