# FS writer corpus

这些固定输入归 FS 测试 owner 维护，供 `novarocks/fs/tests/it/writer_corpus.rs` 经生产 FS reader 验证完整行值和绝对物理位置。运行不启动 producer、不下载、不重写数据；文件缺失或 SHA 不符直接失败。

```bash
cargo test --locked -p novarocks-fs --test it writer_corpus::
```

| Producer | 数据与来源收据 | 独立预期 |
|---|---|---|
| Spark 3.5.5 / parquet-mr 1.17.1 | `spark/data.parquet`、`manifest.json`、`objects.json`、`READY`、两个建表/写入 SQL | 按物理顺序 id=0..4095，value=3×id；`spark/oracle.json` 保存行数与求和 |
| PyArrow 23.0.1 | `pyarrow/pyarrow-23.0.1.parquet`、`pyarrow-manifest.json` | `physical_ids.txt` 对应物理顺序，null/list/UTC 微秒时间戳/decimal 按固定公式 |
| Trino 483 | `trino/trino-483.parquet`、`trino-manifest.json` 内的 SQL、镜像 digest 和原对象来源 | `physical_ids.txt` 对应物理顺序，category/decimal/list 按固定公式 |
| Flink 1.20.5 | `flink/flink-local-1.20.5.parquet`、`flink-local-manifest.json`、`flink-local.sql` | `physical_ids.txt` 对应真实乱序，category/null/decimal/纳秒时间戳/list 按固定公式 |

四份 Parquet、三个 `physical_ids.txt` 的 SHA-256 继续由现有 Rust 测试固定断言。物理位置 oracle 由独立 PyArrow reader 从原 producer 输出按物理读出顺序提取；不从被测 FS reader 重生成。Rust 测试独立构造各列预期，同时要求每一输出位置等于从零开始的行序号，跨多个 FS batch 验证 4096 行完整覆盖。

本目录从 `tests/benchmarks/uea4a2/fixtures/corpus/` 与 `short_iceberg/` 原字节迁入。各收据和 SQL 保留原始记录，其中的对象位置、端点、generator 名称与历史路径是 producer 来源事实，不表示当前环境健康，也不作为当前运行入口。`original-corpus-notes.md` 是原说明的完整来源副本；其中的旧物料单与测量基线命令只用于解释历史，不是当前维护命令。

Spark 数据也供 system 的短 range 功能输入 owner 使用。在线 fixture 需显式准备并生成本次环境的独占 manifest/READY；此处 frozen receipt 不能代替在线 READY 验证。消费与准备相互独立，验证失败不能重写本目录输入。

FS reader、Parquet 类型/位置合同或这些输入变更时运行本测试；成本为本地四文件读取，无集群和外部服务，沿已有 Rust 测试链路维护。输入版本变化必须来自真实 producer 输出并独立核对 oracle，不能用重录 expected 掩盖结果差异。

当前阶段只维护功能与数据正确性；本目录不承接历史吞吐/RSS采样或比较协议。旧来源receipt保留原字节，产品FS conformance为本地canonical消费者；online短表需由保留的显式producer在本次环境供给，不能覆盖此处历史输入。
