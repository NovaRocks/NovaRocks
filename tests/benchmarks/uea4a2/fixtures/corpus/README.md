# UEA-4A-2 四写入端本地 Parquet 语料

这里保存固定的真实写入结果，供 `novarocks/fs/tests/it/writer_corpus.rs` 通过生产 FS reader 逐行验值和绝对物理位置。测试不启动 Spark、Trino、Flink 或 PyArrow，也不会下载或重写输入。缺文件或哈希不符即失败。

总物料单是 `../manifest.json`；从 `tests/benchmarks/uea4a2/fixtures/` 执行 `python3 verify_manifest.py --verify` 会核验四份原始写入收据、数据文件、绝对位置 oracle、短 Iceberg 表附件及已发布 A4/SSB 基线收据哈希。A4/SSB 的远端快照还需按 `../baselines/README.md` 执行只读在线校验。发布只允许在总物料单尚不存在时显式使用 `--publish`，日常校验只读。

| 写入端 | 固定版本与来源 | 文件 SHA-256 | 行 / 行组 |
|---|---|---|---|
| Spark/parquet-mr | `../short_iceberg/data.parquet`，Spark 3.5.5，parquet-mr 1.17.1；同目录 READY manifest、writer image ID 和建表 SQL | `83f8920d01744bb357f93abc46bfce7d6c5c40c9d3c15d44f2d36392926112e3` | 4096 / 1 |
| PyArrow | `pyarrow/pyarrow-23.0.1.parquet`；`pyarrow-manifest.json`，生成入口 `../writer_corpus.py` | `201538becb093d98fec2be4b3dca7126f2e7907f97d9fed63a571e84fd855f40` | 4096 / 4 |
| Trino | `trino/trino-483.parquet`；`trino-manifest.json` 含 Trino 483 SQL、镜像 digest、原对象路径和 parquet-mr-trino footer | `012f19f5e78f28bc006bcdae895df74fd9a69dda7428cbee05986dd6e0dc0ea8` | 4096 / 1 |
| Flink | `flink/flink-local-1.20.5.parquet`；`flink-local-manifest.json` 与 `flink-local.sql` 含 Flink 1.20.5 本地 filesystem sink、官方发行包与插件校验值、Java 版本和 parquet-mr footer | `55faf3ea8a35561c8a4750c244e25bf1d9ad4a2b301508a5e67f6956a9146851` | 4096 / 1 |

`physical_ids.txt` 是使用独立 PyArrow reader 从各真实文件**按物理读出顺序**提取的每行 ID，一行对应一个零起始文件位置。Trino/Flink 的行顺序不能从 ID 值推测；Flink 开头为 0、64、128… 。Rust 测试固定并核验这些文本的 SHA-256，再要求 FS 每行返回的绝对位置等于行序号，ID 等于该位置的 oracle；其余列按各写入端的明确生成公式逐行核对。Spark 的位置→ID 为 0–4095，直接由已发布的短表 SQL/`oracle.json` 确定。此文本是测试 oracle，不是运行时由被测 FS reader 生成。

PyArrow 覆盖四行组、小页、字典候选、null、列表、UTC 微秒时间戳和 decimal；Trino 覆盖带 field ID 的列表、decimal 和单行组；Flink 覆盖真实本地 SQL 写入、乱序物理行、null、列表、纳秒时间戳和 decimal；Spark 覆盖 Iceberg/parquet-mr 的 field ID 与简单类型。测试对任一类型不兼容或位置错误都直接失败，不能用另一个写入端替代。
