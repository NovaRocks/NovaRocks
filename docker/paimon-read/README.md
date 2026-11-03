# Paimon 外部只读 fixture

当前 Paimon 读取与测试暂停；恢复条件由 ADR-0204 维护。测试、SQL、oracle 和 writer 配方保留，配置与 catalog 投影不代表读取能力。完整 CI 不再准备本 fixture；SQL/system runner 拒绝所有依赖 Paimon 的显式场景，`tests/run.sh` 明确退出 2，直接 Python unittest discovery 报告 SKIP。以下命令保留为历史 recipe，不代表当前测试可运行。`cleanup.sh` 仍可清理已存在的私有 fixture。

这个目录用 Apache Spark 3.5.3 与 Apache Paimon 1.3.1 写出 PAI-1 的只读验收数据。Spark 负责写入和直接读取 oracle；NovaRocks 后续只消费已发布的 Paimon warehouse，因此不会用待测 Rust reader 自写自验。

## 推荐输入与保留的配方

软件版本、平台和构建配方统一读取 [versions.json](../fixture-inputs/versions.json)。当前推荐 Spark 3.5.3、Paimon 1.3.1，writer 配方只声明 Linux/amd64。工具接口的迁移不恢复暂停的 Paimon 产品能力，不自动仿真或增加支持平台。

已有 writer 镜像直接使用，不做 digest、BOM、软件版本或整机健康检查。确实缺失时使用统一 builder 按推荐创建；Maven JAR 的长度和 checksum 仍用于实际构建输入完整性。已有损坏、状态未知或获取失败时保留错误，交人工维护，不自动覆盖、换版本或回退来源。

```bash
# Explicit preparation of retained inputs; this does not enable Paimon tests.
docker/fixture-inputs/provision.sh --consumer derived:paimon-writer --platform linux/amd64
# Optional explicit transport selection; recommended versions remain unchanged.
docker/fixture-inputs/provision.sh --consumer derived:paimon-writer --platform linux/amd64 \
  --image-source paimon-spark-base=dockerproxy.net/apache/spark
```

writer 使用独立镜像与一次性容器，不向共享 Iceberg Spark 安装 JAR。数据 SQL、oracle、原 producer 事实、warehouse 与 READY 内容校验继续由本 fixture 维护，不能用推荐环境值替代。warehouse 仅限原 run 的准确对象前缀，不能扩大到共享 benchmark 或任意清理目标。

## 准备数据

以下是暂停前保留的显式数据生成流程；缺失输入可由同一推荐 builder 创建，已有环境直接复用：

生成器会直接检查 Parquet footer 与 Avro OCF header；运行它的 Python 环境必须安装 [requirements.txt](requirements.txt) 中固定版本的 `pyarrow` 和 `fastavro`。

```bash
docker/iceberg-rest/up.sh
source docker/iceberg-rest/runtime/current/env.sh
docker/paimon-read/prepare.sh \
  --run-id "$PAI_RUN_ID" \
  --output-dir "$PAI_REPORT_ROOT/fixture"
```

默认推进到 `schema`。快照隔离场景可以建立明确的外部 barrier：

```bash
docker/paimon-read/prepare.sh \
  --run-id "$PAI_RUN_ID" \
  --output-dir "$PAI_REPORT_ROOT/fixture" \
  --stop-after s1

# NovaRocks 开始读取 S1 后，再由外部控制推进：
docker/paimon-read/prepare.sh \
  --run-id "$PAI_RUN_ID" \
  --output-dir "$PAI_REPORT_ROOT/fixture" \
  --stop-after s2
```

阶段顺序固定为 `s1 → s2 → compacted → schema`。每个阶段必须为全部表发布 snapshot、schema、file 与 manifest 汇总 marker，并证明规定的 snapshot/schema/compaction 转换。对已经发布的同一 run 重复调用是幂等的；复用或继续推进前，生成器会重新读取 MinIO，并逐项比较 `objects.json` 中的 key、size 和 ETag。向后请求会复用较新的 READY，向前推进只执行尚未完成的阶段。阶段失败不会发布 READY。下次重试会先按确定性 prefix 精确清理并确认 prefix 已空，再从 S1 重建，避免重复 INSERT 产生不同结果。

只检查版本、路径和 SQL 渲染时可使用 `--dry-run`。它不调用 Docker，也不会发布 READY：

```bash
docker/paimon-read/prepare.sh \
  --run-id dry-run \
  --output-dir "$PWD/logs/experiments/paimon-dry-run" \
  --stop-after schema \
  --dry-run
```

## 数据与 oracle

fixture 覆盖以下外部写入事实：

- 无 bucket 与固定 bucket 的 append 表、分区表、空表和多次 commit；
- Parquet 的 uncompressed、Snappy、Zstd、LZ4 写入，以及 Avro manifest 的 null、Snappy、Zstandard codec；
- 固定 bucket 和动态 bucket 的 `deduplicate` 主键表；
- 跨文件 upsert、Spark `UPDATE`、row-kind delete、单一整数 `sequence.field`、复合键和分区键；
- 未 compact 的多层输入与外部 full compaction 后的等价关系；
- field ID 保持不变的 rename/reorder、nullable 增列、非键列删除，以及同一读取视图中的历史 schema 文件；
- 首期支持的标量类型、边界值与 null 语义；
- ORC/Avro 数据文件、deletion vectors、aggregation merge、嵌套类型、TIMESTAMP_LTZ 等明确不支持表。

每个阶段都运行排序后的完整 relation SELECT，并把带 `NR_ORACLE` 前缀的 JSON 行与 `expected/*.json` 精确比较。COUNT 只用于空表和旧值零命中补充断言。Spark 同时输出 `NR_SNAPSHOT`、`NR_SCHEMA`、`NR_FILE` 和 `NR_MANIFEST` 记录；它们连同 MinIO 对象清单写入阶段产物。

成功目录包含：

- `manifest.json`：版本、SHA、warehouse、最后阶段、对象规模和所有本地产物摘要；不含访问密钥；
- `READY`：`manifest.json` 的精确 SHA-256；仅在 writer、oracle、自检和对象清单全部完成后原子发布；
- `stages/*.json`、`logs/*.log`、`rendered/*.sql`、`objects.json`：原始阶段证据；日志和命令失败在输出到终端或写盘前按当前凭据值脱敏；
- `formats.json`：直接从远端对象读取的 Parquet footer codec 与 Avro OCF header codec 证据；
- `catalog.sql`：NovaRocks 的正常 `type=paimon` Filesystem Catalog 注册语句，引用现有静态 credential binding；
- `base-server.toml` 与 `sql-runner.toml`：从当前 generated runtime 派生的 system/SQL runner 输入。base config 保留 FE metadata 凭据并补齐同 name/generation 的 BE data 凭据，由 native cluster harness 按角色投影；场景 credential overlay 会替换凭据数组，不会追加重复条目。凭据值替换为 `${ENV:...}` 引用，发布 READY 前还会扫描全部产物，发现当前 access key 或 secret key 即失败。

验证本地产物摘要：

```bash
python3 docker/paimon-read/fixture.py verify \
  --output-dir "$PAI_REPORT_ROOT/fixture"
```

## 精确清理

清理命令只接受 READY manifest 指定的 warehouse，不接受任意 S3 URI：

```bash
docker/paimon-read/cleanup.sh \
  --run-id "$PAI_RUN_ID" \
  --output-dir "$PAI_REPORT_ROOT/fixture"
```

脚本会再次校验 READY、全部本地产物摘要、run ID 和允许的 prefix 形状，再把远端 key、size、ETag 与 manifest 所属 `objects.json` 精确比较。只有完全一致才删除该 prefix，并在确认 prefix 已空后删除 READY、写入 `CLEANED`。重复、串错 run 或被外部修改的 prefix 都会失败，不会扩大删除范围。

## 轻量检查

不启动 Docker 或 Cargo 的合同测试为：

```bash
docker/paimon-read/tests/run.sh
```

### 共享机器运行预算

共享消费者通过本次不可变 publication 读取机器事实，继承
`NOVA_FIXTURE_RUN_CONTEXT` 的 run 身份与原截止时间。嵌套 Spark 调用不会重置
预算；容器内 owner 负责准确进程组的终止与实际 wait。正常清理失败会返回失败，
不能以未来 TTL 清扫代替本次清理。私有 publication 保留其原有资源 owner。

此处仅更新运行工具、共享网络和输入绑定；Paimon SQL/System 与原产品测试仍暂停。
`tests/test_runtime_binding.py` 独立验证预算、准确 scratch scope 和拒绝越界路径。

共享模式下继续 `prepare` 或执行 `cleanup` 会恢复 `state.json` / `manifest.json` 保存的原 run context，沿用同一个绝对期限；过期后拒绝续写或重新准入清理。已完成且目标 stage 不再前进的 READY 只按原 warehouse、definition、input lock 和 inventory 只读复核。此处只说明 runtime tooling，Paimon 产品测试仍保持暂停。
