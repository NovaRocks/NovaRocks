# Paimon 外部只读 fixture

这个目录用 Apache Spark 3.5.3 与 Apache Paimon 1.3.1 写出 PAI-1 的只读验收数据。Spark 负责写入和直接读取 oracle；NovaRocks 后续只消费已发布的 Paimon warehouse，因此不会用待测 Rust reader 自写自验。

## 固定输入

- Spark 基础镜像固定为 `apache/spark:3.5.3` 的 Linux/amd64 manifest digest。所有主机都运行同一个 manifest，包括 Apple Silicon Docker Desktop。该 manifest 必须**预先存在于本机镜像库**：准备过程只从本地取镜像，绝不现场拉取。
- `paimon-spark-3.5:1.3.1` 与 `paimon-s3:1.3.1` 固定 Maven Central 坐标、字节数和 SHA-1。镜像构建时同时校验长度和 checksum。
- writer 使用独立镜像和一次性容器，不向共享 Iceberg Spark 服务安装 JAR。
- warehouse 位于 `s3://novarocks/fixtures/paimon-read/<env>/<run>-<digest>`；生成器拒绝其他 bucket、共享 benchmark prefix、路径跳转和自由指定的清理目标。

## 基础镜像必须在本机

准备过程从不拉镜像，镜像缺失是错误而不是下载。首次使用先把固定 manifest 导入本机，之后离线也能跑：

```bash
docker pull --platform linux/amd64 \
  apache/spark@sha256:b2da01c5855fdf791328a6fa1267b406336a535d39abc05699214a48bee95955
```

如果 Docker daemon 无法访问 Docker Hub，就用能通的镜像站拉**同一个 digest**，再用
`PAIMON_SPARK_IMAGE_REPOSITORY` 告诉 fixture 它在本机叫什么：

```bash
docker pull --platform linux/amd64 \
  dockerproxy.net/apache/spark@sha256:b2da01c5855fdf791328a6fa1267b406336a535d39abc05699214a48bee95955
PAIMON_SPARK_IMAGE_REPOSITORY=dockerproxy.net/apache/spark docker/paimon-read/prepare.sh ...
```

该值只是本机镜像的名字；构建仍强制使用 `versions.env` 中同一个 Linux/amd64 manifest digest，并把实际仓库写入证据 manifest。

`fixture.py` 会先在本机按 digest 找到这个 manifest、校验它的平台与身份，再打一个本地别名 tag 交给
BuildKit。Dockerfile 里**不能**写 digest 形式的 `FROM`：BuildKit 对 `FROM repo@sha256:...`
一律先去 registry 解析元数据，本地已有同一个镜像且 RepoDigest 完全匹配也不例外，于是连一次完全命中缓存的构建都会变成 registry 往返，在拉不到 registry 的机器上直接失败。digest 仍是唯一权威，只是校验点从 BuildKit 的 resolver 移到了显式预检。

版本和 checksum 的唯一清单是 [versions.env](versions.env)。变更任何镜像、JAR、SQL 或 oracle 文件都会改变 fixture definition SHA，从而产生新的对象前缀。

## 准备数据

先启动或复用标准 MinIO 环境，再执行准备脚本：

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
  --output-dir /tmp/paimon-dry-run \
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
- `base-server.toml` 与 `sql-runner.toml`：从当前 generated runtime 派生的 system/SQL runner 输入；凭据值替换为 `${ENV:...}` 引用，发布 READY 前还会扫描全部产物，发现当前 access key 或 secret key 即失败。

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
