<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Iceberg REST + Object Storage + Spark Test Environment

本目录提供机器级共享 stock REST、支持 MV `storage-table` 的 rest-mv 和 Spark。对象存储可显式选择阿里云 OSS，或使用本地 MinIO/mc。每个 Docker daemon 只有一个固定项目 `nr-fixture-machine`、一个固定网络 `nr-fixture-machine_iceberg_net`；输入变化通过显式维护切换，普通 worktree 不创建并存共享栈。私有 stock/publication-hook catalog 和 HMS 各自保留原 owner。

## 阿里云 OSS

新 Linux 节点优先使用 [Linux 环境准备入口](../../tools/dev/README.md)，先为 ECS 绑定可访问测试 Bucket 的 RAM 角色，再提供独立的非秘密配置。配置恰好包含以下五个字段，示例中的 Bucket 和角色须替换为自己的值：

```json
{
  "kind": "aliyun-oss",
  "region": "cn-beijing",
  "bucket": "YOUR_TEST_BUCKET",
  "role_name": "YOUR_ECS_ROLE",
  "root_prefix": "novarocks-development"
}
```

`root_prefix` 限定本环境的对象范围，规范化后带尾 `/`；benchmark、worktree、私有 catalog 和机器 warehouse 分别位于其 `benchmarks/`、`worktrees/`、`catalogs/` 和 `machines/<machine-id>/<generation>/` 子域。不要在配置中加入 AccessKey、SecretKey 或临时 token。FE、BE 各自通过节点本地 ECS RAM Role 来源取得和刷新材料；REST、Spark 和 host 对象操作各用官方 Alibaba provider。各入口固定启用 IMDSv2，凭证只保留在各自进程内存中。容器也须能访问 ECS 的 IMDSv2 服务。

手动维护时，先将这份五字段 JSON 保存到绝对路径，再生成机器输入。以下命令使用默认账户 fixture store；若显式选择 store，provision、verify、机器和 worktree 必须消费同一位置。

```bash
object_store_config=/absolute/path/object-store.json
machine_config=/absolute/path/fixture-machine.json
python3 - "$object_store_config" "$machine_config" <<'PYCONFIG'
import json
from pathlib import Path
import sys
config = json.loads(Path(sys.argv[1]).read_text())
Path(sys.argv[2]).write_text(json.dumps({"object_store": config}, indent=2) + "\n")
PYCONFIG

docker/fixture-inputs/provision.sh --consumer iceberg-rest-oss --platform linux/amd64 \
  --artifact-url-prefix https://repo.maven.apache.org/maven2/=https://maven.aliyun.com/repository/public/ \
  --artifact-url-prefix https://files.pythonhosted.org/packages/=https://mirrors.aliyun.com/pypi/packages/ \
  --artifact-url-prefix https://plugins.gradle.org/m2/=https://maven.aliyun.com/repository/gradle-plugin/
docker/fixture-inputs/verify.sh --consumer iceberg-rest-oss --platform linux/amd64
docker/iceberg-rest/fixture-runtime.sh start --config "$machine_config"
export NOVA_ENV_OBJECT_STORE_CONFIG="$object_store_config"
docker/iceberg-rest/up.sh
docker/iceberg-rest/fixture-runtime.sh check --config "$machine_config"
```

手动 provision 的 `--artifact-url-prefix` 仅选择传输位置，原锁的长度和 hash、BOM identity、内容缓存键不变，错误镜像直接失败且不回退。Linux prepare 从 profile 的 `fixture_artifact_url_prefixes` 显式转发相同设置，check/verify 不下载；完整约束见 [输入来源说明](../fixture-inputs/README.md)。

当前 OSS host SDK 闭包仅声明 Ubuntu 26.04、原生 amd64、Python 3.14。provision 准备固定依赖的独立 venv 和 REST/Spark 凭证桥接镜像；verify、check、运行层不会临时执行 pip/Maven 下载。`iceberg-rest-oss` 只选择对应闭包，不拉取或编译 MinIO/mc；普通本地 MinIO 仍选择 `iceberg-rest`。OSS 配置缺失或错误时明确失败，不回退到 MinIO。

机器将规范化五字段配置保存为 activation 目录中的不可变 `object-store.json`。publication 中的 `NOVA_ENV_OBJECT_STORE_CONFIG` 和 host SDK 路径固定到原激活输入，不能通过改写安装器的期望配置切换旧运行。配置或输入版本变化须显式 `upgrade --config "$machine_config"`，随后重新 attach；同一健康定义的 `start` 复用原 generation。升级停止原共享 writer，确认旧 REST catalog 屏障，再使用新的机器 warehouse，保留旧 OSS 对象；不创建或撤销云 IAM，也不把换代当成旧 token 已失效。MinIO 与 OSS 的机器 owner 不在原地互相转换。

旧 ECS 释放后，在新 ECS 绑定同一 RAM 角色，取得相同源码、profile 和五字段配置，再执行同一 Linux prepare 命令。新机器重新取得本机 BOM、image ID、daemon 身份和 catalog generation，不复制旧 READY、SQLite 或运行定位信息。

OSS 的 S3 兼容接口不接受 AWS SDK 的可选 checksum trailer 编码 `STREAMING-UNSIGNED-PAYLOAD-TRAILER`。cloud REST、REST-MV、Spark 及生成的 env 显式设置 `AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_REQUIRED`、`AWS_RESPONSE_CHECKSUM_VALIDATION=WHEN_REQUIRED`，只保留 API 要求的 checksum 行为；本地 MinIO 不强加这项配置。该设置沿用 SDK 的官方开关，不改变 SDK 版本或生产 Rust 签名逻辑。设置及必需校验边界见 [AWS Java SDK checksum 文档](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/s3-checksums.html)。

普通建表、写入验证使用 REST catalog。Hadoop catalog 的生产 metadata 条件发布协议没有改变，而 OSS 的当前 S3 兼容接口不支持该条件写；不能将普通对象读写可用解释为 Hadoop catalog 提交可用。MinIO IAM/STS、vended 和 Paimon 专项在 OSS 模式下于副作用前明确拒绝，不能以 OSS 冒充这些前置。

OSS 的普通启动不扫描并删除 TTL 遗留；显式 sweep 返回 `status=NOT_RUN`、`scope=cloud-ttl`、`cleanup_performed=false` 和保留原因。原运行清理独立核对 writer 实际退出、原 catalog 屏障及准确 run scope，再清其对象和 multipart；未知 writer、兄弟前缀、benchmark 和旧 generation 不因 TTL 或换机获得删除授权。

## 输入与机器维护

[ADR-0155](../../docs/adr/ADR-0155-fixture-provisioning-and-offline-verification.md) 继续拥有输入供给。只有显式 provision 可以下载、pull/build；verify、机器运行层、普通测试只消费本机已验证的 lock/BOM、derived 定义和实际 image ID。

```bash
docker/fixture-inputs/provision.sh --consumer iceberg-rest  # Explicit acquisition/build only.
docker/fixture-inputs/verify.sh --consumer iceberg-rest
docker/iceberg-rest/fixture-runtime.sh start  # Explicit machine maintenance.
docker/iceberg-rest/up.sh                     # Attach this worktree only.
```

机器工具需要 Python 3.10 或更新版本，并提供 POSIX `waitid/WNOWAIT` 与进程组观察能力；本机验证使用 Python 3.14。macOS 的 CLT Python 3.9 不满足原生监督与跨进程单调时钟合同，工具在调用 Docker 前返回 BLOCKED。请在调用环境的 PATH 明确选择已安装且满足能力的 Python；工具不自动安装或切换解释器。

普通 `up.sh` 要求机器已经 ready、健康且完整版本匹配，不启动、修复或升级服务。机器未就绪、维护中、输入缺失或版本不符返回 BLOCKED（75）。实际生命周期、身份或执行错误保留失败，准入后的期限耗尽不改称 BLOCKED。CI 仍 verify→attach→读取一次 publication，不自行 provision/upgrade。

共享 REST 与 rest-mv 各自使用独立的 SQLite catalog 卷，并设置 `CATALOG_CLIENTS=1`，让同一 catalog 的 JDBC 操作排队使用一个连接。当前固定镜像在重新打开已有数据库时，初始化查询可能留下活动结果集；多连接会因此持续遇到 `SQLITE_BUSY`，仅延长锁等待或启用 WAL 不能保证恢复。此配置保留 REST 请求与对象存储访问的并发，不修改 Java 实现，也不增加 HTTP 500 的盲重试。模板属于机器 version，已有机器须显式 upgrade 后重新附着。

机器控制目录默认 `${XDG_STATE_HOME:-$HOME/.local/state}/novarocks/fixture-machine`；管理命令支持子命令前的 `--root`，控制目录不是另一个资源命名空间。daemon 上的固定 authority 卷阻止换目录创建第二套。机器配置持久保存在 daemon 对应 machine 目录；公共 status 隐去凭证。

```bash
docker/iceberg-rest/fixture-runtime.sh status
docker/iceberg-rest/fixture-runtime.sh check  # Read-only: current source/BOM, exact version and health.
docker/iceberg-rest/fixture-runtime.sh upgrade
docker/iceberg-rest/fixture-runtime.sh --timeout 5 sweep
```

失败的只读 Docker 观察与未知 mutation 分别保留。确认 status 中准确 journal 后，可显式处置该观察的 mutation 责任：

```bash
docker/iceberg-rest/fixture-runtime.sh resolve-readonly-observation --journal /absolute/control-root/machine-id/operations/control-id.json
```

此命令只在原机器维护锁内核对当前 daemon、机器身份和该 journal 的原请求，以原 owner 共用的只读命令分类验证后记录处置；重复调用返回已验证的原处置，不重写记录。它不运行 start、upgrade、sweep，不遍历或结算其他操作。原失败、phase 及原进程退出事实完整保留，缺少退出证明仍为 Unknown；status 分别展示失败和处置。未知 mutation、外来或不可信请求继续报错。该处置不证明 CLI 或远程 writer 已退出，不提供 scratch 清理权限，也不改写旧运行的 cleanup violation；后续运行清理仍须满足原 context、期限、writer 退出及准确 scope 条件。

`start` 幂等恢复同一输入定义，版本不同要求显式 `upgrade`。首次维护可在 start 后传 `--config /absolute/machine.json`；本地 MinIO JSON 字段为 `ports`、`policy`、`benchmark_root`、`minio_volume`；OSS 使用上述 `object_store`，不接受 `minio_volume`。端口是准确固定配置，默认 MinIO API/console=28000/28001、REST/rest-mv=28002/28003、Spark UI=28004、HMS接入槽=28005；冲突报错，不扫描替代端口。FE/BE listener 仍由 worktree/harness 分配。`shared.env` 属于 worktree/private 输入，不是机器端口、root凭证或维护状态的权威。

完整 version 包含协议、验证后的有效 BOM/derived 定义、实际 image ID、机器模板和配置，不把时间、cache路径或可变tag当版本。generation标记一次实际激活。以下 root/IAM 维护步骤属于本地 MinIO；OSS 的维护与保留范围见上节。MinIO 升级先持稳定维护锁并保存maintenance，再停止共享writer、持久化新材料并轮换旧root、撤销run IAM/STS；确认旧root被真实拒绝后，先移除准确旧REST实例及数据库卷，原生确认absence并持久记录catalog屏障，再保持准确MinIO在线清理scratch。benchmark全部对象保留；切换服务、核实资源/image/端点/健康后才提交ready。固定REST恢复后旧请求仍可能成功重建scratch，不承诺这些请求失效。失败留maintenance；未知mutation不伪结算。明确的daemon端口终态失败也须在同一保存的维护意图下重新检查准确资源才可重试。

已有 MinIO 卷只有维护者明确指定准确来源才可迁入，原卷和全部 benchmark 数据保留；不猜“最近一个”。历史版本化项目、旧 `nr-iceberg-rest` / `nr-iceberg-hive` 资源不自动认领、迁移或删除。首次切换须先记录资源/数据清单和维护窗口。

## Worktree publication

```text
runtime/current -> <env-id>
runtime/<env-id>/
  .entry.lock
  published -> publications/<content-id>
  env.sh -> published/env.sh
  publications/<content-id>/
    entry.json env.sh manifest.json README.md fe.toml be.toml sql-test.toml
    ice-rest-catalog.sql spark-defaults.conf spark-iceberg-v3-smoke.sql
  frontend-state.sqlite
```

完整文件集合先落盘，再原子替换一次 `published`。重复附着同配置复用相同内容与env；version/generation改变会改变输入。锁序为worktree入口→机器维护锁，机器维护不反向拿worktree锁。消费者启动时解析一次：

```bash
docker/iceberg-rest/up.sh
fixture_publication="$(python3 -c 'from pathlib import Path; print(Path("docker/iceberg-rest/runtime/current/published").resolve(strict=True))')"
source "$fixture_publication/env.sh"
```

`NOVA_ENV_REST_ENV_FILE` 指向不可变env；`NOVA_ENV_RUNTIME_DIR` 只保存稳定SQLite等数据，不能在其下拼env.sh。正常attach失败会禁用发现入口；已固定的旧不可变publication仍保留，没有消费者租约或在线透明切换。

共享 manifest 的 `runtime.machine` 包含准确 machine ID、daemon、完整version、activation generation、image/container/network/volume facts和producer receipt。私有栈使用 `runtime.catalog`；vended额外携带共享MinIO machine事实。没有旧owner locator或多版本绑定协议。

env导出 `NOVA_FIXTURE_MACHINE_ID/DAEMON/VERSION/GENERATION/ROOT`、TTL/max-run/cleanup policy，以及原有FE/BE/SQL/Spark配置和物理端点。`NOVA_ENV_OBJECT_STORE_RUNTIME` 与 `NOVA_ENV_CATALOG_RUNTIME` 在共享模式投影同一machine ID，不是旧管理API。host使用保存的实际端口；容器内 REST 别名为 `rest:8181`、`rest-mv:8181`；本地 MinIO 使用 `minio:9000`，OSS 使用配置地域的 S3 兼容内网 HTTPS endpoint 与 virtual-host 寻址。客户端warehouse不能当作REST服务端实际数据位置。

```bash
docker/iceberg-rest/up.sh --prepare-only
```

离线prepare不调用Docker、不验证BOM或健康，只发布unbound并清除端点/镜像变量。它不能给测试启动提供ready证明；后续正常up才真正附着。

## Scratch、累计预算与消费者

每次共享运行在首次scratch副作用前准入完整预算，SQL/System参数为 `--fixture-budget-secs`（默认3600）；Spark/直接producer使用同名参数或 `NOVA_FIXTURE_BUDGET_SECS`。机器默认 max-run86400、清理预留300、TTL604800秒。嵌套shell、重试、重启继承同一 `NOVA_FIXTURE_RUN_CONTEXT` 与原绝对期限，不能重新计时。超上限在副作用前BLOCKED，已准入超时为FAIL。

run名称为 `nr_scratch_<13digit epoch_ms>_<32lowerhex>`，共享namespace为完整run ID加 `_` 和最多5字符用途后缀。`NOVA_FIXTURE_RUN_STORAGE` 与原预算分开，开始前固定三个范围：原worktree/run目录根、REST warehouse下的 `<run-id>_` 名称前缀、rest-mv warehouse下的同类前缀。目录根用单个尾 `/`，名称前缀的 `_` 后不加 `/`。原生JDBC默认建表使用服务端warehouse/namespace，不继承namespace location；SQL与Java原行为不改。普通表/view、同运行rename、显式位置及CTAS统一核对所属运行范围，客户端warehouse只约束显式CTAS。Hadoop可变warehouse绑定run root，已有只读READY保持原绑定。

本地 MinIO 的共享 REST/Spark 使用共享凭证，运行 IAM 只约束使用运行凭证的 writer。OSS 各消费者独立刷新节点凭证，存储身份固定 provider、endpoint、region、bucket 和 root_prefix，不能伪装成 MinIO 卷。Python/Rust 按准确存储 owner 核实继承投影；s3a仅作为同绑定比较别名。原始控制字符、重复斜杠、dot组件及歧义URI拒绝，不折叠成其他S3 key。

发布前按bucket/key组件核实REST、rest-mv、benchmark与准确worktree/private基础根不相交，交叠即拒绝；公共祖先不是删除域。CTAS与vended的明确运行子根可以位于原运行根内，不能据此扩大独立基础根或删除范围。

本地 MinIO 的实际写入入口在首次副作用前执行一次显式启动清扫，固定原publication与原预算，最多使用5秒或原工作剩余期限。只清过期scratch；确认完成退出的清扫超时保留pending供后续继续，I/O/权限/清理错误仍失败。通用context准入和READY `--check`保持只读。

独立运行结束确认writer实际退出后，先清两个REST服务中该运行所有条目与namespace并确认不存在，本地 MinIO 再撤销 IAM，最后清原根及实际枚举出的合法 namespace 目录；OSS 不执行 IAM 操作。catalog 或必要的 IAM 步骤失败、结果未知时保留文件。同运行rename可保留旧目录，不能只凭一个namespace消失清文件。嵌套producer将文件责任交原外层owner并明确记录deferred；失败保留登记和原始退出。显式sweep有总期限，以原生时间/名称/准确scope分类TTL遗留；无法确认的资源不删除。自定义benchmark root、祖先/后代及所有对象（包括staging）先保护。TTL不代替正常teardown。未知namespace命名不回收；status显示其数量及最近scratch观察的采样时间、count/oldest、部分枚举和activation适用范围；未观察为UNKNOWN。

```bash
export NOVA_FIXTURE_RUN_CONTEXT="$(python3 docker/iceberg-rest/fixture_run.py context --env-file "$NOVA_ENV_REST_ENV_FILE" --fixture-budget-secs 3600)"
export NOVA_FIXTURE_RUN_STORAGE="$(python3 docker/iceberg-rest/fixture_run.py storage --env-file "$NOVA_ENV_REST_ENV_FILE")"
docker/iceberg-rest/spark-sql.sh "$NOVAROCKS_SPARK_V3_SMOKE_SQL"
# The outer owner invokes this only after all of its writers have exited.
python3 docker/iceberg-rest/fixture_run.py cleanup-run --env-file "$NOVA_ENV_REST_ENV_FILE"
```

共享Spark wrapper固定实际container ID，并由容器内原进程owner管理准确run进程组；杀宿主Docker客户端不等于JVM退出。工作期到后TERM/KILL，保留未reap leader直至真实组退出；逾期清理保持失败，不能把完整期限到当成退出。另一运行及共享Spark容器不因此停止。

```bash
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg,iceberg-rest,iceberg-compatibility \
  --mode verify --cluster-mode cross-process --cluster-size 3 --fixture-budget-secs 3600
```

产品验收使用原生1FE+3BE，all-in-one仅smoke。每个prepared cluster拥有独立deployment ID，同cluster重启沿用；角色进程从启动到执行/重启均受原预算监护。

## 私有栈、HMS、数据集与解绑

私有stock/publication-hook保持唯一项目、独立REST/rest-mv/SQLite和原创建/退出合同，`NOVA_ENV_UPDATE_CURRENT=false`。hook在创建前选择，不能替换已ready的stock。vended测试只共享准确MinIO，在原run root创建真实两套IAM/STS，私有REST/Spark使用原命令argv的容器期限guard；先确认私有writer退出才清IAM/对象，失败不伪成功。

HMS只接入准确machine网络，使用机器预留HMS端口；其Derby卷和项目由HMS入口自己管理，不计为核心栈。机器换activation后先显式退出旧HMS记录再附着，不由REST owner递归删除。

本地 MinIO 的共享 benchmark 默认 `s3://novarocks/shared/benchmarks`，OSS 默认为配置 Bucket/root_prefix 下的 `benchmarks`；显式输入owner按BOM producer供给；已有消费者只check准确READY，不bind/up/ensure或改写warehouse。见 [ADR-0173](../../docs/adr/ADR-0173-prepared-dataset-consumption-is-check-only.md)。

```bash
docker/iceberg-rest/down.sh --runtime-only
docker/iceberg-rest/down.sh --runtime-only --purge
```

共享down发布unbound并保留锁、SQLite及不可变publication。`--purge`不获得整个worktree S3根的删除权；原run cleanup与TTL负责scratch。共享 `--docker/--volumes` 拒绝；私有stack仍走准确项目/卷确认的原teardown。没有多版本stop/delete管理命令。

MV轻量合约探针仍可运行：

```bash
python3 docker/iceberg-rest/rest-mv/probe.py contract --manifest "$NOVA_ENV_MANIFEST"
```

共享contract检查不重启服务；重建容器的lifecycle模式只接受私有隔离栈。Paimon产品测试及五项mixed catalog-planning维持原暂停范围，runtime适配单测不能被记作产品恢复。当前所有权规则见 [ADR-0191](../../docs/adr/ADR-0191-machine-shared-fixture-and-bounded-scratch.md)。

## External-Engine MV Read Interop

NovaRocks Iceberg materialized views are readable from other Iceberg-aware
engines (Spark, Trino, etc.) through this REST catalog, as long as they only
read. NovaRocks is the sole writer of MV tables; it detects and fails loud on
external writes (see below).

### MV package: one Iceberg table, one descriptor

Creating `CREATE MATERIALIZED VIEW mv_orders AS SELECT id, name FROM orders`
against a REST iceberg catalog produces one Iceberg table named `mv_orders`.
That table is the materialized authority. It holds every public MV output
column plus NovaRocks internal columns needed to apply refreshes (an apply-key
column such as `__nova_base_row_id`, and for aggregate MVs, per-aggregate state
columns). Iceberg table properties carry the apply-key wiring
(`novarocks.mv.apply-key.column`, `novarocks.mv.apply-key.source`,
`novarocks.mv.apply-key.field-id`, `novarocks.mv.hidden-columns` when aggregate
state exists) and the MV descriptor (`novarocks.mv.descriptor.package-id`,
`novarocks.mv.descriptor.hash`, `novarocks.mv.descriptor.inline`).

The descriptor is the boundary between external read columns and NovaRocks
internal columns: `visible_columns` lists the public read surface, while
`hidden_columns` lists implementation columns that external engines should not
select unless they are debugging or repairing an MV.

Reading the MV table's visible columns means reading already-materialized data
from the MV table — it is **not** a re-run of the MV's original base-table
query.

### Reading an MV from an external engine

```sql
-- Public read contract: select the descriptor's visible columns.
SELECT id, name FROM <catalog>.<namespace>.<mv_name>;

-- Schema-level contrast: the same table also carries internal columns
-- (e.g. __nova_base_row_id, and __agg_state_<alias> for aggregate MVs).
DESCRIBE <catalog>.<namespace>.<mv_name>;
```

Through this environment's REST catalog, that is `ice_rest.<namespace>.<name>`
from Spark and `<catalog>.<namespace>.<name>` from NovaRocks, where
`<catalog>` is whatever alias NovaRocks registered for the same REST
catalog/warehouse (the two engines see the same physical objects under their
own catalog aliases).

### External writes are a violation

NovaRocks refreshes validate that the MV table's Iceberg snapshot still
matches what NovaRocks itself last wrote (`validate_target_snapshot` in
`src/engine/mv/iceberg_refresh.rs`) before committing the next refresh. If
another engine committed to the MV table's `main` branch in between, the next
NovaRocks refresh fails loud with an explicit "modified outside NovaRocks"
error instead of silently absorbing or overwriting the foreign change.

### Verifying with Spark

`tests/sql/correctness/iceberg-compatibility/sql/novarocks_rest_minio_mv_table_read_by_spark.sql`
is the CI-gated recipe for this contract: NovaRocks creates and refreshes an
Iceberg MV in the REST `ice_rest`-backed catalog, then two Spark `spark-sql.sh`
steps read the MV table's visible materialized columns and verify that
`DESCRIBE` on the same table exposes the internal apply-key column. Run it
with the rest of the suite:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql/runner/Cargo.toml -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility --only novarocks_rest_minio_mv_table_read_by_spark \
  --mode verify
```

### Java REST 提交互操作

`tests/iru13a-rest-interop-test.sh` 沿用固定 publication 和本地 BOM，执行 provider 内全部 14 个 ignored Java REST 用例。调用方先运行 `up.sh`，然后设置本 worktree 的 `CARGO_TARGET_DIR`（推荐当前 worktree 的 `target/`，使用 `dev` profile）与可选 `NR_IRU13A_INTEROP_ARTIFACT_DIR` 独占材料根；入口拒绝已存在目录，并为三个测试族建立独立 namespace/材料子目录。入口不启动或重绑 fixture、不下载缺失输入。

材料保留 Cargo 实际报告的测试二进制路径/hash、源码 HEAD/diff、构建 profile、完整用例清单、构建/运行双流和真实退出。14 个 exact 名称必须全部通过；零用例、少例、额外用例或 ignored 均不算成功。Java 独立读回 UUID、metadata location、snapshot/ref/retention 与物理格式事实，prepared owner/journal 的 receipt、NoOp 和独立收尾失败也在既有用例中验证；这些不替代原生 1FE+3BE 的 FE 核查与 SQL 验收。变更发布、维护或 REST 协议时显式运行，普通 unit 仍保留 ignored 边界。失败材料由调用方持有，不随 fixture 自动删除。

Linux 新开发节点的显式安装入口见 [Linux 环境准备](../../tools/dev/README.md)。它可联网安装和 provision；Codex 自动 setup 的 `--prepare-only` 仍只生成离线 unbound 入口。
