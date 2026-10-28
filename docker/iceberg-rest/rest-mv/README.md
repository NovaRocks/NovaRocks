# rest-mv 构建配方

本 fixture 从 Apache Iceberg 1.10.1 官方源码发布包完整构建 REST adapter，运行 base 与 stock REST 相同。它仅用于验证固定 MV 提案版本的 `storage-table` 保存合同。

## 固定来源

- 源码：`apache-iceberg-1.10.1.tar.gz`，commit `ccb8bc435062171e64bc8b7e5f56e6aed9c5b934`，SHA-1 `09690aa963fd38cefdba69a0d4c7b4eabe0d9ed1`，9216396 字节。下载字节已对照 Apache 发布的 SHA-512。
- 规范：[apache/iceberg#11041](https://github.com/apache/iceberg/pull/11041)，commit `f14886bb4c54d3e292df85161a0250fbb9ae6d12`。
- PoC：[apache/iceberg#9830](https://github.com/apache/iceberg/pull/9830)，commit `bb460f9a9493ceef13d10b6afbf0ee677b8f3a37`。提取时核对 PR head，与补丁中声明的 commit 一致。
- builder：官方 Gradle 8.14.3 + JDK 17，原生平台 `linux/amd64` / `linux/arm64`；digest 由 `docker/fixture-inputs/lock.json` 锁定。直接调用镜像中的 Gradle，避免源码包 `gradlew` 下载未校验的 wrapper JAR。
- 运行 base：lock 中现有 `iceberg-rest`。最终镜像只覆盖 adapter JAR，继承用户、环境与启动方式。

## 补丁

| 补丁 | 来源及目的 |
|---|---|
| `0001` | 移植 PoC 的 6 个服务端类与解析测试；`fromJson` 保留 1.10.1 的 JsonUtil 调用形式。没有移植 Spark 或 refresh-state。 |
| `0002` | 本地规范修正：版本等价判断包含 storage-table；仅修改指针产生不同版本。 |
| `0003` | 本地规范修正：显式 null 与缺失字段同为普通 view；写出省略 null，非法非空形状仍拒绝；测试 servlet 将请求解析错误映射为 HTTP 400，实际 Reader I/O 错误仍向上传递。 |

显式供给的 `--artifact-url-prefix ORIGINAL=TRANSPORT` 也通过 recipe 声明的 `artifact_url_prefix_build_arg` 传给此构建。`transport.init.gradle` 在原 Gradle 命令执行前监听 settings/pluginManagement、buildscript 和 project Maven 仓库，仅改写匹配的目录 URL，未匹配仓库和原顺序保持不变，不额外加入 fallback。Maven Central 与 Gradle plugin portal 可分别选择镜像，完整参数见 [fixture 输入来源](../../fixture-inputs/README.md)。该 JSON 只存在于 builder 阶段，运行镜像不继承它；版本和校验元数据不因传输位置改变。

构建执行 `TestViewVersionParser`、`TestViewMetadata` 和 `TestRESTCatalogServlet`，然后构建 `:iceberg-open-api:shadowJar`。依赖与插件使用仓库内 `verification-metadata.xml` 的 SHA-256 严格校验；缺失或不符即构建失败。Gradle 使用 2 个 worker、1536 MiB heap；构建资源实测记录在实施计划。

源码包报告 `1.10.1`，stock JAR 的版本串为 `1.11.0-SNAPSHOT`，二者的源码 commit 同为 `ccb8bc43`。比较普通行为时忽略版本串、UUID、时间戳和位置。

## 退出条件

Iceberg 官方 REST fixture 发版并通过本配方的合约与生命周期检查后，切换到官方产物并删除源码构建和补丁。stock 对照保留为明确固定的旧版本。

## 运行与端点

从仓库根目录显式 provision 输入后，正常启动 fixture 并固定本次 publication：

```bash
docker/fixture-inputs/provision.sh
docker/iceberg-rest/up.sh
fixture_publication="$(python3 -c 'from pathlib import Path; print(Path("docker/iceberg-rest/runtime/current/published").resolve(strict=True))')"
source "$fixture_publication/env.sh"
```

共享 catalog 和 runner 管理的私有隔离栈都包含 `rest-mv`；普通测试不下载或构建输入。服务使用独立 SQLite 卷，服务端 warehouse 与 stock `rest` 同级、以 `rest-mv` 结尾。接口如下：

| 消费者 | 端点与 warehouse |
|---|---|
| Host | `NOVAROCKS_ICEBERG_REST_MV_URI`、`NOVAROCKS_ICEBERG_REST_MV_WAREHOUSE` |
| publication | `NOVA_ENV_REST_MV_PORT`、`NOVA_ENV_REST_MV_SERVER_WAREHOUSE_URI` |
| manifest | `iceberg_rest_mv.uri`、`iceberg_rest_mv.warehouse`、`iceberg_rest_mv.server_default_warehouse` |
| SQL runner | `[env].iceberg_rest_mv_uri`、`[env].iceberg_rest_mv_warehouse`；同时投影为上述 Host 环境变量 |
| Spark | `ice_rest_mv` catalog、`NOVAROCKS_SPARK_REST_MV_URI=http://rest-mv:8181`；默认 catalog 仍为 `ice_rest` |

客户端 warehouse 为 `s3://warehouse/<env-id>/rest-mv`。端口从 publication 读取；客户端 warehouse 与共享 catalog 的服务端默认 warehouse 各有所有者，不能互换。隔离 publication-hook profile 仅替换 `rest`，保留 `rest-mv` 的普通服务定义。

## 合约与生命周期检查

```bash
python3 docker/iceberg-rest/rest-mv/probe.py contract --manifest "$NOVA_ENV_MANIFEST"
```

`contract` 创建带随机后缀的 namespace，检查创建、只改指针的新 version、历史、显式 null 和普通 view/table 行为；stock `rest` 必须丢弃 `storage-table`。探针在 `finally` 中清理本次对象，清理失败给出告警，不打印凭证，也不重启服务。CI 每轮在准备阶段运行此命令。

`lifecycle` 还检查非法形状返回 400、对象存储 metadata 与 REST 读回的一致性，以及重建 `rest-mv` 容器后 UUID 与历史仍可读取。它只接受 manifest 中 `shared_docker=false` 且项目名以 `nr-isolated-rest-` 开头的栈，通过该栈保存的 Compose 定义操作：

```bash
python3 docker/iceberg-rest/rest-mv/probe.py lifecycle --manifest /path/to/isolated/manifest.json
```

`parity` 用于参考构建；将 manifest 的 `iceberg_rest_mv` 端点指向使用同一配方、去掉补丁的参考镜像，然后检查普通行为与 stock 相同，并且同样丢弃字段：

```bash
python3 docker/iceberg-rest/rest-mv/probe.py parity --manifest /path/to/reference/manifest.json --expect-dropped
```

`--expect-dropped` 仅用于 `parity`。退出码为 0（通过）、1（合约不符）、2（环境或参数错误）；CI 将后两者归为准备阶段的 VERIFY FAILED。fixture 输入缺失的 exit 75 属于运行探针前的 BOM verify，归为 BLOCKED。

## 依赖校验文件维护

源码基线、补丁或构建任务改变时，运行：

```bash
docker/iceberg-rest/rest-mv/update-verification-metadata.sh \
  --artifact-url-prefix https://repo.maven.apache.org/maven2/=https://maven.aliyun.com/repository/public/ \
  --artifact-url-prefix https://plugins.gradle.org/m2/=https://maven.aliyun.com/repository/gradle-plugin/
git diff -- docker/iceberg-rest/rest-mv/build/verification-metadata.xml
```

工具通过 provision 的 `--context-only rest-mv` 组装同一构建上下文，执行 Dockerfile 的 `verification-metadata-export` 目标，并用 `--refresh-dependencies` 重新解析父 POM，避免热缓存遗漏校验条目，再将结果写回 `build/verification-metadata.xml`。它会获取锁定的 base 和源码输入并联网解析依赖，但不发布 fixture store 的 BOM/READY，也不改写 derived image 别名。可选参数为 `--image-source NAME=REPOSITORY`（可重复，显式选择锁定镜像的下载仓库）、`--artifact-url-prefix ORIGINAL=TRANSPORT`（可重复）和 `--docker-pull-timeout-seconds <seconds>`。维护入口与 provision 共用同一前缀校验和 recipe ARG 作者；context-only 下载及 metadata 构建消费相同映射。镜像仍须匹配 lock 中的 digest。

维护者审查新增依赖、插件、来源与校验和后提交变更。正常 provision 使用严格校验，缺项或不符即失败；BuildKit 的 `/home/gradle/.gradle` 缓存可在严格校验下复用。修改探针或本 README 不改变构建身份；`build/` 中全部文件都属于 lock 的定义输入。

## 机器切换与旧实例退出

输入仍只有当前lock/BOM。provision先完成全部derived image构建，再更新别名、BOM与READY；构建失败保持旧状态，提交段中断可能留下部分已改写别名，需显式重跑provision恢复。测试/verify不自行供给。

输入改变后，维护者显式执行 `fixture-runtime.sh upgrade`；worktree普通up只核验同版本ready机器并发布不可变输入，旧lock或定义不符会BLOCKED。机器固定运行stock rest和rest-mv，各有准确SQLite状态卷。维护保存MinIO整卷及benchmark全部对象，撤销旧访问材料并在线清理scratch、切换及健康核验后才提交ready；不按旧catalog ID创建并存栈或执行stop/delete。

旧项目和卷先形成准确保留清单，在维护窗口另行处理，不自动认领或删除。privatelifecycle探针仍只操作自身准确项目；共享contract探针使用本run namespace/location及原预算。回退代码本身不会恢复机器输入、凭证或已清理scratch。详见 [fixture README](../README.md#输入与机器维护) 和 [ADR-0191](../../../docs/adr/ADR-0191-machine-shared-fixture-and-bounded-scratch.md)。
