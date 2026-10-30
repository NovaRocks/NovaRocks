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

# Fixture 输入供给与平台

`lock.json` 是唯一源锁。schema 2 为镜像和派生 recipe 声明平台变体，公共 artifacts 共享；解析、闭包选择和平台规范化由 `fixture_inputs.py` 统一拥有。`linux/amd64` 和 `linux/arm64` 的 Iceberg 输入准确区分；Paimon writer 仍仅支持 amd64。

```bash
# Explicit online acquisition and build; select only the required consumer.
docker/fixture-inputs/provision.sh --consumer iceberg-rest --platform linux/amd64
# Local observation only: no pull, network download or build.
docker/fixture-inputs/verify.sh --consumer iceberg-rest --platform linux/amd64
```

省略 platform 时读取本机原生事实；实际供给/消费还核对 Docker daemon 与目标一致。纯本地 context/producer 投影可以显式选择目标；缺失变体明确拒绝，不仿真、不静默缩小 all。arm64 的 all 包含不支持的 Paimon 时应拒绝；显式选 iceberg-rest。使用 `--store` 选择账户的 store，消费者必须消费同一个位置。

平台参与有效 lock_id，consumer 只决定闭包，不改变 lock_id。同 lock/platform 的重复供给验证实际输入，已满足时不重发 BOM；补充闭包时重新核验保留回执，将有效 artifact 移入新 generation，原子发布合并结果。不得以部分供给回退已有有效覆盖。

BOM schema 2 记录 platform、锁身份、镜像/派生定义和 artifact 证明。schema 1 BOM 不猜测升级；旧输入、旧材料和数据卷保留，显式重新 provision，再按 [机器维护入口](../iceberg-rest/README.md) start/upgrade 和 worktree attach。新 BOM 不可直接从另一机器复制，实际 image ID 和 daemon 仍需本机证明。

传输 repository 可用 `--image-source logical-name=transport-repository` 配置。来源不是内容身份：digest、平台、artifact 长度/hash 不变。数据集 producer 的语义 key 使用对应平台的实际 recipe/基础镜像/artifact 子投影；无关镜像别名或传输变化不应改变 key，producer 源码变化仍按原规则产生新 key。

artifact 下载可显式使用可重复的 `--artifact-url-prefix ORIGINAL=TRANSPORT`，只替换已选择 artifact URL 的传输前缀：

```bash
docker/fixture-inputs/provision.sh --consumer iceberg-rest-oss --platform linux/amd64 \
  --artifact-url-prefix https://repo.maven.apache.org/maven2/=https://maven.aliyun.com/repository/public/ \
  --artifact-url-prefix https://files.pythonhosted.org/packages/=https://mirrors.aliyun.com/pypi/packages/ \
  --artifact-url-prefix https://plugins.gradle.org/m2/=https://maven.aliyun.com/repository/gradle-plugin/
```

两侧必须是 HTTPS 目录 URL，以 `/` 结尾，不带 userinfo、query 或 fragment；原始前缀重复或互相包含时拒绝。只匹配原锁 URL 一次，不递归改写。选择的镜像不可达或内容不符直接失败，不自动回退来源。原锁中的 URL、bytes、SHA1/SHA256、lock identity 与内容缓存键均不变；镜像返回同名文件也须通过原始校验。相同内容缓存可跨传输地址复用，但每次仍核验字节。

Linux profile 的 `fixture_artifact_url_prefixes` 使用原始前缀到传输前缀的 JSON 映射；阿里云 profile 显式配置上述 Maven、PyPI 和 Gradle plugin 映射。installer 仅在 prepare 时将每条映射传给 provision，context-only 同样接受显式 CLI 参数；check 和 verify 不新增网络访问或下载修复。

派生 recipe 可通过可选字段 `artifact_url_prefix_build_arg` 声明一个独占的大写 Docker ARG 名称，例如 `NOVA_FIXTURE_URL_PREFIXES`。provision 只向声明该字段的构建传入同一已验证映射的 JSON；普通 artifact 下载与 Gradle 内部下载复用同一显式来源选择。ARG 名不能与 bases、derived_bases 或固定 build_args 冲突，未声明的 recipe 不接收参数。

REST-MV 的 `transport.init.gradle` 在 Gradle settings、project 和 buildscript 的 Maven 仓库注册时改写匹配的目录 URL，兼容仓库 URL 有无尾 `/`；未匹配的仓库、顺序和数量保持原样，不新增 fallback。Gradle plugin portal 和 Maven Central 分别按自己的显式前缀改写，避免只处理宿主机下载而遗漏容器内部解析。依赖版本及 `verification-metadata.xml` 的严格校验不变，缺项或 hash 不符仍失败；metadata 维护目标也使用同一 init。传输映射不写入源锁或 runtime 镜像环境，recipe 脚本变更仍计入定义 hash。

本地存在准确 Gradle 8.14.3 时，可显式运行无依赖、无网络的仓库生命周期合约；普通单元入口不要求安装 host Gradle：

```bash
NOVA_FIXTURE_TEST_GRADLE=/absolute/path/gradle-8.14.3/bin/gradle \
  python3 -m unittest discover -s docker/fixture-inputs/tests -p test_gradle_transport.py
```

镜像或平台变更由 fixture 维护者更新唯一源锁，保留上游 index、平台 manifest 和 config 的原始证据，再执行 provision 和实际消费者验证。仅修改 JSON 不能证明平台运行可用。单测入口：`bash docker/fixture-inputs/tests/run.sh`；machine、Paimon 和 benchmark reader 回归由各自原测试入口维护。

## OSS 独立闭包

阿里云环境显式选择 `iceberg-rest-oss`；它保留 REST、REST-MV、Spark，提供各自的官方 ECS RAM Role 凭证桥接依赖，以及 host 对象操作的 Python SDK 环境。它不供给 MinIO/mc 或它们的 Go 构建闭包。`iceberg-rest` 继续表示本地 MinIO 完整栈，`object-store` 仅表示 MinIO/mc；省略 consumer 不会自动推断部署方的对象存储选择。

```bash
docker/fixture-inputs/provision.sh --consumer iceberg-rest-oss --platform linux/amd64
docker/fixture-inputs/verify.sh --consumer iceberg-rest-oss --platform linux/amd64
```

当前 OSS host SDK 环境只声明 Ubuntu 26.04、原生 amd64、Python 3.14。provision 从唯一锁取得固定版本、URL、长度和 SHA256 的 Python 及 Java 完整依赖闭包，并在账户 store 中准备独立 Python venv。Python 构建工具也固定在闭包中；GCC、Python headers 等系统前置由 Linux 准备入口安装。REST/Spark 包装镜像复用已验证的派生基础镜像，运行不解析 Maven 版本范围或临时 pip install。BOM 中的 `python_environments` 记录环境定义、解释器版本、包和实际文件证明，verify 只核验已有内容，缺失或不同直接失败。

源锁中的 `derived_bases` 显式描述派生镜像之间的依赖，供给按依赖顺序消费准确 image ID；这与外部基础镜像的 `bases` 分开。相同 lock/platform 补充 OSS 闭包仍遵守原部分 BOM 合并与原子发布规则，不凭旧回执继承未核验内容。host SDK 的 store 路径定位实际安装环境，不因缓存目录不同改变机器的语义版本。

供给不申请 Bucket、不绑定 ECS 角色、不安装长期密钥。新机先绑定已有 RAM 角色，再通过 [Linux 准备入口](../../tools/dev/README.md) 提供 `--object-store-config <absolute-path>`，或按 [OSS 机器维护说明](../iceberg-rest/README.md#阿里云-oss) 使用五字段配置显式 start/check。机器 activation 固定规范化配置和已验证 SDK 的实际位置，worktree publication 继续绑定这一代；verify 成功本身不证明角色权限、IMDSv2 可达、服务健康或 SQL 可用。

普通建写使用 REST catalog；生产 Hadoop catalog 的条件 metadata 发布协议保持原状，OSS 的 S3 兼容接口不支持该条件写。MinIO IAM/STS、vended 和 Paimon 专项不由 OSS 闭包替代，OSS 模式下在副作用前拒绝。旧机器数据保留，TTL 和机器换代均不授予清理未知 writer 数据的权利。

## MinIO 官方源码与发布物

MinIO公开镜像已无法可靠取得，服务端采用官方 `RELEASE.2025-10-15T17-29-55Z` 的未修改源码构建；mc使用官方GitHub `RELEASE.2025-08-13T08-35-41Z` 的未修改源码构建。实际URL/完整源码commit/工具链manifest/长度和hash在唯一源锁中维护，版本允许更新。MinIO与mc属于derived_images；运行层消费准确image_id/定义回执，不依赖旧公开镜像或机器缓存。新增源码/客户端资产同时按声明SHA256核验，Go保留原go.mod/go.sum与校验，不下载另一个工具链或修改依赖。

```bash
docker/fixture-inputs/provision.sh --consumer object-store --platform linux/amd64 \
  --docker-pull-timeout-seconds 600
docker/fixture-inputs/verify.sh --consumer object-store --platform linux/amd64
```

这只准备MinIO/mc及必要的Go工具链归档/Alpine闭包；`verify --consumer iceberg-rest`仍要求完整REST/Spark输入，单独object-store BOM不能代表整栈ready。通用本地 MinIO 准备显式选择 `iceberg-rest`；阿里云 OSS profile 的 Linux 准备入口显式选择 `iceberg-rest-oss`。旧 `--image-source minio=...` / `minio-mc=...` 不再适用；传输覆盖应对`minio-runtime` 基础镜像，两个源码归档通过锁内官方URL取得。Go模块代理由构建recipe的GOPROXY设置选择，内容由原go.sum验证。

更新官方版本时一并核对源归档、客户端源码/许可证、Go工具链归档及recipe自报元数据，并实际构建、读写和验证权限。已有共享fixture须按原显式upgrade切换；不自动认领或删除旧卷/旧输入。研究和原生运行材料按任务约定在库外保留。

Go工具链通过阿里云镜像取得官方1.24.8归档，并核对Go官方SHA256；完整amd64/arm64归档均已核验。它在Alpine构建阶段使用，避免额外大型Go镜像的冷拉取依赖；GOTOOLCHAIN=local禁止构建时自动切换编译器。

失败后的重入会逐字节重新核验已下载artifact；匹配当前声明的缓存或保留staging可复用，校验不符重新下载。缓存不授予BOM/READY。mc及其LICENSE/CREDITS从同一官方commit源码归档构建/提取，减少分散raw下载地址依赖。
