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

# Fixture 推荐版本与缺失输入构建

`versions.json` 是软件推荐版本、平台变体、构建配方和下载输入的唯一声明来源。已有 fixture 直接使用保存配置；普通开发、测试和 CI 不运行环境 digest、BOM、版本、健康或能力预检。推荐清单不要求已有机器升级，也不证明机器当前的软件版本。

## 创建缺失输入

普通 fixture 创建入口复用本目录的 `prepare_inputs` builder。只有确实缺失的目标才按推荐构建；已有目标镜像直接复用，不检查整条构建依赖链。显式提前准备也可调用：

```bash
docker/fixture-inputs/provision.sh --consumer iceberg-rest --platform linux/amd64
# Only the OSS closure; no MinIO/mc inputs.
docker/fixture-inputs/provision.sh --consumer iceberg-rest-oss --platform linux/amd64
# Only object storage or HMS inputs.
docker/fixture-inputs/provision.sh --consumer object-store --platform linux/amd64
docker/fixture-inputs/provision.sh --consumer derived:hive-metastore --platform linux/amd64
```

省略 platform 时按本机平台选择 recipe，不额外核对全机 daemon/platform 环境。Iceberg recipe 声明 `linux/amd64` 和 `linux/arm64`；Paimon writer 和 HMS 只声明 amd64，不自动仿真。Paimon 产品读取仍暂停，迁移工具接口不恢复测试能力。默认 consumer 不保证适合所有平台，应明确选择所需闭包。

`--store` 指定构建缓存位置；本任务的临时脚本、构建日志和实验数据放在自己的 worktree，例如 `logs/experiments/<task>/`。机器长期持久控制状态与临时材料分开。

镜像使用推荐的可读版本 tag，不做环境 digest 锁定。实际创建输出记录所选目标的 image ID 和配置，供原 owner 操作准确目标；这些事实不是后续消费者的 BOM 准入证明。已有但损坏、不可访问、部分创建或状态不明的输入保留并报错，交人工维护，不自动修复或换版本。

## 下载完整性与来源

下载 artifact 的 URL、长度和 SHA-1/SHA-256 保留在推荐清单，Gradle 的 `verification-metadata.xml` 继续严格校验构建依赖。这些校验针对实际取得的构建输入，不是对运行环境的检查。缺失文件在独占临时位置下载，验证成功后原子移交；已有文件不符则报错，不覆盖原件。失败材料保留以便调查。

镜像来源可通过 `--image-source logical-name=transport-repository` 显式指定；版本仍取推荐清单。artifact 可使用 HTTPS 前缀映射：

```bash
docker/fixture-inputs/provision.sh --consumer iceberg-rest-oss --platform linux/amd64 \
  --artifact-url-prefix https://repo.maven.apache.org/maven2/=https://maven.aliyun.com/repository/public/ \
  --artifact-url-prefix https://files.pythonhosted.org/packages/=https://mirrors.aliyun.com/pypi/packages/ \
  --artifact-url-prefix https://plugins.gradle.org/m2/=https://maven.aliyun.com/repository/gradle-plugin/
```

映射两侧必须是以 `/` 结尾的 HTTPS 目录，不带 userinfo、query 或 fragment；重复或相互包含的原始前缀拒绝。仅改写一次，不递归映射或自动 fallback；artifact 内容校验不变。Linux profile 的 `fixture_artifact_url_prefixes` 传给首次创建，已有机器直接复用。

派生配方的 `bases` 和 `derived_bases` 分别声明外部和派生基础镜像。`artifact_url_prefix_build_arg` 允许该配方显式接收传输映射，不向未声明配方或运行环境注入。REST-MV 的 `transport.init.gradle` 仅改写匹配的原 Maven/Gradle 仓库地址，保持数量、顺序与未匹配地址；严格依赖校验不变。

## OSS 与源码构建

`iceberg-rest-oss` 包含 REST、REST-MV、Spark 的 ECS RAM Role 凭证桥和 host 对象操作 SDK；不包含 MinIO/mc。host SDK 配方声明 Ubuntu 26.04 amd64、Python 3.14 和固定依赖，在缺失时建立独立 venv；已有解释器路径直接使用，不遍历整个环境生成证明。已有目录但解释器缺失报错，交人工处理。

供给不申请 Bucket、不绑定 ECS 角色或安装长期密钥。新机使用已有 RAM 角色和 [Linux 准备入口](../../tools/dev/README.md)；对象存储配置和原 activation 路径遵守 [机器维护说明](../iceberg-rest/README.md)。OSS 不替代 MinIO IAM/STS、vended、Paimon 或 Hadoop 条件提交能力。

MinIO 与 mc 从清单所列的官方发布源码构建，保留原源码、许可证、Go 模块及 checksum 约束。Go 工具链归档按声明下载并校验，`GOTOOLCHAIN=local` 禁止构建期间自动换编译器。版本、源码和工具链的修改须由维护者审查并实际验证；已有机器仍只通过显式 upgrade 切换，不自动认领或删除旧数据卷。

## 手动诊断与测试

环境诊断只有显式入口，普通 CI 不调用：

```bash
docker/iceberg-rest/fixture-runtime.sh check
```

它报告推荐值、保存的实际事实、健康观察和错误；未知版本保持未知，不生成可运行资格，也不下载、构建或修复。固定全量 CI 机器可另行显式调用并留存报告。实际创建或消费失败时保留原错误，agent 交人工维护。

输入工具测试使用原入口：

```bash
bash docker/fixture-inputs/tests/run.sh
NOVA_FIXTURE_TEST_GRADLE=/absolute/path/gradle-8.14.3/bin/gradle \
  python3 -m unittest discover -s docker/fixture-inputs/tests -p test_gradle_transport.py
```

Gradle 合约只在显式提供 host Gradle 时运行；普通单元入口不要求安装它。首次空白机器构建、真实 Java 互操作和 SQL 正确性均须由实际消费者另行取证，不能用单元通过或诊断报告代替。现行裁决见 [ADR-0155](../../docs/adr/ADR-0155-fixture-provisioning-and-offline-verification.md)。
