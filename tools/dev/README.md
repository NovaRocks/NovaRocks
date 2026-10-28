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

# Linux 开发环境

`linux-env.sh` 从版本化输入恢复 Ubuntu 26.04 原生 amd64 开发节点。系统包由发行版包源维护；Rust 版本读取仓库 `rust-toolchain.toml`，rustup/nextest 下载内容由 `linux-env.inputs.json` 的 SHA-256 固定。来源配置在 `profiles/aliyun-ubuntu26-amd64.json`，不依赖某台实例、旧缓存或开发者电脑传输文件。

## 从基础系统开始

可信源码是引导输入。只有 SSH 和发行版基础系统时，先按正常发行版源安装取得源码所需的种子，再检出选择的完整源码 SHA：

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates git
install_user=root  # Or choose an existing development account.
# Clone the project from its authorized repository and checkout the chosen full commit.
# Run the following commands from that checkout.
object_store_config=/absolute/path/object-store.json
sudo tools/dev/linux-env.sh prepare --user "$install_user" \
  --profile tools/dev/profiles/aliyun-ubuntu26-amd64.json --fixture iceberg-rest \
  --object-store-config "$object_store_config"
```

`install_user` 必须是现有系统账户；工具从 passwd 记录取得 home，不猜 `SUDO_USER`。prepare 的薄 shell 在 Python 主体运行前补齐 Python、CA、curl、git，随后准备 native 编译依赖、libclang、protoc、OpenSSL、Rust、nextest、Docker/Compose/Buildx 和 locale。调用者须具备系统安装权限，源码目录须可由目标账户使用。

阿里云 profile 明确选择 OSS。首次部署时先在 ECS 控制台为新实例绑定已有 RAM 角色，再创建上述独立非秘密配置文件，内容如下；不要把临时凭证或长期 AccessKey 加入其中：

```json
{
  "kind": "aliyun-oss",
  "region": "cn-beijing",
  "bucket": "YOUR_TEST_BUCKET",
  "role_name": "YOUR_ECS_ROLE",
  "root_prefix": "novarocks-development"
}
```

`--object-store-config` 必须是绝对路径。字段与前缀由 [OSS 操作模块](../../docker/iceberg-rest/oss_ops.py) 的同一校验作者解析；缺失或错误时直接失败，不回退到 MinIO。准备在安装账户的 `~/.cache/novarocks/linux-env/` 原子保存规范化的 `object-store.json` 和含 `object_store` 的 `fixture-machine.json`，环境变量 `NOVA_ENV_OBJECT_STORE_CONFIG` 指向前者。它们是本次期望配置，不能替代各代 runtime publication 的不可变输入快照。

准备 fixture 使用原 `provision -> verify -> fixture-runtime start -> up`；OSS profile 请求 `iceberg-rest-oss` 闭包，不供给 MinIO/mc。机器 start/check 显式传入上述 `fixture-machine.json`。SDK 及其固定依赖由 fixture provision 准备，运行和 check 不执行 pip 安装。没有 `object_store_kind` 的通用 profile 保持本地 MinIO/`iceberg-rest` 路径；向此类 profile 传入 OSS 配置会明确失败，避免参数被忽略。

已有机器版本不同会要求显式维护；先确认资源、旧数据和维护窗口，再按 [fixture 文档](../../docker/iceberg-rest/README.md) 执行原 upgrade。不会自动清卷、删除云端旧数据、复制旧 READY 或创建第二套共享项目。释放旧 ECS 后，在新实例绑定同一角色，取得同一源码和非秘密配置，再执行相同准备命令；旧机器的磁盘、venv、BOM、READY 和 target 都不是恢复输入。

## 日常使用与检查

在安装账户的新登录 shell 中执行：

```bash
eval "$(tools/dev/linux-env.sh env --user "$install_user" \
  --profile tools/dev/profiles/aliyun-ubuntu26-amd64.json --fixture iceberg-rest \
  --object-store-config "$object_store_config")"
tools/dev/linux-env.sh check --user "$install_user" \
  --profile tools/dev/profiles/aliyun-ubuntu26-amd64.json --fixture iceberg-rest \
  --object-store-config "$object_store_config"
```

登录环境按账户实际 shell 与 Bash 启动文件优先级接线，保留原内容。SSH 非交互命令不保证读取登录配置，因此先显式求值 `env` 的输出。环境包含账户自己的工具目录和来源设置；专用配置按本机可用 CPU 导出 Cargo 并发，显式 `CARGO_BUILD_JOBS` 优先。通用开发入口的半核默认不变。`env` 只输出环境，不能证明 fixture ready。

`fixture-runtime.sh check --store <store> --config <machine-config>` 由原机器 owner 只读核对当前源码/BOM 要求的版本与实际健康实例，避免仅凭旧 machine 和旧 publication 相互一致而误报。检查使用既有维护锁，不建立缺失状态。OSS check 同时比较准备时保存的两份配置与本次输入；缺失或不同只报错，不重写配置。

check 只观察本机实际工具、daemon、BOM、机器与 worktree publication，不访问外部下载源，不下载、构建、安装或重启。缺 Python 时 check/env 直接失败，不运行 apt。权限不足时使用具有观察权限的账户，不能用 root 的成功代替普通账户可用。修改 docker 组成员后须开启新会话。

相同配置再次 prepare 应保留健康服务和 machine generation。Docker 配置只合并受管字段，保留不相关配置；配置相同时不重启。有活动容器且配置必须改变时先报告维护冲突，操作员安排窗口后重试。

## 来源与失败诊断

复制 profile 到自己的受管配置位置再修改来源，无需改锁定内容。`docker_registry_mirrors` 改 Docker Hub 传输路径；`fixture_image_sources` 按逻辑输入指定 repository；rustup 的 dist/update URL、Cargo registry index 及 `tool_download_urls` 可以调整下载位置。镜像 digest、工具 SHA-256 和 Maven 内容校验保持不变。第三方镜像站可用性由部署者维护；来源失败不是 Docker daemon 必然故障。401/403 表示先检查该 registry 的访问权限或同内容镜像来源；超时要检查路由和传输期限。某个锁定对象不可取得时明确停在供给阶段，不换任意 tag、不从旧机器复制 READY 冒充恢复。

`download_timeout_seconds` 同时传给 curl 和原 fixture owner 的单次镜像传输期限，避免冷拉取大镜像仍落入供给工具的较短默认值；不为整个构建/维护进程另加会丢弃子进程的外层期限。下载校验失败不安装；重新执行会观察真实状态并续做未满足阶段。磁盘不足保留材料并失败，不 prune 镜像、删除源码/旧卷或诊断。失败时保留真实命令、退出与阶段日志，secret 不入库；不要将一次安装回执当作成功权威。

## 验证与维护

```bash
bash -n tools/dev/linux-env.sh
python3 -m unittest discover -s tools/dev/tests -p 'test_*.py' -v
```

这些标准库功能测试维护安装、账户与幂等合同；命令适配器不证明冷系统安装。实际部署还应检查 native 编译、nextest、Docker 拉取/构建/Compose 网络、fixture verify/status/attach 和最小 Spark SQL。独立空白机器必须从无旧镜像、BOM/READY 和 target 开始，成功后用相同配置复执行，对比 daemon 启动时间和 machine generation。已安装机器复验只证明当前条件。

实际部署保留机器规格、候选源码、非秘密 profile、命令和真实退出到实例外；旧机释放前核对材料可恢复。部署者负责网络与实例生命周期，本工具不申请/释放实例、不注册 GitHub Runner；未来 CI 平台可消费同一准备入口。Linux 扩展测试矩阵和整工作区构建不属于环境准备。
