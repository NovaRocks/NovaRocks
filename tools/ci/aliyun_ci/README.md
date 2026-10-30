# 本机按需远端 Rust 验证

开发者或Agent可以从开发机直接申请一次性ECS、执行完整原生Rust batch、下载校验后的材料并释放机器。local配置显式使用 `"execution_kind":"local-rust"` 并省略 `github`；不需要推送提交或配置GitHub。规格、磁盘与期限由部署配置明确提供。可选 `build_jobs` 显式传递 `CARGO_BUILD_JOBS`，只调整编译并发，不改变测试选择；省略时使用当前CPU观察。

local可选 `"cargo_registry":"rsproxy"`，通过Cargo标准source replacement将crates.io的下载定向到 `sparse+https://rsproxy.cn/index/`；省略或选择 `"crates-io"` 保持原配置行为。原Cargo.lock、包来源身份、版本与checksum及完整Rust选择保持不变，不增加下载回退或重试。local Cargo固定使用HTTP/1.1；编译并发仍由原 `build_jobs` 决定。GitHub与G19共享profile不受影响。

local服务显式提供 `LimitNOFILE=65536`，满足原生FE/BE的文件描述符前置条件；根服务实际限额保留在 `service_rlimit_nofile` 状态事实中。普通batch继承情况须按原PID读回，不能用systemd默认值代替实测。

选择rsproxy时，镜像必须在guest实际准备事实中报告 `local_rust_cargo_registries` 支持该值；旧镜像缺少此字段时只认crates-io，控制方会在分配前拒绝rsproxy。实例root仅在runner的Cargo_HOME中写本入口管理的 `config.toml`，未知已有配置、软链和优先级更高的旧 `config` 均拒绝且不覆盖。实际写入内容与readback SHA-256进入本次 `cargo-registry.json`，随原attempt归档取回。[Cargo source replacement](https://doc.rust-lang.org/cargo/reference/source-replacement.html)；[rsproxy官方配置](https://rsproxy.cn/)。

local另可显式选择 `"swap_gib":16`；省略或为0保持原行为，不关闭已有swap。16从原200GiB系统盘中使用16GiB，不增加机器RAM或磁盘规格，也不降低8 jobs或修改Rust参数/范围/重试。镜像guest必须实际报告 `local_rust_swap_gib` 支持16并具备dd/mkswap/swapon；旧镜像缺字段仅支持0，分配前拒绝16。

仅worker原生任务启动前由镜像root代码在固定 `/var/lib/novarocks-ci/private-swap/swapfile` 独占创建root0700目录与root0600文件，检查ext4/xfs磁盘空间、用dd实际写满并核对精确size/已分配blocks，再执行mkswap/swapon并核验 `/proc/swaps` 和meminfo。未知已有目录/文件、软链、空间不足或激活失败均明确失败，不回退；不写fstab或/etc/sysctl配置。仅显式16GiB托管swap通过激活及内核文件核验后，将本台一次性worker的运行时 `/proc/sys/vm/swappiness` 设为Linux标准默认值60并独立读回，保留before/requested/after；0模式不读写该策略，builder不设置。该实例运行时策略随机器释放消失，写入或读回失败保留已激活swap与失败阶段并明确停止原任务启动。配置、命令原退出和内核读回进入 `swap.json` 及原状态/归档，私有swap文件禁止归档。builder不创建或启用swap且捕获前拒绝私有swap目录；worker释放时随原磁盘回收。Swap可为编译提供磁盘后备容量，SIGKILL或swappiness为0本身仍不证明OOM；60不保证对本工作负载最优，Linux swappiness语义见[官方文档](https://docs.kernel.org/admin-guide/sysctl/vm.html#swappiness)。实际Linux激活与内核读回须由真实guest验证，macOS单测不替代。

原batch启动与原进程观察结束后，root分别保存 `resources-start.json` / `resources-end.json`，记录实际限额、原cgroup及其祖先、内核内存/Swap/PSI和OOM计数，结束时限时读取OOM相关日志。运行时0600保护副本的实际摘要随原batch结果返回，可与取回归档核对。退出后的观察不重读可能复用的PID；缺失、权限或超时记unknown，空日志不证明没有OOM。这些观察不改变原退出码或测试选择，也不证明清理完成。

```sh
cd tools/ci
# 使用安装了 requirements.lock 的 operator venv；标准阿里云凭证链读取本机登录。
python3 -m aliyun_ci --config /absolute/local-config.json local-run \
  --source /absolute/NovaRocks --sha FULL_LOCAL_COMMIT --image-id IMAGE_ID \
  --record /absolute/new/run.json
python3 -m aliyun_ci --config /absolute/local-config.json local-status --record /absolute/new/run.json
python3 -m aliyun_ci --config /absolute/local-config.json local-cancel --record /absolute/new/run.json
python3 -m aliyun_ci --config /absolute/local-config.json local-collect \
  --record /absolute/new/run.json --destination /absolute/new/collected
python3 -m aliyun_ci --config /absolute/local-config.json teardown --plan /absolute/new/run.json
```

工作树必须clean，所选提交必须是准确HEAD；源码以自包含Git bundle交付，不带本机Git配置或登录信息。`local-run --prepare-only`只生成本机源码与计划，供离线预览。默认执行原 `test-run.py batch` 的完整常规nextest和独立doctest，不接受过滤或任意shell。

run记录在云副作用之前保存；断连后 `local-status` 仅观察原命令，不能重新运行 `local-run` 当作续跑。Ctrl-C与 `local-cancel` 向原batch发送SIGTERM并观察实际退出；强制终止或未知清理如实记录。完成后原生日志/index/JUnit随逐成员校验归档取回，测试、材料、机器释放和遗留资源分别报告。自动释放不依赖本机控制方存活；未上传材料可能在释放时丢失。

local `deploy render/plan/apply/check` 仅管理独立安全组和实例执行角色，复用明确VPC/vSwitch/Bucket；不创建GitHub OIDC或控制角色。镜像仍由显式image命令制作。OAuth预签名使用官方CLI缓存中与实际签名身份匹配的STS到期事实；未知或短于run期限时在创建前拒绝，需重新登录/续期。其它临时凭证仍须提供真实 `ALIYUN_CI_STS_NOT_AFTER`，不可填写预计时长。

本地与云端材料由运行者保管，不自动过期；机器释放不删除OSS原证据和镜像/快照。该入口只证明所执行的Rust范围，SQL/System与正式准入仍须相应原生环境和验收。

## GitHub 手动 Rust CI（后续接入）

首版入口仅接受完整 SHA，执行原生 `test-run.py batch`；不增加过滤、重试或跳过表。它不授予正式准入、完整 E1 e2e 或 E2 服务资格。维护 owner 为 CI 维护者。

```sh
cd tools/ci
python3 -m aliyun_ci --config aliyun_ci/config.example.json validate-config
python3 -m aliyun_ci --config aliyun_ci/config.example.json plan-run --run-id 42 --now 2026-10-28T00:00:00Z --nonce example
python3 -m unittest discover -s aliyun_ci/tests -t . -v
```

示例不包含真实部署值。150 分钟工作预算以已有托管 job 约 103 分钟为起点，首次云运行后按实测调整；200 GiB 系统盘为初值。watcher 每 10 秒观察，最多等 120 秒取消，为云平台 5 分钟预告留出上传时间。这些均是配置，不是产品限制。

平台测试在本地使用纯逻辑、记录的 API 响应及本地 HTTP/短命子进程验证请求和实际退出；不调用云 API。修改平台或 workflow 时运行，由具名 CI stage 消费。真实镜像、Runner 上线、自动释放和 Rust 闭环须独立验收；回放不证明云部署完成。云资源只在维护者审阅部署、权限和预算并明确授权后创建。

## 镜像与执行边界

基础是部署地域的官方 Ubuntu 26.04 amd64 公共镜像，准确 ImageId 必须由维护者填入 `base_image_id`。制作时开发机按准备源码中锁定的nextest归档摘要校验并不可变暂存OSS，builder只将准确文件交给G19原缓存；原安装器仍核验归档和可执行文件，不依赖builder直连GitHub。镜像捕获前清除云助手中带限时签名能力的历史脚本并读回零残留。

镜像提供 G19 工具模式、平台 venv、引导/上传/watcher、Runner 原生依赖与只读检查定义，不提供产品 fixture、Docker、源码 checkout、target、Runner 注册或凭证。Runner 包每日从官方 release 校验后同步 OSS，启动时按准确版本与 SHA-256 下载；镜像不会随 Runner 发布自动失效。账户 HOME 是 `/var/lib/novarocks-ci/runner`，Runner runtime 是 `agent/` 子目录。

已登记的候选镜像可以直接启动。start 核对镜像身份和实例的自动释放时间后才发布 JIT；机器上的只读检查失败就结束，清理由 teardown 执行。`promote-image` 记录已有的工具检查通过事实，不再是首次启动门，也不自动修改仓库变量。

Runner UID 在服务启动前被禁止访问阿里云 IMDS；root bootstrap 和 watcher 保留访问。该规则的实际内核行为、systemd 与真实镜像仍须 Linux P05a 验证，macOS 单测不能替代。root-only JIT 在降权执行 Runner 前删除。test job 只得到本次归档的预签名写入 URL，权限只有 contents:read。

## 部署前需要落实的值

在任何计费操作前审阅配置、ROS 模板和预算：

- 账号、地域、官方基础 ImageId、VPC、候选规格/交换机、已有 OSS bucket 与部署前缀。
- GitHub App 需要本仓库 Runner 管理与组织 runner group 只读权限；生成安装 token 的 secret 仅供控制 job。
- environment `aliyun-ci` 限制受信 main 分支；runner group 仅开放本仓库、允许公开仓库、只允许 `NovaRocks/NovaRocks/.github/workflows/aliyun-ci.yml@refs/heads/main`。设置必须由维护者落实，start 会读取核实。
- 仓库变量 `ALIYUN_CI_CONFIG_JSON`、`ALIYUN_CI_ROLE_ARN`、`ALIYUN_CI_OIDC_PROVIDER_ARN`、`ALIYUN_CI_APP_ID`；App private key 存于 `ALIYUN_CI_APP_PRIVATE_KEY`。镜像验证后显式设置 `ALIYUN_CI_IMAGE_ID`。
- ROS 额外参数需要 `VpcId` 与 GitHub OIDC issuer 的实际证书指纹 `GitHubOidcFingerprint`；不猜测指纹。可复用交换机或按模板显式新建。

预算范围包括抢占实例、系统盘、公网出站流量（配置 `internet_max_bandwidth_out_mbps`，样例5Mbps、按流量计费）、自定义镜像/快照和 OSS 对象。没有按量实例回退；自动释放与 janitor 不能保证零账单。OSS 没有无条件到期删除；未解决失败和正式引用由维护者负责保留。

## 本机运维入口

以下命令只展示用法，须在各自授权和前置值落实后执行。所有命令从 `tools/ci` 运行，用安装了 hash 锁文件的 venv Python。SDK 采用标准阿里云凭证链，GitHub token 从显式环境变量 `GH_TOKEN` 读取，未记录 token 内容。

```sh
python3 -m venv /absolute/operator-venv
/absolute/operator-venv/bin/pip install --require-hashes -r aliyun_ci/requirements.lock

python3 -m aliyun_ci --config /absolute/config.json deploy render
python3 -m aliyun_ci --config /absolute/config.json deploy plan --stack-name novarocks-ci --create --parameters /absolute/ros-parameters.json
python3 -m aliyun_ci --config /absolute/config.json deploy inspect --change-set-id CHANGE_SET_ID
# 审阅 inspect 返回的 change_set 与 reviewed_digest 后应用同一变更集。
python3 -m aliyun_ci --config /absolute/config.json deploy apply --change-set-id CHANGE_SET_ID --reviewed-digest DIGEST
python3 -m aliyun_ci --config /absolute/config.json deploy check --stack-id STACK_ID --variables /absolute/repository-variables.json

python3 -m aliyun_ci --config /absolute/config.json runner-sync
# source.tar 必须来自仓库根的 git archive --format=tar 完整SHA；CLI核对准确内容。
python3 -m aliyun_ci --config /absolute/config.json --output /absolute/new/image.json image --preparation-sha FULL_SHA --source-archive /absolute/source.tar
python3 -m aliyun_ci --config /absolute/config.json --output /absolute/new/start.json start --sha FULL_SHA --image-id IMAGE_ID
python3 -m aliyun_ci --config /absolute/config.json teardown --plan /absolute/new/start.json
python3 -m aliyun_ci --config /absolute/config.json janitor
python3 -m aliyun_ci --config /absolute/config.json promote-image --image-id IMAGE_ID --summary-key DEPLOYMENT_PREFIX/runs/RUN_ID/ATTEMPT/summary.json
```

本机 `start` 自动生成 `local-UTC-随机` 身份，仅验证开机和 Runner 上线，不派发测试；其SHA核对本地准确准备提交，允许尚未发布的候选代码。真正GitHub测试运行仍要求远端SHA及其工具profile存在。首个云副作用前，非秘密 plan 输出到 stderr；有 `--output` 时也独占保存同名 `.plan.json`，即使 start 失败也能用 `teardown --plan` 找回。start 正常输出再保存完整结果；不依赖它才能按原标签回收。

临时 STS 用于预签名时，必须通过非秘密 `ALIYUN_CI_STS_NOT_AFTER` 给出实际凭证的 UTC 到期时间；未知或短于请求 deadline 时在创建实例前拒绝。工作流在请求12小时 STS 前记录保守的有效期上界，RAM role 同样配置12小时上限。SDK 标准公开模型没有到期字段，不能根据预签名参数伪造凭证有效期。长期凭证仍由标准链提供，执行机永远不获得它。

## 工作流与交接

合入 main 之后，手动入口为 Actions 的 Run workflow，或：

```sh
gh workflow run aliyun-ci.yml --ref main -f sha=FULL_SHA -f image_id=IMAGE_ID -f mode=rust
```

`start → test → teardown` 在同一 workflow 中。`mode=rust` 只执行完整原 batch，不加过滤；`probe` 用于取消/自动释放演练。只有 probe 且 start 成功核实自动释放后才允许 `drill_skip_teardown=true`；start 失败仍回收。每次 GitHub run/attempt 独立命名，不自动重试。

OSS `runs/<run>/<attempt>/` 下保留 final/interrupted 归档和 summary。teardown 先释放算力，再读取并核对归档成员摘要；summary 分别保留原生 Rust index、材料完整性、算力终态和遗留资源。缺材料照样释放，但明确记缺口。GitHub artifact 仅作方便副本；90天平台保留不能替代 OSS 的长期保留责任。janitor 每小时、Runner 同步每日，各自保留异常材料。

GitHub 原生 `queue: max` 保留等待请求（平台上限100，超出由 GitHub 拒绝）。固定 actionlint 1.7.12 尚未识别此已发布语法，上游问题 #657/#746；验证保留原始诊断后仅排除 `unexpected key "queue" for "concurrency" section`，其余错误仍失败，不补丁第三方、不退回会替换等待者的默认队列。
