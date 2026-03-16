---
name: sql-test
description: 在 NovaRocks 仓库中运行 StarRocks `dev/test` SQL-tester，严格绑定 `dev/fe/conf/fe.conf` 与 `novarocks.toml` 的端口约束，禁止使用系统其他 StarRocks 实例。
---

# SQL Test（StarRocks `dev/test` + NovaRocks）

目标：在本仓库内稳定执行 SQL-tester，复用已有 SQL case 做验证，并快速归因失败。

## 1) 硬约束（必须遵守）

- StarRocks FE 只能使用 `dev/fe` 启动的实例（“归属于自己”的 FE），可任意启停。
- FE 端口必须来自 `dev/fe/conf/fe.conf`（`query_port`、`http_port`），禁止写死固定端口（例如 `9030`）。
- 禁止查找、复用或依赖系统中其他 StarRocks server（包括其他目录、历史残留进程、随机端口实例）。
- 不要执行“全系统扫端口找 StarRocks”的动作；只验证 `dev/fe` 对应端口与进程。
- NovaRocks 必须严格使用 `novarocks.toml` 的 `[server]` 端口：
  - `heartbeat_port`
  - `be_port`
  - `brpc_port`
  - `http_port`
  - `starlet_port`
- NovaRocks 端口号不可修改；`novarocks.toml` 其他非端口配置可修改。
- 执行 SQL-tester 前必须确保本机 `127.0.0.1/localhost` 请求不走代理（否则 `/_stream_load` 可能出现空响应或 `502`）。
- 在 Codex `exec_command` 环境中，`nohup ... &` / 一次性后台启动的长驻进程可能会在命令结束后被会话回收；不要把这种方式当作可靠守护进程方案。
- 若需要稳定保活 FE / NovaRocks，优先复用用户已手动启动的实例，或在持久 PTY 会话中前台运行并保持会话存活。
- 若 StarRocks FE / NovaRocks 启停遇到无法解决的问题（重试后仍失败），允许中断并直接报告阻塞原因。

## 2) 启动前检查

在仓库根目录执行：

```bash
ls -la dev/test
ls -la dev/fe/conf/fe.conf novarocks.toml
ls -la dev/test/conf/sr.conf dev/test/run.py dev/test/requirements.txt
```

检查要求：

- `dev/test/conf/sr.conf` 中 `cluster.port` 必须等于 `dev/fe/conf/fe.conf` 的 `query_port`
- `dev/test/conf/sr.conf` 中 `cluster.http_port` 必须等于 `dev/fe/conf/fe.conf` 的 `http_port`
- 仅检查 FE query 端口对应进程，且命令行/日志路径必须包含 `dev/fe`
- NovaRocks 监听端口必须与 `novarocks.toml` 一致

推荐快速核对命令：

```bash
grep -E "^(query_port|http_port)\\s*=" dev/fe/conf/fe.conf
grep -E "^(\\s*port|\\s*http_port)\\s*=" dev/test/conf/sr.conf
grep -E "heartbeat_port|be_port|brpc_port|http_port|starlet_port" novarocks.toml
```

## 3) 代理环境变量（stream load 必须）

在当前 shell 明确禁用本机代理：

```bash
export NO_PROXY=127.0.0.1,localhost
export no_proxy=127.0.0.1,localhost
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy
```

若不想改当前 shell，可在命令前临时加：

```bash
NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost \
HTTP_PROXY= HTTPS_PROXY= ALL_PROXY= http_proxy= https_proxy= all_proxy= \
<your_command>
```

## 4) Python 与虚拟环境

SQL-tester 依赖 `nose`。推荐固定 Python 3.11 并使用独立 venv：

```bash
cd dev/test
python3.11 -m venv .venv311
.venv311/bin/python -m pip install -r requirements.txt
```

复用已有 venv：

```bash
cd dev/test
.venv311/bin/python --version
.venv311/bin/pip --version
```

如 venv 被其他仓库路径污染，直接重建：

```bash
cd dev/test
rm -rf .venv311
python3.11 -m venv .venv311
.venv311/bin/python -m pip install -r requirements.txt
```

## 5) 服务启停与连通性

StarRocks FE（仅 `dev/fe`）：

```bash
cd dev/fe
bin/start_fe.sh --daemon
bin/stop_fe.sh
```

NovaRocks（端口严格来自 `novarocks.toml`）：

```bash
./build.sh run -- start --config ./novarocks.toml
./build.sh run -- stop
```

注意：

- 在 Codex 工具环境中，不要假设通过一次性 `exec_command` 触发的 `start_fe.sh --daemon`、`./build.sh run -- start`、`nohup ... &` 一定能持续存活。
- 若需要 agent 自己托管服务，优先使用持久 PTY 会话并持续保留该 session；否则优先让用户手动启动服务后再做 SQL 验证。
- 每次启服后都要立即做端口与探针校验，不要仅凭启动命令返回成功就继续跑 case。

MySQL 探针（端口取自 `fe.conf`）：

```bash
mysql -h 127.0.0.1 -P<query_port_from_fe_conf> -u root -e "select 1"
```

探针失败时先修复服务，不要继续跑 SQL case。

## 6) 如何查找并执行现成 SQL case

先在 `dev/test/sql` 下定位已有 case，再按目录执行，不要为常规验证重新造 case。

### 给定 case 名快速定位 T 文件路径

T 文件位于 `dev/test/sql/*/T/` 下。给定 case 名（完整名或关键词），用以下命令找到完整路径：

```bash
# 精确查找（case 名就是文件夹名或文件名）
find dev/test/sql -path "*/T/*" -name "*test_bucket_shuffle_right_join*"

# 模糊查找（只记得关键词）
find dev/test/sql -path "*/T/*" -name "*bucket_shuffle*"
```

输出示例：
```
dev/test/sql/test_bucket_shuffle_right_join/T/test_bucket_shuffle_right_join
```

从路径推断 `-d` 参数（**重要**：validate 模式读 R 文件，不是 T 文件）：
- **整个文件夹**（跑该 case 下所有子 case）：`-d sql/test_bucket_shuffle_right_join/`（父目录，含 T/ 和 R/）
- **单个文件**（精确跑某个 case）：`-d sql/test_bucket_shuffle_right_join/R/test_bucket_shuffle_right_join`

> ⚠️ 禁止用 `-d sql/.../T/` 或 `-d sql/.../T/xxx`：validate 模式只使用 R 文件，给 T 路径会导致 `case num: 0`（零 case，静默跳过）。

通用检索方式（按内容搜索）：

```bash
rg -n "<keyword>" dev/test/sql -S
```

常见调用方式：

- 跑一个目录（父目录，包含 T/ 和 R/）：

```bash
cd dev/test
.venv311/bin/python run.py \
  -d sql/test_profile/ \
  --skip_reruns \
  -v -c 1 -t 300
```

- 跑一个具体 case 文件（指向 R/ 文件）：

```bash
cd dev/test
.venv311/bin/python run.py \
  -d sql/test_profile/R/test_profile_analysis \
  --skip_reruns \
  -v -c 1 -t 300
```

- 在目录下按关键字缩小范围：

```bash
cd dev/test
.venv311/bin/python run.py \
  -d sql/test_profile/ \
  --case_filter profile \
  --skip_reruns \
  -v -c 1 -t 300
```

建议默认策略：

- 首轮先跑单目录或单文件
- 单并发：`-c 1`
- 禁重跑：`--skip_reruns`
- 超时显式给出：`-t 300`
- 若目标已经很明确，优先直接复用现有 case 路径，不要先大范围扫全量 suite

## 7) 失败分流（fail-fast）

- `case num: 0`（零 case 静默跳过）
  - 原因：给了 T 路径但 validate 模式读 R 文件。改用**父目录**（`-d sql/test_xxx/`）或 R 路径（`-d sql/test_xxx/R/xxx`）
- `ModuleNotFoundError: no module named ...`
  - 在 `dev/test/.venv311` 内重装 `requirements.txt`
- `Can't connect to MySQL server`
  - 先核对 `dev/test/conf/sr.conf` 与 `dev/fe/conf/fe.conf` 是否一致，再做 `mysql` 探针
- `TTransportException: ... Read timed out`
  - 立即重试一次；若连续失败，检查 FE 与 NovaRocks 日志并报告
- `shell result: code: 0, stdout: ''` 或 `curl` 出现 `502 Bad Gateway`
  - 优先检查代理变量，按“代理环境变量”章节重新设置后重跑
- SQL diff / 语义错误
  - 记录实际结果与期望差异，归类为语义问题

若启停问题不可解，按硬约束中断并报告。

## 8) 回报格式（给用户）

每次执行后输出：

1. FE 来源与端口（明确来自 `dev/fe/conf/fe.conf`）
2. NovaRocks 端口校验结果（明确来自 `novarocks.toml` 且未改端口）
3. Python 与 venv
4. 执行命令
5. 结果（PASS/FAIL）
6. 若 FAIL：归类 + 下一步动作（或中断原因）

示例：

- FE: `dev/fe/conf/fe.conf query_port=9031 http_port=8031`
- NovaRocks ports: `heartbeat=9051, be=9061, brpc=8061, http=8041, starlet=9071`（unchanged）
- Python: `dev/test/.venv311/bin/python (3.11.x)`
- Command: `run.py -d sql/test_profile/T/test_profile_analysis --skip_reruns -v -c 1 -t 300`
- Result: `PASS`

## 9) 禁止事项

- 不要扫描系统范围端口去“找可用 StarRocks”。
- 不要接入任何非 `dev/fe` 的 FE/BE。
- 不要修改 `novarocks.toml` 的端口号。
- 启停阻塞不可解时，不要继续高风险操作；直接中断并报告。
