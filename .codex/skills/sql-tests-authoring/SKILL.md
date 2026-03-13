---
name: sql-tests-authoring
description: 在 NovaRocks 仓库中新增、迁移或维护 `sql-tests` 用例时使用。适用于把 `dev/test` case 迁到 `sql-tests`、设计一个 `.sql` 文件承载完整状态化 case、编写聚合 `.result`、为异步 MV/schema change/Iceberg 场景选择合适 metadata 注解，并执行定向/全量校验。
---

# SQL Tests Authoring

目标：在 `sql-tests/` 下新增或迁移用例时，保持语义覆盖完整、结果文件稳定、失败归因清晰，并避免“为了跑通测试而记错结果”。

如果任务还需要运行 `dev/test` 原始 case 或校验 FE/NovaRocks 端口约束，同时加载 [$sql-test](../sql-test/SKILL.md)。

## 1. 先定边界，再写 case

- `sql-tests` 的基本单位是“一个 `.sql` 文件 = 一个完整 case”。
- 文件内允许多个 `-- query N` step；这些 step 共用同一前后状态。
- 适合放进同一个文件的场景：
  - 先建表 / 建 MV / 插数 / refresh，再验证多个 query。
  - 同一状态链上逐步验证 rewrite、refresh、inactive、repair、show status。
- 不要为了追求“单条 SQL 一个文件”把强相关状态拆散。

### suite 归类原则

- 同一执行语义、同一对象类型放在同一 suite。
- 外表 / Iceberg / 外部 catalog 相关的 MV case，单独建 suite，不和内表 MV 混放。
- 本次经验：
  - 内表 MV 放 `sql-tests/materialized-view`
  - `mv on iceberg` 放 `sql-tests/mv-on-iceberg`

## 2. 从 `dev/test` 迁移时保语义，不保形状

- 不要求和 `dev/test` 原文件一一对应。
- 允许重组 step、合并 case、改 suite 边界。
- 但原测试覆盖的测试点不能缺失。
- 迁移前先写清楚覆盖点列表，再开始改写 SQL。

### 必须保留的内容

- 目标功能点：rewrite / refresh / inactive / swap / partition / show / privilege / error path。
- 关键状态转换：创建前、创建后、增量变更后、repair/active 后。
- 关键兼容性点：StarRocks FE 下发协议、物理 schema、同步/异步可见性、外表行为。

### 不要机械照抄

- `dev/test` 中为了 Python tester 写的包装、sleep、输出格式，不一定要原样保留。
- 如果原用例语义依赖动态行为，不要擅自改成静态配置来“简化”。
- 例如：原 case 是动态分区 / 异步 refresh / recreate 语义，就保留动态语义。

## 3. 每个 SQL 文件必须写清测试意图

- 文件顶部先写总注释，说明这个 case 在测什么。
- 每个非直观 step 前补 1-2 行注释，解释为什么要等待、为什么要断言某个子串、为什么跳过结果快照。

推荐格式：

```sql
-- Test Objective:
-- 1. Validate sync MV rewrite after build completion.
-- 2. Cover visibility lag between SHOW ALTER finished and _SYNC_MV_ queryability.

-- query 1
...

-- query 5
-- Wait for the previous sync MV job to settle before querying the physical MV.
-- @retry_count=60
-- @retry_interval_ms=1000
...
```

## 4. 结果文件规则

- 一个 `.sql` 文件只对应一个同名 `.result`。
- 多 step case 的 `.result` 使用 `-- query N` 分段。
- 只记录“确定且正确”的成功结果。
- 不允许把明显的运行时错误文本塞进 `.result` 里伪装成预期输出。
- 如果查询本应成功但实际报错，先停下来修 bug。

示例：

```text
-- query 1
col1\tcol2
1\ta

-- query 2
count
3
```

### 何时不用 `.result` 分段

- `@expect_error=...`：只校验错误，不写结果分段。
- `@skip_result_check=true`：执行但不快照。
- 纯文本断言 step：只用 `@result_contains` / `@result_not_contains` 等。

## 5. 优先选稳定断言，不要快照不稳定文本

### 适合快照完整结果的场景

- 普通 `SELECT` 行集结果稳定。
- `SHOW` 输出列少且文本稳定。
- 物理结果是关键语义本身。

### 适合文本断言的场景

- `EXPLAIN`
- `SHOW CREATE MATERIALIZED VIEW`
- `SHOW ALTER MATERIALIZED VIEW`
- 带动态 `QUERY_ID`、job id、耗时、warehouse、默认属性扩展的输出

优先使用：

- `@result_contains=...`
- `@result_contains_any=...`
- `@result_not_contains=...`

必要时再用：

- `@skip_result_check=true`

### `SHOW CREATE` 经验

- 以当前 NovaRocks 实际输出为准，不强行追旧文本。
- 但不要把整段长 DDL 全量快照到不稳定文本上。
- 对稳定的关键片段做文本断言更稳。

## 6. 异步场景要用 retry，不要裸 `sleep`

MV / schema change / show status 经常有“状态 finished 了，但对象短时间还不可见”的窗口。

优先做法：

- 对查询 step 加：
  - `@retry_count=<N>`
  - `@retry_interval_ms=<ms>`
- 把 retry 放在真正依赖可见性的 step 上。

只在必要时保留短 `SELECT sleep(...)`，例如：

- 触发异步 job 后给 FE 一个最短调度窗口。
- 原逻辑明确依赖前一个 job 被调度。

经验：

- `SHOW ALTER ... FINISHED` 不代表物理对象立刻可查询。
- `_SYNC_MV_`、`SHOW MATERIALIZED VIEWS`、refresh status 都可能需要 retry。

## 7. 常用 metadata 选择

- `@expect_error=...`
  - 只用于“本来就应该失败”的稳定错误路径。
- `@result_contains=...`
  - 适合 `EXPLAIN` / `SHOW CREATE` / 状态文本。
- `@result_contains_any=...`
  - 适合多种合法文案之一。
- `@result_not_contains=...`
  - 排除错误 rewrite / 错误 plan 片段。
- `@skip_result_check=true`
  - 适合动态 job 输出、纯副作用语句、只做文本断言的 step。
- `@retry_count` + `@retry_interval_ms`
  - 适合异步状态收敛。
- `@order_sensitive=true`
  - 只有顺序本身是语义的一部分时才开。
- `@float_epsilon=<v>`
  - 浮点比较需要容差时再开。
- `@db=<name>`
  - 跨库场景才用。

## 8. 出现失败时的处理原则

先判断是哪一类：

- 用例问题：
  - 断言选错了。
  - 输出本来就不稳定，却用了全量快照。
  - suite 边界放错了。
- 真 bug：
  - 本应成功却报错。
  - rewrite 命中错了。
  - 结果值错了。
  - StarRocks FE 下发的协议语义没遵守。

### 必须停下来修 bug 的信号

- 为了通过测试，你打算把明显报错文本写进 `.result`。
- 查询语义和 StarRocks 不一致。
- FE/BE 协议字段被 NovaRocks 错绑、错补、错猜。
- 真实物理 schema / 聚合类型 / exchange sender / RPC 行为和 StarRocks 不兼容。

本次经验里的典型 bug：

- FE rewrite 输出 slot 的 `unique_id` 不是物理列 uid，不能直接当物理 schema 绑定。
- 这类问题要修执行引擎，不要调低用例标准。

## 9. 推荐工作流

1. 先找现有 `dev/test` 或已存在 `sql-tests` case，列出覆盖点。
2. 设计目标 suite 和 case 边界。
3. 写 `.sql`：
   - 文件头写测试目标
   - 用 `-- query N` 分段
   - 给异步/特殊 step 写注释和 metadata
4. 写 / 更新 `.result`：
   - 只记稳定正确的成功输出
   - 动态文本改用 metadata 断言
5. 先定向跑：
   - `cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --suite <suite> --only <case> --mode verify`
6. 若失败像 bug，先手工 SQL + 日志定位并修代码。
7. 修完后再跑定向 verify。
8. 定向通过后跑 suite 全量。
9. 如果你拆了 suite（例如 `mv-on-iceberg`），把两个 suite 都跑完。

## 10. 验证命令模板

定向：

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite <suite> \
  --only <case_a>,<case_b> \
  --mode verify
```

全量：

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite <suite> \
  --mode verify
```

需要重录时：

```bash
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --suite <suite> \
  --only <case> \
  --mode record \
  --update-expected
```

前提：只有在确认“当前输出就是正确语义”后，才允许 `record`。

## 11. 交付前自检

- 是否每个新增 `.sql` 文件都写了测试目标注释？
- 是否保持了原覆盖点，不只是“能跑通”？
- 是否把 Iceberg / 外表 / 内表语义分开维护？
- 是否避免了把报错写进 `.result`？
- 是否对异步可见性用了 retry，而不是只堆 sleep？
- 是否针对当前 NovaRocks 实际稳定输出更新了断言？
- 是否跑过定向 verify？
- 是否跑过相关 suite 全量 verify？
