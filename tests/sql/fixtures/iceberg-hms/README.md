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

# HMS read-only compatibility fixture

Start the already provisioned `docker/iceberg-rest/` and
`docker/iceberg-hive/` fixtures, then source their exact generated publications.
The SQL suite remains `explicit_only` and runs on native 1FE+3BE with `-j 1`.
`fixture.py` does not provision images or download inputs.

Spark creates format v1/v3 partitioned tables and a v2 unpartitioned table, a v2 position delete,
a v3 deletion vector, and v2 branch/tag references. Preparation checks for a PARQUET position-delete file on v2 and a PUFFIN deletion vector on v3; the v2 rows share one file to avoid a whole-file delete. Spark changes the v2 delete mode to copy-on-write after preparing its position delete, so both mutation modes can be refused through valid SQL requests. Tables use locations under
`<generated HMS warehouse>/iru7/<unique namespace>/`. NovaRocks only reads
these tables and attempts operations expected to be refused.

The before/after evidence includes namespace/table/view lists, each table's
**current** HMS metadata location (via `TableOperations.current()`), and every
object's key, etag and size. ListObjectsV2 uses two objects per page and rejects
missing or repeated continuation tokens. Inventory covers the private namespace
location and the exact Hive default namespace locations (including `.db`) for
attempted namespace/table creation. It never compares just one table's files.
Evidence is saved under `tests/sql/.runtime/hms-evidence/<namespace>/`.

Cleanup executes Spark PURGE/DROP, then deletes remaining objects only in those
unique namespace prefixes and verifies that they are empty. Shared HMS services,
other namespaces and other worktrees are retained. Provider entry tests cover
operations that cannot be reached through SQL on an HMS-managed MV, as HMS
cannot create that target.

### 共享机器运行预算

共享消费者通过本次不可变 publication 读取机器事实，继承
`NOVA_FIXTURE_RUN_CONTEXT` 的 run 身份与原截止时间。嵌套 Spark 调用不会重置
预算；容器内 owner 负责准确进程组的终止与实际 wait。正常清理失败会返回失败，
不能以未来 TTL 清扫代替本次清理。私有 publication 保留其原有资源 owner。

### D21：最终 SQL 绑定前的固定输入

`suite.toml` 的 `case_input` 只允许这一个固定 recipe。runner 在纯定义读取和选择后，使用原运行的 budget、storage、namespace 及访问投影调用固定的 `fixture.py prepare` 路径。准备保持原 Spark CREATE/INSERT/DELETE/branch/tag 顺序，以真实 SDK metadata 冻结唯一最早的 append snapshot ID、table UUID 和 metadata location；来源与实际输入字节的 hash 随 receipt 保存。

q1 的 `verify-prepared` 核验该 receipt 及真实准备结果，q11 读取 metadata 并核验同一 ID，q12 使用其 numeric `VERSION AS OF`，继续比较原来的三行。当前资产共 61 步，保留 38 个拒绝断言；q57 改用标准 MV 系统表，q58 保留表列举，q59 改用 VIEW 类型过滤，HMS 不支持视图列举时为零行。改写期望仅作静态核对，未运行或 record；其余既有期望不重新录制。普通发现和 dry-run 不创建这些数据；没有可绑定输入时明确保留未绑定事实。

准备之前先持久登记 intent；清理在首次派发前登记 attempt。正常 q60 清理、q61 删除 catalog 与公共尾部共同遵守一次派发合同，失败或 Unknown 保留原件和诊断。远端 child 的 settlement receipt 不代替 host 进程组或 Server 退出证明；硬退出、缺失 receipt 和过期预算均不能补成成功。`case_input` 绑定固定 recipe，runner 使用其既有准备、核验和清理动作。
