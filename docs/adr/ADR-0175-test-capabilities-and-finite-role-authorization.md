---
id: ADR-0175
title: "Test capabilities require finite authorization for each actual role launch"
domain: [test-capabilities]
status: active
supersedes: []
superseded-by: null
partially-superseded-by: [ADR-0181]
date: 2026-10-14
provenance:
  - "discussion: 2026-10-13 controlled test capabilities, actual build evidence and MV recovery ownership"
code-anchors:
  - "novarocks-server/library/src/build_information.rs (value)"
  - "novarocks/failpoint/src/session.rs (SessionDeclaration)"
  - "novarocks/failpoint/src/scope.rs (SessionScope, GrantHandle, ActionClaim)"
  - "novarocks/failpoint/src/liveness.rs (RunnerSession)"
  - "tests/cluster-harness/src/test_sessions.rs (ClusterTestSessionPlan, check_binary)"
  - "novarocks/frontend-application/src/mv/domain/readiness.rs (wipe_accelerator_for_test)"
---

## 问题

如何让测试控制覆盖真实 FE/BE 生命周期，同时保证普通产品不包含控制入口，并且已编入的能力只能作用于本次准确授权的进程与工作？

## 背景与执行事实

构建能力、角色启动授权和产品执行身份是三个独立事实。Cargo feature 可以使控制代码存在，但不能证明某次启动允许使用它；路径和 profile 可以用于寻找产物，但不能证明那个文件实际编入了什么。FE/BE 的真实进程身份、statement generation 和 query/task identity 仍由产品 owner 签发，测试声明只能绑定这些事实。

| 实体 | 所有者与职责 | 不能替代的事实 |
|---|---|---|
| `normal-build` / `test-controls` | 每个含控制代码的 crate 在真实依赖边传播能力并拒绝冲突；Server 选择产品构建种类 | 环境变量、debug assertions、场景名称不能绕过普通构建排除 |
| `--build-info` | 实际 binary 在启动配置与运行时副作用前自报 owner 能力、声明 profile、实际 binary cfg 和已知构建事实 | runner 自报、目录名、源码 HEAD 不能代替实际产物自报；自报不是发布认证 |
| `SessionDeclaration` | runner 冻结有界声明、role、launch、有限 domain grants 与有限寿命；原绝对期限由 runner owner 持有 | 声明不是跨进程 `Instant` 或产品 query identity，也不是自动匹配后续任意语句的权限 |
| `SessionScope` / `ActionClaim` | 准确角色进程完成 Hello 后绑定；一次认领、关闭与取消保持单向 | 观察许可不能变成注入许可，结束后不能重新激活或继承给重启进程 |
| owner 本地控制对象 | 在领域已有调用接缝校验参数、绑定真实工作并执行有限动作 | 通用机制不解释领域语义，不增加产品配置或公共管理路由 |
| `ClusterTestSessionPlan` | 冻结每个真实角色的 binary 与授权，保留实际自报和生命周期证据 | 控制 helper join、工作退出、角色 reap 与资源收敛分别取证 |

声明具有版本，序列化大小上限为 64 KiB，授权条目数由该字节上限约束。每次角色启动拥有独立身份与私有材料，liveness 使用准确进程握手及有限心跳；runner 持有原绝对期限，握手只交付剩余预算，角色将其转换成本地时钟期限。EOF、失联、期限、主动结束均使控制失效。握手和心跳不续期。领域 payload 保持 owner 本地解释，共享 failpoint crate 只提供中立的有限授权与生命周期机械能力。

MV 文档观察和整族 Accelerator 擦除具有不同授权。`wipe_accelerator_for_test` 复用产品已有 admission stop 与 background join，在真实后台退出且不存在活跃 publication 后才执行擦除；随后 runner 停止并 reap 原 FE，创建新 launch，并按剩余已冻结需求选择普通或受控产物及新会话，不继承旧 grant。擦除不定义新的启动期恢复语义，也不改变湖上文档、管理接续或查询结果判据。

## 考虑过的选项

1. **debug/profile 加进程环境变量。设计否决。** 接线简单，但 debug/profile 不是权限；进程级开关无法限制准确 statement、角色重启或有限次数，也不能确保普通构建排除控制代码。
2. **整套 suite 使用单一控制产物并授予全部控制。设计否决。** 减少产物和声明管理，但普通步骤因此不再验证普通构建，无关请求可能认领故障，重启权限也难以保持有限。
3. **统一控制 registry 或公共管理接口。设计否决。** 表面上能集中调用，但会让共享测试机械层拥有领域语义、目标选择和生产生命周期政策，形成第二 authority。
4. **按真实依赖排除普通构建，并对每个实际角色启动授予有限 owner 本地能力。采用。** 需要维护编译传播、产物选择和精确声明，但这些事实可分别验证，领域解释和生产执行身份保留原 owner。
5. **用外部签名与可复现构建证明产物身份。待评估。** 这可以加强供应链认证，但本裁决只定义真实 binary 的事实报告，不把该报告扩张为完整编译命令、干净源码或可复现发布的证明。

## 裁决

1. **普通构建排除规则。** 含控制代码的 owner 沿实际依赖边传播 `normal-build`，与 `test-controls` 冲突即编译失败。普通 Server 不包含内部测试路由和控制实现；不能在同一 Cargo 命令中统一普通 Server 与控制 owner 的 features。
2. **实际产物自报规则。** 普通、控制和兼容性变体保持独立 target 与实际路径；启动前读取每个文件自身的 `--build-info` 并保留原始 stdout/stderr/退出结果。已知事实据实记录，未知 flags、依赖 overrides、最终 LTO/codegen 等保留 unknown，不能从 profile 名反推。
3. **准确启动授权规则。** 编入能力仅是必要条件；grant 必须属于当前 role launch，握手绑定真实 FE/BE 进程，并匹配原始声明和 domain。重启需新启动绑定，不能移交旧会话或用旧文件恢复权限。
4. **有限工作绑定规则。** runner 显式指定有限动作；领域 owner 将它绑定到真实 statement generation、query/task 或所拥有的工作。无关请求不能抢占，建 attempt 前失败不能让旧授权顺延。观察、注入和擦除权各自独立；裸测试 SQL、场景名与时序不授予权限。
5. **单向结束与原期限规则。** 一次 claim 不因对象重建、轮询、重试或心跳刷新。所有等待使用原绝对期限的剩余预算，关闭/取消后既有与迟到调用均不能恢复控制；控制失败与 cleanup 失败保留真实结果。
6. **实际退出规则。** 超时、terminal、关闭标志和取消通知不证明退出。helper 要实际 join，角色要实际 reap，产品工作与资源依旧按其 owner 的既有判据验证；一种证据不能代替另一种。
7. **领域接缝规则。** 共享层仅拥有有限授权机械能力；控制解释留在 owner 本地，复用已有产品生命周期。MV 擦除必须先 admission stop、实际后台 join、检查 publication，再擦除和重启，不新增 start-empty 或通用擦除产品接口。

## 接受的妥协（诚实记录）

独立产物与逐启动声明增加构建缓存和接线成本。选择这个成本是为了保留普通产品的排除边界、准确权限和可审核证据，不能用一个控制产物通过所有步骤来抵消它。缓存可复用，但运行证据必须独占并保留失败；清理只回收自身可重建缓存，不删除必要产物和原始材料。

产物自报有明确知识边界：它报告编译期可知的事实和显式声明，无法认证完整依赖编译配置或发布供应链。构建/机制检查也不证明 Linux 性能或所有 Native/SQL 场景通过，实际验收仍单独记录。ADR 的 active 表示裁决有效；实施与验收状态由实际证据另记。缺失历史私有输入的矩阵保持未运行，不能用新生成输入冒充。

当前 MV 控制承接已有保护，其管理接续与擦除载体可能仍是迁移中资产。保留它是因为对应领域替代保护尚需逐项完成；未来唯一产品 owner 必须先实际验证替代性质，再退役旧载体。控制机制不替该领域决定新恢复语义。

## 何时重新评估

1. 发布供应链需要认证完整编译配置或来源时，评估可复现构建和签名证明；保持认证与运行授权分开。
2. owner 边界或实际依赖图改变时，重新核验普通能力传播与控制入口归属；不能以源码形状扫描代替编译边界。
3. 新控制需要跨 role 转移、长期续期或未声明的目标选择时，必须重新设计授权模型，不能扩张当前有限会话。
4. MV 唯一 owner 的替代保护实际通过后，逐项退役相应旧管理/擦除载体；不以未来计划或接口名称作为删除证据。
5. 自报字段可以从真实编译链获得更多确定事实时，补充可证明字段；仍无法证明的内容继续保持 unknown。
