---
id: ADR-0157
title: "Native RPC 接收保护为何分布在认证后入口、方法尺寸门和 codec"
domain: [distributed-query-lifecycle, runtime-role]
status: superseded
supersedes: []
superseded-by: ADR-0161
date: 2026-09-23
provenance:
  - "discussion: 2026-09-23 native RPC ingress and control isolation"
code-anchors:
  - "novarocks/native-adapter/src/native_ingress.rs (NativeIngressService)"
  - "novarocks/native-adapter/src/native_server.rs (NativeRpcServerHandle::start)"
  - "novarocks/native-adapter/src/native_codec.rs (NativeProstDecoder::decode)"
  - "novarocks/native-adapter/src/native_control_executor.rs (NativeControlExecutor)"
  - "novarocks/native-adapter/src/backend_metrics.rs (native ingress observations)"
---

## 问题

Native RPC 同时承载较大的 Task 创建请求和必须在普通工作拥塞时继续推进的取消、续租与释放。接收、protobuf 解码、执行和响应各有不同成本与最后持有者；准入放在哪些边界，才能使本地工作有界，且不把一种门误称为完整的资源治理？

## 背景与执行事实

| 接缝 | 可见事实与实际成本 | 责任 |
|---|---|---|
| `NativeListenerAuthService` 后的 `NativeIngressService` | 已认证的 header、方法 path、尚未交给 Tonic 的 body；尚未发生 protobuf 对象构造 | 按方法分普通、控制、流，取得有界 running/waiting 资格，保留到真实 holder 退出；从到达时刻计算期限 |
| `NativeRpcServerHandle::start` 的精确方法路由 | Tonic 读取消息前能选择不同的实例上限 | 小控制请求使用独立的小解码帧长门，普通 Task 保留较大的合法消息上限；同一 proto service 与方法名不变 |
| `NativeProstDecoder::decode` | 已组装的原始消息字节，prost 树尚未构造 | 按实际消息类型检查可导致资源展开的结构形状，随后交给 prost 解码；业务准确性仍由原 owner 校验 |
| `NativeControlExecutor` 与 Worker | typed 请求已到达，需要运行并取得本地状态 | 小控制有独立的有界执行能力；Worker 仍拥有 Task、Context 和 ticket 裁决 |

入口资格、帧长上限、Worker context reservation、Exchange slot 的单位、owner 和释放事件不同，不能把它们合并成一份含糊的“容量”。运行期具名读数分别暴露普通/控制入口的占用、排队、拒绝原因和期限，以便现场判断当前是哪道门饱和。单消息字节上限乘以同时持有数及解码放大，仅是配置时必须登记的容量包络估算，并非精确 RSS 保证。

## 考虑过的选项

**只在 typed handler 内取得许可。** 接口简单，但 Tonic 已收完整 body 并构造 prost 树，无法保护前面的实际消耗；这是设计否决。

**给所有 Task 消息加一层 envelope，并长期维护第二套 protobuf 语义解释器。** 可以在解码前拒绝更多编码细节，但复制 IDL 语义、与 prost 漂移的成本很高；在受控部署、同发行二进制和 Native trust 前提下，只保留资源展开检查，由 prost 与准确业务门处理其余合法性。这是成本否决，不允许悄悄放宽既有业务拒绝。

**拆分 proto service 来取得不同的 Tonic 帧长。** 可获得方法级上限，但引入 service/IDL/客户端迁移；同一 Axum listener 的精确方法路由已能选择两个 Tonic 实例，因此本次是成本否决。路由只解决收包前尺寸，不能代替执行隔离。

**认证后分层准入、精确路由、codec 预检和独立小控制执行。** 在各自成本发生前施加相应的门，保留现有 Worker 准确裁决；这是裁决。

## 裁决

1. **成本前置规则。** 本地接收 running/waiting 资格在认证后、Tonic 读取 body 前取得；请求的资格随 body、handler、blocking closure、响应及其真实 backing 的最后持有者移动，不因某个 future 提前 Drop 就虚报释放。等待只取请求剩余期限，不重置到达时钟；Issued ticket 的 Establish 不得等过其真实剩余有效期，Redeemed 的准确重放按 Worker 合同判断。
2. **按方法尺寸规则。** 控制方法走精确 path 与小 Tonic 解码上限，普通 Task 走其合法较大上限。分类与帧长必须在配置中联合校验；控制接收、执行和响应都须在普通路径饱和时可推进。Native async worker、普通 blocking 与独立控制执行容量使用正常角色配置及跨参数关系校验。
3. **单一解码语义规则。** codec 只对可能引起对象展开的原始结构做资源预检，protobuf 编码合法性仍交 prost，身份、ticket、生命周期和创建冲突仍交现有准确 owner。新增操作必须显式决定 FE 调度、FE transport 与 BE 方法分类，不靠通配默认。
4. **分账与诊断规则。** Tower 计数资格和每条消息帧长的乘积按方法类别登记估算，并以真实饱和场景记录粗驻留峰值与限制；入口、Worker 和 Exchange 各自保留其准确单位与 owner。运维观察同时给出各入口门当前占用、排队和拒绝原因，不只记录每次失败的历史事件。

## 接受的妥协（诚实记录）

计数资格乘单消息上限不能严格约束 prost 树、临时 clone、下游持有和运行时分配的 RSS；本设计只给局部接收结构界与实测量级检查，动态内存记账仍由内存治理工作接续。精确方法路由复用同一个 proto service，减少协议迁移，但必须持续验证静态路由优先于通配、以及新方法被准确分类。独立控制执行器占用固定少量线程；这是为了在 registry 锁及普通 blocking pool 压力下维持控制进展，而非宣称任何饱和形态都不会影响 h2/async worker。

## 何时重新评估

- Native 端口开始接纳非同发行或外部工具直连时，重新评估 trust 边界、protobuf 严格拒绝范围及版本协商；届时第二套原始解释的成本收益可能变化。
- Tonic 提供可靠的逐方法解码限额，或 proto service 因独立产品理由需要分拆时，撤掉 Axum 精确路由的重复装配。
- 实测容量包络、async 调度延迟或控制进展在目标负载下大幅回退时，重估线程/许可比例与执行隔离；不要仅放大消息或队列上限。
- 动态内存账本能在每个真实 holder 上分配前预留并在最后使用后释放时，升级局部计数与粗 RSS 估算的治理边界。
