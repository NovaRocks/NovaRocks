---
id: ADR-0276
title: "Third-party encoding buffers use transport headroom"
domain: [memory-governance, native-transport]
status: active
supersedes: []
superseded-by: null
date: 2026-11-27
provenance:
  - "discussion: 2026-11-27, Root reply encoding ownership and transport envelope"
code-anchors:
  - "novarocks/native-adapter/src/root_result_unary.rs (RootCodec and RootEncoder)"
  - "novarocks/native-adapter/src/native_transport_geometry.rs (backend_root_encoding_report)"
---

## 问题

大小取决于查询数据的第三方传输编码副本，应该由查询预付，还是由进程传输余量覆盖？

## 背景与执行事实

Root 生产轮次分配原 segment。Task Codec 投影 DTO 时保留其 `Bytes` 别名。随后 tonic 以一块独立 `BytesMut` 编码 Root 回复，Hyper 与 h2 保留同一编码 backing 的别名。发送位置与固定元数据原本就由 Root 读生命周期持有。Root 末端的 Body 和受管 DATA 可以在不同时间退出。原有界 Root 发送路径见 [PR #1173](https://github.com/NovaRocks/NovaRocks/pull/1173)。

[ADR-0170](ADR-0170-third-party-crates-are-bounded-by-public-configuration-not-forked.md) 定义库内对象的公开尺寸、库外计数和真实退出规则。[ADR-0148](ADR-0148-process-memory-capacity-authority.md) 将 P 划分为硬治理容量 B 和余量 H。两者合起来允许传输报告用已知数量与单项大小作结构估算，H 也覆盖尚未测清的第三方内部、原生库和 allocator 开销。

## 考虑过的选项

- **沿数据来源向查询预付编码字节（设计否决）。** 这能在查询账本内控制在途字节，却把独立传输分配的容量决定交给结果生产者。内存紧张时，读取已有结果也会再次申请大块查询信用。
- **增加传输动态预算（设计否决）。** 可以为每次编码申请设定单独的字节背压。接入它会让发送同时受进程 H 和另一套独立配置约束，现有进程容量模型只保留一套余量配置。
- **用已有限定的位置和冻结大小做结构报告（采用）。** 原传输入口控制在途数量，第三方编码缓冲由 H 承担。每次正常回复按实际成帧长度申请，冻结上限用于校验和结构算术。

## 裁决

1. **按分配作者划分。** 查询承担原 segment 和查询自有元数据。tonic 在传输任务中创建的编码 backing 属于传输服务，由现有 `[runtime.memory].headroom_bytes` 覆盖。缺省 H 为 P/4。这笔编码内存不进入单查询上限。相同原则适用于 exchange，后者按自己的发送位置和实际退出证明结构贡献。
2. **原位置保管到实际退出。** 每个 Root RPC 的 ingress permit 随 Body 与 DATA 退出。数据回复也持有原 Root delivery，后者使该 Root 的固定钱包信用维持到最后相关别名退出。临时固定元数据不足仍可在 ACK 前延期。
3. **单项尺寸与分配长度分别验证。** Root 在原 tonic unary 路径使用实际 protobuf 长度加 5 字节前缀作为编码申请。冻结上限保护原协议和结构算术。异常内部编码由原 tonic 机制终止。
4. **BE 报告单列编码请求字节。** 以原 ingress 位置 I、有效 Root 进程钱包 J、每 Root 固定信用 F、每 Root 发送位置 K 和冻结数据/控制大小 D、C，计算 `I × C + min(I, K × floor(J/F)) × (D−C)`。数据回复同时占两个作用域的位置，因此每份 backing 只计一项。使用启动日志的有效输入、lane 小计、Root 编码项及已知结构合计诊断。缺失或不可推导的量保留未知。

## 接受的妥协（诚实记录）

默认 Root 钱包为 4 GiB 时，上式的保守请求字节界为 8,775,008,256 B，含仍可能在途的短控制回复。这是基于现有门的保守算术。支持几何的 1,280 个发送位置是另一种部署算例。1,280 份同时落入 jemalloc 的 1280 KiB 档时，档位合计为 1.5625 GiB。若 H 默认等于 P/4，仅容纳这一项需要 P 至少为 6.25 GiB。实际长度、allocator 档位、触页及物理驻留须分别观察。H 由既有配置和物理用量判断，结构报告不会拒绝个别发送。

## 何时重新评估

- 固定钱包信用或 Root 与 ingress 的持有寿命发生变化，重新证明大小和在途数量，再更新结构报告。
- 生产观测发现传输服务或 allocator 偏差挤占 H，复核已知结构项和实际物理用量，沿现有内存配置调整。
- 已采用的 tonic 版本提供公开零拷贝接口，且验证 Root 和 exchange 的实际 backing 退出后，重新评估两条路径的复制行为。
