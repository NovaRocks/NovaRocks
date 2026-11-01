---
id: ADR-0204
title: "One-shot spot CI separates compute release from evidence handoff"
domain: [test-capabilities]
status: active
supersedes: []
superseded-by: null
date: 2026-10-28
provenance:
  - "discussion: 2026-10-28 manual spot execution, direct candidate image start and evidence handoff"
code-anchors:
  - "tools/ci/aliyun_ci/lifecycle.py (create_once, delete_exact)"
  - "tools/ci/aliyun_ci/local_run.py (start_local, finish_local)"
  - "tools/ci/aliyun_ci/source_bundle.py (create, import_bundle)"
  - "tools/ci/aliyun_ci/teardown.py (teardown_run, delete_exact)"
  - "tools/ci/aliyun_ci/handoff.py (pack, verify)"
  - ".github/workflows/aliyun-ci.yml"
---

## 问题

手动测试使用可随时回收的云算力时，谁拥有一次执行的身份、测试结果、资源关闭和材料保留？

## 背景与执行事实

平台沿现有原生 Rust batch 执行。它可以申请机器、启动 Runner、传递取消与搬运材料，但不拥有测试选择或失败解释，也不授予正式准入身份。

| owner | 事实与责任 |
|---|---|
| 本机run/attempt或GitHub run/attempt | 一次请求身份；调用方保管原run记录与真实终态，GitHub三job是独立接入方式 |
| start | 创建一台抢占实例；创建时写全部归属标签，以原 ClientToken 处理未知结果；核对实际镜像、标签和自动释放时间后交付 JIT |
| 原生 batch | 完整常规选择、联合准备、断言、取消、子进程收束、execution-index 与真实退出 |
| 实例 bootstrap/watcher | 只读工具及 Runner ABI 检查；root-only JIT 交给降权后的 Runner；抢占预告取消原 batch 并上传部分材料 |
| teardown | 按准确 ID 释放与查询 absence；从机外验证归档，再记录原生结果、交接完整性、算力终态与遗留资源 |
| 云平台自动释放 / janitor | 前者不依赖 GitHub 存活；后者按原归属与原 deadline 巡检异常，不按显示名或宽泛前缀删除 |

执行实例不是常驻池，不加入伸缩组。固定镜像包含工具和平台脚本；源码、target、Runner 注册和凭证不进入镜像。Runner 版本从已校验的独立副本取得。已登记候选镜像可直接启动，工具检查通过事实与是否允许首次启动分开，避免把验证结果同时设成验证的前置。

代码已有本地纯逻辑、API 回放与真实短命进程/HTTP 功能测试；这些事实不证明真实云部署、Linux systemd/网络隔离、完整 Rust 执行或 GitHub 取消已经验收。

## 考虑过的选项

1. **一次性实例与三个依赖 job（采用）。** 测试期间无需托管控制 job 持续观察；每个请求只接一个 job，结束或被抢占后不自动补机器。
2. **伸缩组与持久控制会话（设计否决）。** 它们适合维持常驻容量或恢复长控制会话；在单请求、无自动重试的合同下，自动补出的机器没有合法接单者，反而增加身份与回收责任。
3. **仅靠 teardown 关闭（设计否决）。** GitHub 本身可以取消、失联或无法调度收尾；可计费实例必须有已核实、不依赖该控制面的终点。
4. **交接成功后才释放实例，失败时保留磁盘（成本否决）。** 能保住更多唯一材料，但需要停机保留、恢复访问和人工接管的完整生命周期。当前选择一次性磁盘与显式交接缺口，接受尚未上传的数据可能丢失。
5. **Runner 烤入镜像（成本否决）。** 使镜像随 Runner 的升级期限重建；独立版本副本和启动时校验将该维护周期与 OS/工具身份分开。

## 裁决

- **原身份规则。** GitHub 使用 run/attempt，本机开发验证使用具名local身份和显式local-rust执行种类。未知创建结果保持原 ClientToken，不换 token 再建，不做按量回退或透明重试。
- **先核实后接单规则。** 可接单前读回准确 ImageId、完整归属和自动释放时间；引导秘密只交给核实后的实例。测试账户不能通过 IMDS 获取实例角色能力。
- **原生执行规则。** 本机云助手与GitHub只启动同一原生batch；本机源码以准确已提交SHA/tree和逐文件摘要的自包含Git bundle交付，允许未推送提交，dirty在副作用前拒绝。Rust step直接exec原batch。平台不加测试过滤或跳过表，不根据断言文字决定重试；取消与原始退出保持原事实。
- **独立关闭规则。** teardown 按准确身份删除并确认 absence；自动释放承担控制面失联兜底；janitor 只清理拥有准确归属、原 deadline 与相应云事实的资源。
- **原任务观察规则。** 本机记录在副作用前持有原run，派发未知不能重新启动测试。status/collect/cancel使用原实例和原命令；断连不授予第二次派发权。local基础设施不依赖GitHub，测试账户不能取得维护者凭据。
- **四类事实规则。** 测试结果、归档校验、算力终态和遗留资源分别记录。有效归档中的 execution-index 原样投影；缺少结果时保持未知，不能由实例已释放或归档存在补成成功。
- **有限交接规则。** 执行机只取得本运行对象的限时写入能力；预签名期限不能超过已知临时凭证有效期。结束与中断归档均带逐成员摘要，机外读取验证，不把上传调用成功等同于交接完整。

## 接受的妥协（诚实记录）

材料上传失败时仍释放算力，未交接部分可能永久丢失；GitHub 实时日志是独立诊断来源，不能替代原生材料。每次冷构建接受下载与链接成本。抢占、库存不足和工具不符会让请求失败，使用者须另发请求。镜像、快照、OSS 与未确认释放的资源仍可能产生费用。

候选镜像直接启动意味着第一次运行可能因工具或 ABI 不合而失败；它必须保留失败和资源终态，不获得验证通过或正式准入资格。OSS 的正式保留、访问权限与真实云验收由维护者落实，源码与本地回放不代替它们。

## 何时重新评估

- 需要维持常驻容量、自动抢占重试或跨控制会话恢复时，重评伸缩组与请求所有权。
- 唯一材料丢失已不可接受，且维护者能承担完整恢复流程时，重评停机磁盘保留。
- Runner 交付的网络条件或维护成本变化时，重评烤入镜像与独立副本的选择；不得靠过期 Runner 静默等待。
- 需要 fixture、原生多进程拓扑或正式准入时，分别取得原 owner 的运行与来源证据，不从 Rust 平台机制推导资格。
