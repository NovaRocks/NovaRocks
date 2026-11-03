---
id: ADR-0191
title: "One machine fixture activation owns shared services and bounded scratch"
domain: [test-fixtures]
status: active
supersedes: [ADR-0165]
superseded-by: null
date: 2026-10-21
provenance:
  - "accepted design: TST-G18 revision 5 and approved implementation plan revision 3"
  - "fixture consumption policy updated by accepted TST-G18a revision 1, 2026-11-03"
code-anchors:
  - "docker/iceberg-rest/machine_runtime.py (MachineOwner, MachineDocker)"
  - "docker/iceberg-rest/machine_operation.py (CommandOwner)"
  - "docker/iceberg-rest/entry_publication.py"
  - "docker/iceberg-rest/runtime_entry.py"
  - "docker/iceberg-rest/scratch.py"
  - "docker/iceberg-rest/fixture_run.py"
  - "tests/cluster-harness/src/fixture_run.rs"
  - "tests/sql/runner/library/src/fixture_context.rs"
  - "tests/test-support/src/monitor/main.rs"
---

## 问题

共享fixture按worktree输入创建多版本资源，导致项目、网络、端口和状态卷持续增长。如何在每个daemon只运行一组核心服务，同时保留明确的维护权限、benchmark数据、不可变消费者输入和真实writer退出责任？

## 裁决

机器owner按准确Docker daemon拥有固定 `nr-fixture-machine` 项目、固定网络及一组MinIO/REST/rest-mv/Spark/mc。控制目录是状态位置，不能换目录创建第二套；daemon上的authority卷保护唯一性。端口来自固定机器配置，冲突失败，不扫描替代值。HMS与私有catalog仍由自己的owner管理，不隐藏计入核心栈。

TST-G18a 替代本 ADR 原先要求普通 up 核验 BOM、完整版本和健康的规则，现行输入政策见 ADR-0155。推荐版本与构建 recipe 集中在 `docker/fixture-inputs/versions.json`。已有机器直接使用保存配置，不比较当前推荐值，不自动做 digest、版本、健康或能力预检；只有确实缺失时按推荐创建。维护中、部分创建或状态不明报错交人工处理，不自动修复、重建或升级。机器 version 保留为创建时实际 image ID、模板与配置的定义标识，generation 区分实际激活；二者不再充当当前推荐环境的一致性 gate。显式 `fixture-runtime.sh check` 只报告诊断，普通 CI 不调用。

维护者显式start/upgrade。升级先保存maintenance，停止共享REST/Spark写者，持久化新访问材料并轮换旧root、撤销run IAM/STS；确认旧root确实拒绝后，先按准确旧容器ID移除已退出REST实例与其状态卷，原生核实absence并持久记录离线catalog屏障，再在准确MinIO在线时清理已分类scratch。整个MinIO卷和benchmark全部对象保留；purge完成后恢复目标activation不再删除已创建的新catalog，最后记录实际创建的目标并提交ready；不另加全环境健康预检。普通attach不能越过提交点。失败保留准确意图和maintenance；真正未知的mutation不能凭单次不存在或新snapshot猜成功。

维护锁由原机器owner及短命命令监护者持有。owner消失后，监护者停止后续派发，并保持CLI leader未reap直至原进程组实际退出；mc还须确认准确临时容器消失。超时和发出KILL均不代替实际退出。明确的daemon端口终态拒绝仅能在同一保存维护意图、真实CLI组退出、准确source/target资源重新核验后允许重试，不直接ready。

worktree publication以完整文件集合的内容身份保存，落盘后只原子替换一次published。锁序worktree→machine，机器维护不拿worktree锁。消费者固定一次不可变env/config/manifest，机器version/generation随输入保存。SQLite等稳定数据不在publication中；离线prepare仅发布不可用入口。解绑保留旧publication，没有消费者租约、在线切换或按旧data_locations递归删除的权威。

每次共享运行在首次副作用前准入整次预算，默认3600秒、max-run86400、清理预留300、TTL604800。嵌套沿用完整run身份和原期限，工作期限只能收窄，不能借重启/重试/shell重置；清理使用原完整期限。超预算在零副作用点BLOCKED，已准入超时为FAIL。Rust角色进程、Docker CLI及容器内Spark/JVM均由原child owner监督真实退出，期限不能转交为产品控制协议。

实际writer启动显式做有界过期sweep，使用原预算与固定publication；通用预算准入和READY只读检查不做清扫。

scratch名称保留13位epoch毫秒及32位随机token，共享namespace必须是完整57字符run ID加下划线及最多5字符用途后缀，裸run ID不属于namespace语法。运行开始前确定原worktree/run目录根、两个REST服务warehouse下的run名称字面前缀，共三个范围；CTAS与显式普通位置位于原运行根。`NOVA_FIXTURE_RUN_STORAGE` schema1独立于原预算JSON，继承时核实准确machine、MinIO卷和原generation，历史version只作为记录保留，不与当前推荐值比较，不能从metadata位置、namespace集合或通配符增加删除权。比较按bucket/key组件，绑定相同MinIO才允许s3a比较别名；原始控制字符、重复斜杠、dot组件、歧义编码与query/fragment拒绝，不改实际I/O URI。

发布前按bucket/key组件校验REST、rest-mv、benchmark与准确worktree/private基础根两两不相交，交叠即拒绝。公共祖先不构成删除域；明确属于原运行根的CTAS与vended私有子根允许包含，不扩大独立基础根或清理权限。

SQL原CREATE/golden和现有Java保持；JdbcCatalog普通表/view使用服务端warehouse的原生目录，不设置或依赖namespace location继承。普通表/view、同运行跨namespace rename、显式位置和CTAS使用统一运行范围谓词。默认位置等形状只由各具体测试断言。客户端REST warehouse仅在运行根下约束显式CTAS；可变Hadoop warehouse绑定run root，READY保持原绑定及check-only。

正常与TTL清扫先完整枚举两个共享REST服务中该运行所有namespace/table/view，校验实际位置，table non-purge删除条目和namespace，再核实整运行catalog均不存在。只有这一屏障及原运行IAM撤销都完成，才能递归清理原根与准确warehouse下实际枚举、完整语法匹配的目录。删除、枚举、撤销失败或结果未知保留文件；单个namespace消失不能放行跨namespace rename的旧目录。未知命名永不自动清理，status报告数量与枚举完整性；benchmark全部对象及其祖先/后代保护。停机升级用确认的准确旧状态卷absence替代在线catalog屏障，不请求已停REST。

共享REST和Spark使用共享凭证；运行IAM只约束实际使用它的writer。其余隔离依靠完整命名、统一位置校验、原预算与清理。独立运行owner确认writer退出后清整运行；嵌套producer只释放自己的catalog条目，文件责任交原外层owner，receipt明确deferred。TTL不掩盖正常teardown违约，也不按worktree活性猜测。Iceberg1.10.1没有unique-table-location配置，同名重建可复用目录，不能用目录名代替表代次。

私有stock/publication-hook保留独立REST及其原生命周期。vended只共享准确MinIO，私有网络避免REST DNS alias冲突；真实STS双身份及原key/token/expiry语义保留。私有writer先退出，才清原run IAM/对象；未确认退出时保留失败登记。HMS只接入机器网络并保留自己的Derby卷；ADR-0169的HMS永久只读裁决不变。

## 否决的替代与接受的代价

普通up可创建确实缺失的机器，但不能按当前checkout重建已有固定服务，因为这会中断未知消费者；已有机器的维护必须显式。继续并存多版本共享栈不能约束资源增长，因此退役其locator/绑定/stop-delete管理协议。所有测试改私有栈会丢失共享并发义务，因此只保留已明确需要的私有profile。持有者租约、统一reaper和自动历史删除引入另一个活性/数据权威，当前不采用。

升级会使旧直接S3凭证失败，scratch不能靠git回滚；固定无认证REST恢复后可能接受旧请求并重建scratch，不保证旧REST请求拒绝，也没有代理/代次URI或透明迁移。benchmark与整个MinIO保留卷须以真实对象清单及内容校验核对，READY标记本身不够。旧项目/卷不自动认领或删除，首次迁移要有准确保留清单和维护窗口。新机器若使用空卷，需要显式供给所需READY；输入缺失不改称测试成功。

共享catalog仍存在MV discovery的list/load与别的运行teardown并发时NotFound导致catalog隔离的产品风险。catalog优先删除避免长期悬空条目，但不消除该竞态；原始失败保留，产品修复归UEA-7B或独立任务。

## 验证与重新评估

定向fake/backend、真实child进程、壳层与Rust测试分别保护协议、期限、取消与发布。机器升级、TTL/S3/IAM、旧凭证撤销、Spark/JVM真实退出、同/跨worktree两个原生1FE+3BE并发以及约定SQL/System/benchmark完整正确性，须在授权环境另有真实执行证据；文档、mock或Compose parse不替代它们。

若需要不中断在线消费者的升级、跨机器runtime、持有者租约或历史卷自动迁移，重新讨论所有权与失败语义。不能把自动fallback、延长期限或扩大清扫范围作为局部修复。
