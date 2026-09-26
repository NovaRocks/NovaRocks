# MEM-W2B Linux 性能验收准备

本指南只规定可复现的运行输入与判定方法。批准的阈值以外部 workflow 的 MEM-W2B core plan v6 §4 为准；macOS 冒烟不能填作 Linux 通过。运行前使用干净的同一代码 SHA、原生 Linux、release profile 和 `CountingAllocator<System>`，记录 kernel、CPU/硬件线程数、Rust 版本、allocator、频率/隔离条件、命令和原始文件目录。

## 叶子供给负载

在观察任何候选结果前，仅在目标机器上校准一次：

```bash
cargo bench -p novarocks-memory --bench reservation_cost -- --calibrate
```

保存输出中的三个 `work_iters`、`observed_ns` 和 checksum。随后每个进程显式传入同一档固定迭代数，不逐进程重校准。每档 T 为 1、硬件线程数、2 倍硬件线程数；每个候选至少五个独立进程，候选次序交错。示例单格命令：

```bash
cargo bench -p novarocks-memory --bench reservation_cost -- \
  --candidate reservation --threads 10 --pairs 100000 --work-iters "$WORK_ITERS_5US" \
  --latencies-file /absolute/output/reservation-10t-5us-run1.csv
```

同格分别运行 `reservation`、`protocol`、`mutex`、`none`，相同 `--threads`、`--pairs`、`--work-iters`。`--work-iters 0` 是饱和诊断，不设吞吐通过门。大额混合另用 `--candidate reservation --mixed-every N --large-bytes 16777216`，单列成功/拒绝对数、CAS 重试、父链补额/返还，不混入 64B 快路径门槛。保存所有 stdout 与每轮原始延迟 CSV。

每个候选取五轮 `pairs_per_s` 的中位数及五轮 `p999_ns` 的中位数；在每个相同 T、相同工作档比较：5 µs 时 Reservation/none ≥0.90，20 µs 时 ≥0.95，1 µs 时 Reservation/Mutex ≥1.00，全部三档 Reservation 的 p99.9 ≤ Mutex。任何硬门失败都按 plan 返回设计评审，不用饱和或 macOS 数字抵消。

## 端到端留存负载

当前入口可运行候选/无治理 90% filter、8 跳 derive、8 次本地 move、fanout 2/4/8、共享 body 的整组导入、Arrow IPC 往返后的整组导入、重复 slice、`unary_mut` 原地复用/复制回退，以及跨线程最终释放。`ipc_group` 是合成共享 body，`ipc_decode` 在计时前完成真实序列化/解码，并报告往返后的实际 backing 共享数。`--value-type` 可选 primitive、nullable、dictionary、nested、view；`--backing-bytes` 请求完整输入 backing 容量并输出实际容量。`--phase-alloc` 仅支持单线程 `unary_mut`，输出输入、kernel、输出阶段的分配量。输出的 `matrix_complete=false` 表示尚不能据此裁决完整 V4：move8 是本地交接槽，分阶段分配仅覆盖单线程 unary，长时间 slice 稳态和 Linux 多轮矩阵仍待测。先用下面的命令确认两个候选完成相同的行数与 checksum，并保存原始延迟；其余已批准布局与类型矩阵补齐后再做 Linux 正式裁决。

```bash
cargo bench -p novarocks-memory-arrow --bench retained_cost -- \
  --candidate retained --scenario derive8 --layout shared --common-lineage \
  --threads 10 --rows 4096 --columns 8 --batches 100 \
  --latencies-file /absolute/output/retained-derive8-10t-run1.csv
```

同输入执行 `--candidate none`。`shared`、`siblings`、`independent` 是域布局；以输出中的 `lineage_scope` 判断是否真为跨线程同一谱系，不以布局名推断。记录输入/输出实际 backing 数和容量、data/metadata exposure、分配次数/字节、父链次数、拒绝数与完成行数。正式 90% filter 与 8 跳 derive 的门为同 T median 延迟比 ≤1.15、p90 比 ≤1.25、吞吐比 ≥1/1.15；每格至少五个独立进程，并保留全部原始样本。

## 专项入口与生命周期收据

`--scenario mixed_output` 使用与 derive8 相同的透传列加一列操作，只执行一次派生。正式共同谱系仍需 `--layout shared --common-lineage`。

`--scenario ipc_pin --keep-columns N` 在计时外准备独立的真实无压缩 IPC body，移交完整解码批次，保留 N 列后释放源，再释放最后 holder。N 必须非零且小于输入列数；该场景不支持共同谱系。collector 要验证选中列持有准确 record-body 基址及完整 capacity，不能用其他共享 backing 冒充。字典 message 的独立 backing 仍计入输入/投影的实际容量。为满足请求 backing，准备阶段可能序列化更多物理行，再 slice 到指定可见行数；输出报告实际容量、可见数据字节与放大比。每个线程在计时前准备全部批次，因此需要事前冻结 `batches × threads × 实际 body` 的内存预算。

```bash
cargo bench -p novarocks-memory-arrow --bench retained_cost -- \
  --candidate retained --scenario ipc_pin --layout shared \
  --threads 2 --rows 1024 --columns 8 --keep-columns 1 --batches 10 \
  --value-type primitive --backing-bytes 1MiB \
  --latencies-file /absolute/output/ipc-pin-retained.csv
```

同输入运行 `--candidate none`；两边完成相同 projection、消费和源/输出析构。`phase=domain_lifecycle` 单列本进程实际域构造、稳态、close、最后 domain drop 与 sponsor drop 的时间及分配量，并断言 Entry/set、account 数与最终 root L/C 回到静止点。none 不创建治理域，harness 初始化成本单独标示；不以零基线计算域构造比。该统计每进程只经历一次真实域生命周期，不是逐批创建域的额外矩阵；多线程分配只记录全局增量，不归因给单线程。

本轮仅交付可运行入口和正确性 smoke。五轮独立进程、完整参数矩阵与 Linux 门仍由后续正式验收执行，`matrix_complete=false` 保留。
