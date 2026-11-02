# Filesystem conformance coverage

Run these targets from the repository root with `cargo test --locked -p
novarocks-fs --test <target> -- --test-threads=1`.

| Target | Behavior protected |
| --- | --- |
| `access_conformance` | Bound file identity, access domains, authorized ranges, and error redaction. |
| `lifecycle_conformance` | Cancellation, deadlines, close, and prevention of I/O after termination. |
| `reader_conformance` | Parquet projection, row group/page selection, absolute positions, budgets, and existing cache behavior; typed ORC refusal with its recovery condition. |
| `parquet_probe_conformance` | Object-store Parquet read and metadata probe. |
| `writer_corpus_conformance` (UEA-4A-2 I00) | Frozen Spark/parquet-mr, PyArrow, Trino, and Flink files read through the current production FS reader with row-value and absolute-position oracles. |
| `parquet_session_conformance` (UEA-4A-2 I02) | One identity-bound footer across runs, incremental index capability, projected column/page planning without a decoder, small-file one-read reuse, and absolute positions. |
| `range_read_conformance` (UEA-4A-2) | Demand priority, query/source fairness, bounded request dispatch, exact segment assembly, access, and actual drain. |
| `prefetch_conformance` (UEA-4A-2) | Byte and candidate windows, partial promotion, retained and reclaimed inputs, operation generations, and late completion. |

The session, range, and prefetch targets are added with their corresponding production slices.
The existing targets are retained as regression checks; a new target must
exercise behavior through the public filesystem boundary rather than assert an
implementation symbol or a source-file shape. Iceberg run, runtime-filter,
delete, and split-claim behavior is verified in its connector and Worker tests;
native 1FE+3BE behavior is verified by the system-test runner.

## UEA-4A-2 I01 停止信号实现者

| 来源 | 实际 owner 与投影 | 定向验证 |
| --- | --- | --- |
| Native BE Task | `NativeTaskExecutionHost` 在准备前创建 Task `ConnectorStopOwner`；`NativeRunnableTask` 的首次 stand-down 和准备失败回滚发布停止。 | Native adapter `execution_host::tests`；Worker Task registry 既有状态转移测试。 |
| FE 查询与统计任务 | FE 将已有 `QueryCancellationView::cancelled()` 经请求生命周期内的 relay 投影成只读 `ConnectorStopView`；请求克隆及外部效果后的上下文保留 relay。 | Frontend connector application、statistics、native execution 定向测试。 |
| FE 维护尝试 | 独立维护 fence 使用 `ConnectorStopOwner`；请求视图与 fence 以 `any_of` 组合，保留原绝对 deadline。 | Frontend maintenance 定向测试。 |
| Connector 读写 | Iceberg、Paimon 的生产请求检查消费同一具体视图；Paimon 文件控制保留请求视图。StarRocks 参考实现的测试也使用该契约。 | 各 Connector 的取消与生命周期测试。 |
| FS source / range operation | `FileCancellation` 将请求视图、source 子树和原 deadline 交给文件操作；`FileRangeOperation` 分开发布结果与真实 Task 退出回执。 | FS `runtime::tests`、`range_operation::tests` 和 `lifecycle_conformance`。 |

## UEA-4A-2 I02 文件读取接点

`parquet_session_conformance` 验证准确文件身份和授权域、footer 跨 run 复用、按需补页索引、元数据阶段范围规划、小文件单次整文件读取以及绝对行位置。`reader_conformance` 验证单投影 push decoder 的列值、行组/页选择和既有缓存语义。`physical_reader::range_io::tests` 验证合并范围与原请求的准确映射；`range_operation::tests` 验证固定目标分段读取及结果与真实退出的分离。后续 G2/G3 的公平并发和后继窗口不由 I02 的串行 G1 测试宣称通过。
