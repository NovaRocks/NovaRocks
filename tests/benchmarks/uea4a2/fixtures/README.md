# Range 功能fixture与来源记录

当前位置保留A4/range、短表与RF因果功能输入，以及真实producer。目录名不代表性能资产；pilot、RSS窗口、latency比较门和旧G0–G3协议不再维护。独立writer本地数据已原字节迁入 [FS canonical corpus](../../../../novarocks/fs/tests/data/writer-corpus/README.md)，FS测试直接消费该owner，不从此处副本选择另一份golden。

- `baselines/`和原manifest/receipt是历史冻结来源；不改其中旧端点、SQL或hash来冒充当前输入。
- 多文件A4的id/值公式、真实文件数和oracle需由实际task-private producer与READY证明；prepare不能藏在功能消费的错误恢复路径。
- `publish_short_iceberg.py`显式声明namespace/table和官方Spark文件输出；模板来源是FS corpus `spark/`，不会自动写入冻结旧namespace。独立逐行oracle与publication附件hash必须一致。
- `rf_late/`保留32行组及whole-file snapshot来源，供现有RF因果测试；固定物理输入不代替RF到达、消费/剪枝和实际退出观察。

旧writer receipt及附件校验器只核对已有输入；缺项和hash不匹配直接失败，不下载或合成另一writer。旧远端对象是否存在另由当前typed消费者验证。作用域cleanup只能移除本次task-private namespace/对象，不能删除FS corpus或共享READY。

当前入口与环境变量见 [range输入](../README.md)，planning多文件显式prepare/verify见 [planning输入](../../uea4a4/README.md)。相关input/provider/FS合同变更必跑对应canonical owner；本地FS沿Rust CI，native功能组每周手工领域轮次，缺输入不记通过。

Trino 语料与两份 `trino-manifest.json` 已冻结为来源证据，原生成脚本已退役。清单中的生成器名称、版本与校验值保留历史事实；现有校验器只读取冻结语料，不启动 Trino，也不重新生成数据。
