# 无 Schema 在线单元的深值 SQL

深值用例只写浅 `id INT` seed，深值由执行表达式产生，保留 shuffle/broadcast 的计划断言与按 id 排序的两行结果。独立期望为整数7、19各包63次：ROW为单元素位置数组，ARRAY为单元素数组，MAP为单键"k"对象；两侧按同id连接后保持原值。Map aggregate替代路线每层按id分组，每组恰一行，故同样添加一个map层。

维护 owner为complex-type suite / 在线批次codec。suite级正确性运行消费这些用例；改动codec、exchange或嵌套类型作者时必跑。用例不新增数据集规模或性能门限；深值及 COUNT 用例使用两行 seed，字典值用例使用四行浅 id/label，均沿 runner 原 case namespace 清理。

原始正向答案先按上述代数独立推导，再形成类型化golden。`row`/`struct`/`named_struct`/直接`map`当前的pure注册能力缺口按用户2026-11-03裁决保留真实FAIL交后续任务；不能把这些文件改成预期报错或通过。已安装的ARRAY与map_agg路线继续验证，Map/Struct的真实Driver组件证据单独记录。原生1FE+3BE实际结果与清理以本次runner记录为准；文件存在不表示验证通过。

独立期望的原始推导在本地实验材料`20261103T090705Z-deep-sql-oracle-96d416`保留，不声称已经跨机移交。当前标准golden比较政策不变；实际record后须逐块与独立期望核对，禁止从错误结果反写答案。

`schema_free_exchange_count_only`独立预期为2×2=4与两次等id匹配=2。SQL COUNT输出不证明某条边有零列；真正的零列三行shuffle/broadcast几何由原Driver生产路径测试逐单元断言，SQL实际冻结布局另行核查。

`schema_free_exchange_dictionary_values`按相等id连接，独立预期就是原4行的a/a/b/SQL NULL。SQL字面值重复与wire字典消息的证明分开；实际单元内完整字典、字典ID和bare buffer别名由原Driver/decoder测试核对，原生运行还消费low-cardinality suite既有观察。

分发路线使用现有 CBO 会话配置：预算为 0 的步骤排除广播，恢复原默认 1 GiB 的步骤保留广播可行性，并分别严格核对 PARTITIONED/BROADCAST 计划断言。当前表引用 hint 尚未进入逻辑 Join 约束，不能仅凭 `[shuffle]` 字面量声称实际分发。深值在右输入子查询内生成，保留原63层及结果，不在 JOIN 后才构造。多消费者 CTE 的现有 compiled 能力拒绝不属于在线 codec；这些用例以独立输入分支覆盖真实交换。

按用户2026-11-03的本轮补充，四个受阻 `schema_free_exchange_depth64_{struct,list,map,map_aggregate}` case 暂时忽略，沿 dispositions.quarantine 保留逐项owner、复查日期与恢复条件。运行时显式使用原 `--skip` 选择，不在编解码器或 runner 中加入按case名称的限制。ROW/MAP是缺函数，List原失败是客户端递归限制，Map aggregate原Resource作者尚未定位；不能将四项全部归因为上游缺陷。COUNT与dictionary_values继续必跑。
