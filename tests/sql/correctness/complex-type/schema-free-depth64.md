# 无 Schema 在线单元的深值 SQL

深值用例只写浅 `id INT` seed，深值由执行表达式产生，保留 shuffle/broadcast 的计划断言与按 id 排序的两行结果。独立期望为整数7、19各包63次：ROW为单元素位置数组，ARRAY为单元素数组，MAP为单键"k"对象；两侧按同id连接后保持原值。Map aggregate替代路线每层按id分组，每组恰一行，故同样添加一个map层。

维护 owner为complex-type suite / 在线批次codec。suite级正确性运行消费这些用例；改动codec、exchange或嵌套类型作者时必跑。用例不新增数据集规模或性能门限；深值及 COUNT 用例使用两行 seed，字典值用例使用四行浅 id/label，均沿 runner 原 case namespace 清理。

原始正向答案先按上述代数独立推导，再形成类型化golden。`row`/`struct`/`named_struct`/直接`map`当前的pure注册能力缺口按用户2026-11-03裁决保留真实FAIL交后续任务；不能把这些文件改成预期报错或通过。已安装的ARRAY与map_agg路线继续验证，Map/Struct的真实Driver组件证据单独记录。原生1FE+3BE实际结果与清理以本次runner记录为准；文件存在不表示验证通过。

独立期望的原始推导在本地实验材料`20261103T090705Z-deep-sql-oracle-96d416`保留，不声称已经跨机移交。当前标准golden比较政策不变；实际record后须逐块与独立期望核对，禁止从错误结果反写答案。

`schema_free_exchange_count_only`独立预期为2×2=4与两次等id匹配=2。SQL COUNT输出不证明某条边有零列；真正的零列三行shuffle/broadcast几何由原Driver生产路径测试逐单元断言，SQL实际冻结布局另行核查。

`schema_free_exchange_dictionary_values`按相等id连接，独立预期就是原4行的a/a/b/SQL NULL。SQL字面值重复与wire字典消息的证明分开；实际单元内完整字典、字典ID和bare buffer别名由原Driver/decoder测试核对，原生运行还消费low-cardinality suite既有观察。
