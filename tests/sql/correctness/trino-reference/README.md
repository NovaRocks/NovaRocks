# Trino 参照互操作验收

本显式 suite 使用 fixture input builder 声明的 Trino 镜像，以及共享 REST/MinIO fixture，在 native 1FE+3BE 上验证 Puffin 统计互操作。

`trino_rest_puffin_show_stats.sql` 让 Trino 创建并分析父表，NovaRocks 追加并写出统计，再由新的 Trino 进程读取当前快照并通过 `SHOW STATS` 核对合并后的 NDV。suite 的 init/cleanup 独立管理所需 REST catalog；不会依赖先运行 `statistics`。

运行入口在最终选择后解析参照，保存镜像引用并传给 shell 步骤。参见 `tests/datasketches-tck/interop/README.md`。
