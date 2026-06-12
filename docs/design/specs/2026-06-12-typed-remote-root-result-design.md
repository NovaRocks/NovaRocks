# Typed Remote Root Result 设计

日期：2026-06-12
状态：Draft，等待评审
背景：PR #295 之后的 1FE+3BE 长期修复设计拆分

## 1. 背景

Standalone distributed execution 中，root fragment 可能运行在 remote BE。当前 coordinator
获取 root result 时存在一个不稳固边界：remote BE 返回接近 MySQL text/binary-like 的
`TResultBatch`，coordinator 再按照 analyzer output schema 反向解析成 Arrow `Chunk`。
这条路径在 decimal、largeint、binary、complex type 上都容易出错，因为“最终客户端显示”
和“BE 间 typed transport”被混在一起。

StarRocks 的参考实现说明了边界应该如何分层：BE 间 Exchange 使用 typed `ChunkPB`；
MySQL protocol result 只在最终 `MysqlResultWriter` 边界发生；Arrow Flight 另有 Arrow
result writer。NovaRocks 应该借鉴 typed internal transport，而不是把 MySQL text 当作
coordinator 内部数据交换格式。

## 2. 目标

- remote BE 到 coordinator 的 root result 使用 typed payload。
- MySQL/text formatting 只发生在最终客户端协议边界。
- coordinator 不再通过文本倒解析恢复 decimal/complex/binary 值。
- root result transport 与 Exchange 共用 execution schema contract。
- 保留兼容路径以便分阶段切换，但新 distributed SQL 默认走 typed result。

## 3. 非目标

- 不改变 StarRocks FE-compatible fetch result 对外协议。
- 不在本 spec 中实现 Arrow Flight。
- 不要求 standalone MySQL 客户端接收 Arrow。
- 不把所有 result sink 重写成一个新框架。

## 4. 协议形态

新增内部 typed root result payload：

```text
TypedResultBatch {
  schema_fingerprint,
  rows,
  columns_payload,
  slot_id_map,
  nullable_bitmap,
  encoding,
}
```

实际落地可以选择两种 wire encoding：

1. Arrow IPC stream / batch bytes。
2. NovaRocks `ChunkPB`，结构上参考 Exchange 的 chunk encoding。

推荐先选 NovaRocks `ChunkPB`，原因是它更贴近已有 `Chunk`、slot id 和
execution schema contract，且不需要一次性引入 Arrow IPC 的兼容矩阵。

## 5. 数据流

```text
Remote root fragment
  -> ResultBuffer stores typed Chunk
  -> FetchResult returns TypedResultBatch bytes + EOS
  -> Coordinator validates schema_fingerprint
  -> Coordinator reconstructs Chunk
  -> Standalone QueryResult carries typed Chunks
  -> MySQL server encodes final wire rows
```

coordinator 只负责 typed validation 和 chunk assembly，不做用户协议 formatting。

## 6. 组件边界

### `runtime/result_buffer.rs`

结果缓冲区需要支持两类输出：

- final protocol row batch：FE-compatible / legacy path。
- typed chunk batch：standalone distributed root result。

这两类不能共用“文本行”作为内部结构。

### `service/grpc_server.rs` / `service/grpc_client.rs`

`FetchResult` 增加 typed result 字段，或新增 `FetchTypedResult` RPC。为降低迁移风险，
推荐新增 RPC，保留旧 RPC。

### `runtime/coordinator.rs`

`ExecutionCoordinator` 对 standalone distributed path 优先调用 typed fetch。旧的
`coerce_fetch_chunks_to_output_columns` 保留为 legacy/fallback test helper，但不作为默认路径。

### `server/encoding.rs`

MySQL encoding 保持最终边界职责：输入是 typed `QueryResult`，输出是 MySQL protocol value。
这里可以使用 semantic output schema 做最终格式化。

## 7. 错误处理

- `schema_fingerprint` 不匹配：fail query，错误包含 query id、fragment id、expected/actual。
- typed payload decode 失败：fail query，不尝试文本 fallback。
- remote BE 只支持旧 fetch：在过渡期可显式降级，并打 warning；cutover 后移除。
- result buffer 被取消或超时：保持现有 coordinator cancel 语义。

## 8. StarRocks 借鉴点

可借鉴：

- BE 间数据用 typed chunk transport。
- final result writer 按 sink type 分流，MySQL 与 Arrow 是不同 writer。
- result manager 可以同时管理 row result 和 Arrow result schema。

不直接照搬：

- StarRocks 的 MySQL result writer 是面向 FE/client 的最终协议，不应作为 NovaRocks
  coordinator 内部 transport。

## 9. 落地顺序

1. 为 `Chunk` 增加 typed root result serialize/deserialize。
2. 新增 typed fetch RPC 和 in-process dispatcher 对应接口。
3. remote dispatcher 优先 typed fetch，legacy fetch 仅用于兼容旧 backend。
4. coordinator 删除默认 text coercion 路径。
5. SQL tests 开启 1FE+3BE targeted remote-root result cases。

## 10. 验证

- Unit tests 覆盖 typed result roundtrip：decimal、largeint、binary、utf8、list/struct。
- Integration tests 覆盖 root fragment 固定落在 remote BE 的 SELECT。
- Regression tests 覆盖此前 remote text coercion 相关的 decimal precision 问题。

## 11. 成功标准

- coordinator 不再从 MySQL text 反推 Arrow value。
- remote root result 与 local root result 在 `QueryResult` 层完全同构。
- 用户协议格式化只存在于最终 server/result sink 边界。
