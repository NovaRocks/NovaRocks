# 对象存储与 FileIO

> Iceberg 的 FileIO 抽象决定 data file / manifest / Puffin / metadata.json 实际写到哪。

| 后端 | 状态 | 入口 / 备注 |
| --- | --- | --- |
| 本地文件系统 | ✅ | `src/fs/local.rs` |
| HDFS | ✅ | `src/fs/hdfs.rs` |
| S3 / S3-compatible（含 MinIO） | ✅ | `src/fs/object_store.rs`、`src/fs/opendal.rs` |
| 阿里云 OSS | ✅ | |
| Azure Blob / ADLS Gen2 | ❌ | |
| Google Cloud Storage（GCS） | ❌ | |
| 腾讯 COS | ❌ | |
| 华为云 OBS | ❌ | |
| FileIO 加密（S3 SSE-KMS / SSE-C） | ❌ | |
| FileIO checksum（v3 `content-checksum`） | ❌ | |

---

## ✅ 本地文件系统

```sql
CREATE EXTERNAL CATALOG ice
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "warehouse" = "file:///var/iceberg/warehouse"
);
```

适合本地学习 / 开发 / 单测；生产请用对象存储或 HDFS。

## ✅ HDFS

```sql
CREATE EXTERNAL CATALOG ice
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "warehouse" = "hdfs://nameservice1/iceberg"
);
```

> TODO: 列出可识别的 HDFS / Kerberos 相关 properties。

## ✅ S3 / S3-compatible（含 MinIO）

```sql
CREATE EXTERNAL CATALOG ice
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "warehouse" = "s3://my-bucket/wh",
  "s3.endpoint" = "http://minio:9000",
  "s3.access-key" = "...",
  "s3.secret-key" = "...",
  "s3.region" = "us-east-1"
);
```

> TODO: 列出完整的 `s3.*` / `aws.*` properties 与 NovaRocks 的对应关系；标注哪些会在未来切到 catalog credential vending。

## ✅ 阿里云 OSS

```sql
"warehouse" = "oss://my-bucket/wh"
"oss.endpoint" = "oss-cn-hangzhou.aliyuncs.com"
"oss.access-key" = "..."
"oss.secret-key" = "..."
```

> TODO: 列出完整 OSS properties。

## ❌ Azure Blob / ADLS Gen2

Spec：`abfs://` / `abfss://` / `wasb://` URI scheme + `azure.account-key` / `azure.tenant-id` 等 properties。

**TODO**：未实现，FileIO factory 不识别 Azure scheme，建表会报错"unsupported scheme"。

## ❌ Google Cloud Storage（GCS）

Spec：`gs://` URI scheme + service-account / ADC 鉴权。

**TODO**：未实现。

## ❌ 腾讯云 COS / 华为云 OBS

Spec：`cosn://` / `obs://` URI scheme。

**TODO**：未实现。两者均与 S3 协议兼容，理论上配 `s3.endpoint` 走 S3 backend 可以绕过去，但 NovaRocks 没有官方 fixture 验证过。

## ❌ FileIO 加密

Spec：S3 SSE-KMS / SSE-C 透传到 putObject 调用，让对象存储层做加密。

**TODO**：未实现。当前所有 S3 / OSS 写入都是明文（依赖 bucket 级别的默认加密）。

## ❌ FileIO checksum（V3 `content-checksum`）

Spec：v3 manifest 在每个 data file / delete file 上记录 `content-checksum`（CRC32 / xxhash），读端可以二次校验防 bit rot。

**TODO**：未实现。当前写出的 manifest entry 不含 `content-checksum` 字段，读端也不校验。
