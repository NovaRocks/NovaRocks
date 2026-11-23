# Iceberg ADD FILES verification inputs

The Spark helper uses the selected Iceberg compatibility fixture to author source
files and inspect published snapshots through Iceberg's table and file APIs. Each
SQL case owns its namespace and the fixture's allocated object-store prefix.

`metrics.parquet.b64` decodes to the exact RWM-17c P01 Parquet input. PyArrow
23.0.1 wrote two row groups of two rows each. The author removed only the
`null_count` fields for the second row group's `id` and `details.flag` footer
statistics. A read of the final input returned the same four independently
specified rows. The decoded file's SHA-256 is
`b90e7a6aafaeb38254b6a0b450f0b1b9775142ef85137f4af19f7414e3813de7`.

`metrics-mapping.json` contains the effective mapping from the same P01 input.
`verifyReferenceMetrics` compares actual manifest metrics with
`ParquetUtil.fileMetrics` from Iceberg Java 1.12.0
(`212aa894e919ff9fdf89160da4aadcbe6858ac01`), with default `truncate(16)`.
The SHA-256 of `iceberg-parquet-1.12.0.jar` is
`fba0355f0d54bd68e21170e0e5f05632845d42ba346945017d2156e66297b85f`.
The reference's physical INT32 bounds for `id` are interpreted as Iceberg LONG
bounds when comparing the target manifest.

`legacy-array.parquet.b64` decodes to P01's single-row Spark 3.5.5 file. Spark
wrote it with `spark.sql.parquet.writeLegacyFormat=true`. The physical leaf
path is `items.bag.array.value`. The decoded file's SHA-256 is
`9f6bfa14ff3516636854c83e4793f86df8d2892278fbf3a9fd0b12d1c96ce2c2`.

`trc6c-wide-zoned-micros.parquet.b64` contains one raw INT64 value,
`2251799813685248000`. PyArrow 23.0.1 wrote it as an optional UTC-adjusted
microsecond timestamp with field ID 1 using Parquet 2.6, without compression,
dictionary encoding, optional statistics or an embedded Arrow schema.
Reading the file as INT64 reproduces the original value. Its decoded
SHA-256 is `7b2862f55a4fed09a4cd6b809d242009ba051fce99474258936b7be8d5604e1f`.
