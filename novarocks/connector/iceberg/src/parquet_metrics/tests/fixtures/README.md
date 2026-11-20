# Parquet footer metric references

The Iceberg Java 1.12.0 comparison uses the same C16 Parquet bytes as the native
Iceberg compatibility suite. The input SHA-256 is
`b90e7a6aafaeb38254b6a0b450f0b1b9775142ef85137f4af19f7414e3813de7`. The expected metrics came from
`ParquetUtil.fileMetrics` at Iceberg commit
`212aa894e919ff9fdf89160da4aadcbe6858ac01` with the default `truncate(16)` mode.
The test checks the Iceberg LONG result after promoting the source INT32 value.

The other two fixtures were registered in Trino 484 at commit
`e2d50bb733e193607c44be84ad80ce2430ba1d01`. The additional unbound field uses
Parquet INT96 with field ID 99. Both runs produced 113 bytes and 3 values for that
field. Only the fixture with complete footer NULL counts produced a NULL count.

| File | Decoded SHA-256 |
|---|---|
| `embedded-extra-int96.parquet` | `40d50cc969e8d52a1aec843ea07aab399797c82caffbca6155e608fb30d39906` |
| `embedded-extra-int96-known-nulls.parquet` | `09ff06c31f5780a3a0e2e0b027aba831115807276387ca70b98499b56328f43a` |
