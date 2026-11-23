# Trino 484 时区规则

此目录的生成器使用锁定 Trino 镜像中的公开 JDK、Joda 和 Trino API。输出供 Rust 查询，日常构建直接消费仓库内的数据。

- Trino 源码为 `e2d50bb733e193607c44be84ad80ce2430ba1d01`。编号来自 Apache-2.0 许可的 Trino `zone-index.properties`，包括原有编号空洞和有独立编号的地区别名。 [Trino 时区索引](https://github.com/trinodb/trino/blob/e2d50bb733e193607c44be84ad80ce2430ba1d01/core/trino-spi/src/main/resources/io/trino/spi/type/zone-index.properties)
- 原始规则来自 Temurin `25.0.4+7` 的 TZDB `2026b` 和 Apache-2.0 许可的 Joda-Time `2.15.0`。生成文件包含时区编号、转换时刻及钟面偏移。 [IANA 数据及许可](https://data.iana.org/time-zones/tzdb-2026b/LICENSE) · [Joda-Time 许可](https://github.com/JodaOrg/joda-time/blob/v2.15.0/LICENSE.txt)
- arm64 参照镜像 ID 为 `sha256:2704eb53935af5e582d63d6033e9bf99d20c325e9b0e30b1016ef59a4773979f`。JDK `tzdb.dat` 的 SHA-256 为 `5906b44e25a8a730131d7ba1beb5d15af27043f6536f1a169ca8a5d8b8bed8f8`。Joda-Time JAR 为 `6bbe1302e5bec0f03eb7e7442334b475bb225361a5498369d6a0a9a472630c33`。镜像内索引为 `3f2e2ce24a9b6adeb8b5ce26c426159cdfa71ab5ab588f8d11c2bb83b3cc9507`。

生成器遍历整个带区时间戳值域及相邻偏移覆盖。若从 2400 年起的所有转换点、偏移、周期接缝和区间后的首个转换均与 400 年模板一致，就保存该模板。其它表保留完整有限段。Joda 的同偏移转换事件仍被保留。

从仓库根目录运行以下命令，先把 `output` 指向一个新的独占目录的绝对路径。

```bash
docker/fixture-inputs/provision.sh --consumer image:trino --result "$output/input.json"
image=$(jq -er '.images.trino.image_id' "$output/input.json")
docker run --rm --pull never \
  -v "$PWD/novarocks/functions/temporal-rules:/generator:ro" \
  -v "$output:/output" --entrypoint java "$image" \
  -cp '/usr/lib/trino/lib/*' /generator/ExportTemporalRules.java /output
cmp "$output/temporal_zone_ids.rs" novarocks/type-contract/src/time_zone_key/temporal_zone_ids.rs
cmp "$output/temporal_zone_rules.bin" novarocks/functions/src/temporal_zone_rules.bin
```

同一输入生成两次并比较字节。规则更新时同时核对本地转换及渲染策略，再协调 Native 计划契约版本。
