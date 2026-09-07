/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.novarocks.tck;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/** Independent Iceberg conversion and DataSketches Java oracle for NCP-8. */
public final class GenerateIcebergThetaVectors {
  private static final long DEFAULT_SEED = 9001L;

  private GenerateIcebergThetaVectors() {}

  public static void main(final String[] args) throws IOException {
    if (args.length != 1) {
      throw new IllegalArgumentException("usage: GenerateIcebergThetaVectors OUTPUT_DIR");
    }
    final Path theta = Paths.get(args[0]).resolve("theta");
    Files.createDirectories(theta);
    final List<String> lines = new ArrayList<>();
    lines.add("# Apache Iceberg 1.10.0 Conversions + DataSketches Java 6.2.0 QuickSelect, lg_k=12, seed=9001");
    lines.add("# label\tcanonical_value_hex\tordered_compact_v3_hex");

    add(lines, "boolean_false", Types.BooleanType.get(), false);
    add(lines, "boolean_true", Types.BooleanType.get(), true);
    add(lines, "int32_negative", Types.IntegerType.get(), -123456789);
    add(lines, "int64_negative", Types.LongType.get(), -1234567890123456789L);
    add(lines, "float_negative_zero", Types.FloatType.get(), Float.intBitsToFloat(0x80000000));
    add(lines, "float_nan_payload", Types.FloatType.get(), Float.intBitsToFloat(0x7fc00001));
    add(lines, "double_negative_zero", Types.DoubleType.get(), Double.longBitsToDouble(0x8000000000000000L));
    add(lines, "double_nan_payload", Types.DoubleType.get(), Double.longBitsToDouble(0x7ff8000000000001L));
    add(lines, "decimal_negative", Types.DecimalType.of(38, 4),
        new BigDecimal(new BigInteger("-129"), 4));
    add(lines, "date_negative", Types.DateType.get(), -12345);
    add(lines, "time_micros", Types.TimeType.get(), 1234567890L);
    add(lines, "timestamp_micros", Types.TimestampType.withoutZone(), -1234567890123L);
    add(lines, "timestamp_nanos", Types.TimestampNanoType.withoutZone(), 1234567890123456789L);
    add(lines, "utf8", Types.StringType.get(), "NovaRocks-\u96ea");
    add(lines, "utf8_empty", Types.StringType.get(), "");
    add(lines, "binary", Types.BinaryType.get(), ByteBuffer.wrap(new byte[] {0, 1, -1, 127}));
    add(lines, "binary_empty", Types.BinaryType.get(), ByteBuffer.wrap(new byte[0]));
    add(lines, "fixed", Types.FixedType.ofLength(4), ByteBuffer.wrap(new byte[] {0, 1, -1, 127}));
    add(lines, "uuid", Types.UUIDType.get(), UUID.fromString("00112233-4455-6677-8899-aabbccddeeff"));

    Files.write(theta.resolve("iceberg_java62_single_value_vectors.tsv"), lines,
        StandardCharsets.UTF_8);
  }

  private static void add(final List<String> lines, final String label, final Type type,
      final Object value) {
    final ByteBuffer canonical = Conversions.toByteBuffer(type, value);
    final byte[] canonicalBytes = remainingBytes(canonical);
    final UpdateSketch sketch = UpdateSketch.builder()
        .setFamily(Family.QUICKSELECT)
        .setNominalEntries(4096)
        .setSeed(DEFAULT_SEED)
        .build();
    sketch.update(canonical.duplicate());
    final CompactSketch compact = sketch.compact(true, null);
    lines.add(label + "\t" + hex(canonicalBytes) + "\t" + hex(compact.toByteArray()));
  }

  private static byte[] remainingBytes(final ByteBuffer buffer) {
    final ByteBuffer copy = buffer.duplicate();
    final byte[] bytes = new byte[copy.remaining()];
    copy.get(bytes);
    return bytes;
  }

  private static String hex(final byte[] bytes) {
    final StringBuilder output = new StringBuilder(bytes.length * 2);
    for (final byte value : bytes) {
      output.append(String.format("%02x", value & 0xff));
    }
    return output.toString();
  }
}
