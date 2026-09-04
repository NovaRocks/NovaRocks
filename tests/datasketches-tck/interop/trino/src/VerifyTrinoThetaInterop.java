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

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;

import io.airlift.slice.Slice;
import io.trino.plugin.iceberg.aggregation.DataSketchState;
import io.trino.plugin.iceberg.aggregation.IcebergThetaSketchForStats;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.datasketches.theta.CompactThetaSketch;
import org.apache.datasketches.theta.UpdatableThetaSketch;

/** Runs Trino's production Iceberg aggregate in both producer and consumer directions. */
public final class VerifyTrinoThetaInterop {
  private VerifyTrinoThetaInterop() {}

  public static void main(final String[] args) throws IOException {
    if (args.length != 3) {
      throw new IllegalArgumentException(
          "usage: VerifyTrinoThetaInterop NOVA_COMPACT NOVA_COMPRESSED_COMPACT OUTPUT");
    }
    final State trino = new State();
    final BlockBuilder values = BIGINT.createFixedSizeBlockBuilder(1000);
    for (long value = 1000; value < 2000; value++) {
      BIGINT.writeLong(values, value);
    }
    final ValueBlock valueBlock = values.buildValueBlock();
    for (int index = 0; index < valueBlock.getPositionCount(); index++) {
      IcebergThetaSketchForStats.input(BIGINT, trino, valueBlock, index);
    }
    assertEstimate("Trino producer", trino.getUpdateSketch().getEstimate(), 1000.0);

    final byte[] trinoCompact = output(trino);
    Files.write(Paths.get(args[2]), trinoCompact);
    assertEstimate("Trino output is readable", CompactThetaSketch.wrap(trinoCompact).getEstimate(),
        1000.0);

    final State nova = new State();
    nova.setCompactSketch(CompactThetaSketch.wrap(Files.readAllBytes(Paths.get(args[0]))));
    assertEstimate("Trino reads Nova compact", nova.getCompactSketch().getEstimate(), 1000.0);
    final CompactThetaSketch novaCompressed =
        CompactThetaSketch.wrap(Files.readAllBytes(Paths.get(args[1])));
    if (!novaCompressed.isEstimationMode() || novaCompressed.getEstimate() < 90_000.0
        || novaCompressed.getEstimate() > 110_000.0) {
      throw new AssertionError(
          "Trino failed to read Nova compressed compact: " + novaCompressed.getEstimate());
    }
    IcebergThetaSketchForStats.combine(trino, nova);
    if (trino.getUpdateSketch() != null || trino.getCompactSketch() == null) {
      throw new AssertionError("Trino combine did not replace mutable state with compact state");
    }
    assertEstimate("Trino unions Nova and Trino", trino.getCompactSketch().getEstimate(), 2000.0);
    assertEstimate("Trino union output", CompactThetaSketch.wrap(output(trino)).getEstimate(),
        2000.0);
    System.out.println("verified Trino theta interop: nova=1000 trino=1000 union=2000");
  }

  private static byte[] output(final State state) {
    final BlockBuilder output = VARBINARY.createBlockBuilder(null, 1);
    IcebergThetaSketchForStats.output(state, output);
    final Block block = output.build();
    final Slice slice = VARBINARY.getSlice(block, 0);
    return slice.getBytes();
  }

  private static void assertEstimate(final String label, final double actual,
      final double expected) {
    if (Double.doubleToLongBits(actual) != Double.doubleToLongBits(expected)) {
      throw new AssertionError(label + ": actual=" + actual + ", expected=" + expected);
    }
  }

  private static final class State implements DataSketchState {
    private UpdatableThetaSketch updateSketch;
    private CompactThetaSketch compactSketch;

    @Override
    public UpdatableThetaSketch getUpdateSketch() {
      return updateSketch;
    }

    @Override
    public void setUpdateSketch(final UpdatableThetaSketch value) {
      updateSketch = value;
    }

    @Override
    public CompactThetaSketch getCompactSketch() {
      return compactSketch;
    }

    @Override
    public void setCompactSketch(final CompactThetaSketch value) {
      compactSketch = value;
    }

    @Override
    public long getEstimatedSize() {
      return 0;
    }
  }
}
