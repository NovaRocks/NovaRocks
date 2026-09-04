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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;

/** Independent Java reader, producer, and union oracle for NovaRocks Theta compacts. */
public final class VerifyBidirectionalTheta {
  private VerifyBidirectionalTheta() {}

  public static void main(final String[] args) throws IOException {
    if (args.length != 2) {
      throw new IllegalArgumentException("usage: VerifyBidirectionalTheta FIXTURE_ROOT OUTPUT");
    }
    final Path fixtureRoot = Paths.get(args[0]);
    final CompactSketch novaExact = read(
        fixtureRoot.resolve("theta/rust_quickselect_n1000_ordered_v3.sk"));
    assertEstimate("Java reads Nova exact compact", novaExact.getEstimate(), 1000.0);

    final CompactSketch novaCompressed = read(
        fixtureRoot.resolve("theta/rust_quickselect_n100000_ordered_v4.sk"));
    if (!novaCompressed.isEstimationMode() || novaCompressed.getEstimate() < 90_000.0
        || novaCompressed.getEstimate() > 110_000.0) {
      throw new AssertionError("Java failed to read Nova compressed compact: "
          + novaCompressed.getEstimate());
    }

    final UpdateSketch javaSketch = UpdateSketch.builder().setNominalEntries(4096).build();
    for (long value = 1000; value < 2000; value++) {
      javaSketch.update(value);
    }
    final CompactSketch javaCompact = javaSketch.compact(true, null);
    assertEstimate("Java producer", javaCompact.getEstimate(), 1000.0);
    Files.write(Paths.get(args[1]), javaCompact.toByteArray());

    final Union union = SetOperation.builder().setNominalEntries(4096).buildUnion();
    union.union(novaExact);
    union.union(javaCompact);
    assertEstimate("Java unions Nova and Java", union.getResult().getEstimate(), 2000.0);
    System.out.println("verified Java theta interop: nova=1000 java=1000 union=2000");
  }

  private static CompactSketch read(final Path path) throws IOException {
    return CompactSketch.heapify(Memory.wrap(Files.readAllBytes(path)));
  }

  private static void assertEstimate(final String label, final double actual,
      final double expected) {
    if (Double.doubleToLongBits(actual) != Double.doubleToLongBits(expected)) {
      throw new AssertionError(label + ": actual=" + actual + ", expected=" + expected);
    }
  }
}
