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

import java.nio.file.{Files, Paths}
import org.apache.iceberg.shaded.org.apache.datasketches.memory.Memory
import org.apache.iceberg.shaded.org.apache.datasketches.theta.{CompactSketch, SetOperation, UpdateSketch}

val interopRoot = Paths.get(sys.env("NOVA_THETA_INTEROP_ROOT"))
val novaPath = interopRoot.resolve("fixtures/theta/rust_quickselect_n1000_ordered_v3.sk")
val novaCompressedPath = interopRoot.resolve("fixtures/theta/rust_quickselect_n100000_ordered_v4.sk")
val outputPath = interopRoot.resolve("spark_disjoint_n1000.sk")

def read(path: java.nio.file.Path): CompactSketch =
  CompactSketch.heapify(Memory.wrap(Files.readAllBytes(path)))

def assertEstimate(label: String, actual: Double, expected: Double): Unit =
  require(java.lang.Double.doubleToLongBits(actual) == java.lang.Double.doubleToLongBits(expected),
    s"$label: actual=$actual expected=$expected")

val nova = read(novaPath)
assertEstimate("Spark reads Nova exact compact", nova.getEstimate, 1000.0)

val compressed = read(novaCompressedPath)
require(compressed.isEstimationMode && compressed.getEstimate >= 90000.0 && compressed.getEstimate <= 110000.0,
  s"Spark failed to read Nova compressed compact: ${compressed.getEstimate}")

val sparkSketch = UpdateSketch.builder().setNominalEntries(4096).build()
(1000L until 2000L).foreach(sparkSketch.update)
val sparkCompact = sparkSketch.compact(true, null)
assertEstimate("Spark producer", sparkCompact.getEstimate, 1000.0)
Files.write(outputPath, sparkCompact.toByteArray)

val union = SetOperation.builder().setNominalEntries(4096).buildUnion()
union.union(nova)
union.union(sparkCompact)
assertEstimate("Spark unions Nova and Spark", union.getResult.getEstimate, 2000.0)
println("NOVAROCKS_SPARK_THETA_INTEROP_OK nova=1000 spark=1000 union=2000")
