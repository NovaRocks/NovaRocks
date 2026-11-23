# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Verify CREATE placement, snapshot facts, and independent Spark readback."""

import json
import re
import sys

from verify_interleaving import WORKSPACE, spark


def check(action, token, warehouse):
    logs = WORKSPACE / "logs/rwm17a/sql-ctas-contracts" / token
    logs.mkdir(parents=True, exist_ok=True)
    namespace = json.dumps(f"ice_rest.ctas_contract_{token}")
    root = json.dumps(f"{warehouse}/ctas-contract-{token}")
    old_staging = json.dumps(f"{warehouse}/_novarocks/ctas-staging/v1/")
    before = logs / "before.log"
    common = f"""
val namespace = {namespace}
val root = {root}
def identifier(name: String) = s"$namespace.$name"
def load(name: String) = Iru5SparkCommitInterop.table(spark, identifier(name))
val source = load("source_rows")
val explicit = load("explicit_location")
val io = explicit.io().asInstanceOf[org.apache.iceberg.io.SupportsPrefixOperations]
def objects(path: String) = io.listPrefix(path).asScala.map(_.location()).toVector.sorted
objects(root + "/fixed-location/").foreach(path => println("CTAS_OWNED_OBJECT:" + path))
"""
    if action == "capture":
        spark(common, "RWM17A_CREATE_PREFIX_CAPTURED", before)
        if not object_lines(before):
            raise AssertionError("The occupied location contains no objects")
        print("RWM17A_CREATE_PREFIX_CAPTURED")
        return

    after = logs / "after.log"
    spark(
        common
        + f"""
val uuid = "[0-9a-f]{{32}}"
def generated(table: org.apache.iceberg.Table, name: String) =
  table.location().matches(java.util.regex.Pattern.quote(root + "/" + name + "-") + uuid)
require(generated(source, "source_rows"), "Ordinary CREATE did not use the namespace location")
require(explicit.location() == root + "/fixed-location", "CTAS changed its explicit location")
require(!spark.catalog.tableExists(identifier("rejected_create")), "Rejected CREATE registered a table")
require(!spark.catalog.tableExists(identifier("rejected_ctas")), "Rejected CTAS registered a table")
for ((name, version) <- Seq("default_format" -> 2, "empty_result" -> 2,
   "version_one" -> 1, "version_two" -> 2, "version_three" -> 3)) {{
  val table = load(name)
  val metadata = table.asInstanceOf[org.apache.iceberg.HasTableOperations].operations().current()
  require(metadata.formatVersion() == version, s"$name did not preserve format version $version")
  require(generated(table, name), s"$name did not use the namespace location")
  val current = table.currentSnapshot()
  require(table.snapshots().asScala.size == 1 && current.operation() == "append",
    s"$name must have a single append snapshot")
  if (name == "empty_result") {{
    val planned = table.newScan().planFiles()
    try require(!planned.iterator().hasNext(), "Empty CTAS added a data file")
    finally planned.close()
    require(table.statisticsFiles().isEmpty(), "Empty CTAS wrote a statistics file")
  }} else if (name == "default_format") {{
    val rows = spark.sql(s"SELECT id, v FROM $namespace.$name ORDER BY id").collect().toVector
      .map(row => (row.getInt(0), row.getInt(1)))
    require(rows == Vector((1, 10), (2, 20), (3, 10)), "Spark read different CTAS rows")
    val statistics = table.statisticsFiles().asScala.toVector
    require(statistics.size == 1 && statistics.head.snapshotId() == current.snapshotId(),
      "CTAS statistics must refer to its only append snapshot")
    val theta = statistics.head.blobMetadata().asScala.toVector
      .filter(_.`type`() == org.apache.iceberg.puffin.StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1)
    require(theta.flatMap(_.fields().asScala).toSet == table.schema().columns().asScala.map(_.fieldId()).toSet,
      "CTAS omitted a Theta field")
    require(theta.map(_.properties().get("ndv")).sorted == Vector("2", "3"), "CTAS NDV differs")
  }}
}}
require(objects({old_staging}).isEmpty, "CREATE wrote objects under the former CTAS staging root")
""",
        "RWM17A_CREATE_CONTRACTS_OK",
        after,
    )
    if object_lines(before) != object_lines(after):
        raise AssertionError("Rejected CREATE or CTAS changed the occupied location's objects")
    print("RWM17A_CREATE_CONTRACTS_OK")


def object_lines(log):
    return {line for line in log.read_text().splitlines() if line.startswith("CTAS_OWNED_OBJECT:")}


if __name__ == "__main__":
    action, token, warehouse = sys.argv[1:]
    if action not in {"capture", "verify"} or not re.fullmatch(r"[a-zA-Z_0-9]+", token):
        raise ValueError("Invalid CTAS verification action or fixture token")
    check(action, token, warehouse)
