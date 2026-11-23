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

"""Check the original CTAS output and the competing Spark tables independently."""

import json
import re
import sys

from verify_interleaving import WORKSPACE, spark


RACE_ROWS = {"raced_plain": 91, "raced_if_not_exists": 92}
FORBIDDEN = "forbidden_after_write"
UNKNOWN = "unobserved_commit"


def storage_probe(namespace, warehouse):
    root = json.dumps(f"{warehouse}/ns_{namespace}/rwm17a_ctas_")
    return f"""
val source = Iru5SparkCommitInterop.table(spark, "ice_rest.ns_{namespace}.source_rows")
val storage = source.io().asInstanceOf[org.apache.iceberg.io.SupportsPrefixOperations]
def output(name: String) = storage.listPrefix({root} + name + "/")
  .asScala.map(_.location()).toVector
"""


def race(namespace, warehouse, table, logs):
    winner = RACE_ROWS[table]
    spark(
        storage_probe(namespace, warehouse)
        + f"""
val name = "ice_rest.ns_{namespace}.{table}"
require(!spark.catalog.tableExists(name), "The competing CTAS target was already visible")
val loserFiles = output("{table}")
require(loserFiles.exists(_.endsWith(".parquet")), "The held CTAS produced no data file")
println("NovaRocks staged output: " + loserFiles.sorted.mkString(", "))
spark.sql(s"CREATE TABLE $name (id INT) USING iceberg")
spark.sql(s"INSERT INTO $name VALUES ({winner})")
""",
        "RWM17A_CTAS_RACE_WINNER_COMMITTED",
        logs / f"{table}.log",
    )


def verify(namespace, warehouse, logs):
    spark(
        storage_probe(namespace, warehouse)
        + f"""
for (name <- Seq("raced_plain", "raced_if_not_exists", "{FORBIDDEN}")) {{
  require(output(name).isEmpty, "Known-uncommitted CTAS retained owned objects at " + name)
}}
require(!spark.catalog.tableExists("ice_rest.ns_{namespace}.{FORBIDDEN}"),
  "The rejected final CREATE registered a table")
val qualified = "ice_rest.ns_{namespace}.{UNKNOWN}"
val table = Iru5SparkCommitInterop.table(spark, qualified)
val current = table.currentSnapshot()
require(table.snapshots().asScala.size == 1 && current.operation() == "append",
  "The unobserved CREATE must have exactly one append")
val objects = output("{UNKNOWN}").toSet
require(objects.contains(current.manifestListLocation()), "Missing committed manifest list")
require(current.allManifests(table.io()).asScala.forall(manifest => objects.contains(manifest.path())),
  "Missing committed manifest")
val tasks = table.newScan().planFiles()
try {{
  val dataPaths = tasks.asScala.map(_.file().location()).toVector
  require(dataPaths.nonEmpty && dataPaths.forall(objects.contains), "Missing committed data")
}} finally tasks.close()
val rows = spark.sql(s"SELECT id FROM $qualified ORDER BY id").collect().toVector.map(_.getInt(0))
require(rows == Vector(1, 2, 3), "Readback differs after the unobserved CREATE")
""",
        "RWM17A_CTAS_PUBLICATION_FILES_OK",
        logs / "verify.log",
    )
    print("RWM17A_CTAS_PUBLICATION_FILES_OK")


if __name__ == "__main__":
    action, namespace, warehouse, *arguments = sys.argv[1:]
    if not re.fullmatch(r"[a-zA-Z_0-9]+", namespace):
        raise ValueError(f"invalid fixture namespace suffix: {namespace!r}")
    logs = WORKSPACE / "logs/rwm17a/sql-ctas" / namespace
    logs.mkdir(parents=True, exist_ok=True)
    if action == "race":
        table, = arguments
        race(namespace, warehouse, table, logs)
    elif action == "verify" and not arguments:
        verify(namespace, warehouse, logs)
    else:
        raise ValueError(f"invalid CTAS verification action: {action!r}")
