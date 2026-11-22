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

"""Check publication fencing and the fresh output spec on one isolated table."""

import json
import sys
import urllib.request

from verify_interleaving import check_table_name, evidence_dir, spark, spark_dml, traffic

CASE = "rwm17b_optimize_default_spec_change"


def load_metadata(uri, table):
    namespace, name = table.split(".")
    with urllib.request.urlopen(
        f"{uri.rstrip('/')}/v1/namespaces/{namespace}/tables/{name}", timeout=10
    ) as response:
        return json.load(response)["metadata"]


def assignment(metadata):
    return {
        "head": metadata["refs"]["main"]["snapshot-id"],
        "default": metadata["default-spec-id"],
        "snapshots": sorted(snapshot["snapshot-id"] for snapshot in metadata["snapshots"]),
    }


def evolve(uri, table, evidence):
    original = assignment(load_metadata(uri, table))
    # The held phase runs a single independent Spark write. Verification of
    # file spec IDs and row contents happens after the original statement exits.
    spark_dml(
        f"ALTER TABLE ice_rest.{table} ADD PARTITION FIELD bucket(4, id)",
        evidence / "spark-spec-evolution.log",
    )
    metadata = load_metadata(uri, table)
    changed = assignment(metadata)
    if (changed["head"] != original["head"] or changed["snapshots"] != original["snapshots"]
            or changed["default"] == original["default"]
            or original["default"] not in {spec["spec-id"] for spec in metadata["partition-specs"]}):
        raise RuntimeError(f"expected metadata-only evolution retaining the old spec: {original}, {changed}")
    with (evidence / "spec-evolution.json").open("x") as saved:
        json.dump({"original": original, "changed": changed}, saved, sort_keys=True)


def rejected(uri, table, evidence, binding):
    actual = assignment(load_metadata(uri, table))
    with (evidence / "spec-rejected.json").open("x") as saved:
        json.dump(actual, saved, sort_keys=True)
    if actual != binding["changed"]:
        raise RuntimeError(f"rejected OPTIMIZE changed the Catalog contents: {actual}")
    baseline = json.loads((evidence / "traffic-before.json").read_text())
    final = traffic(uri)
    with (evidence / "traffic-after.json").open("x") as saved:
        json.dump(final, saved, sort_keys=True)
    delta = {
        "conflicts": final["by_status"].get("409", 0) - baseline["by_status"].get("409", 0),
        "commit_requests": final["table_commit_requests"] - baseline["table_commit_requests"],
    }
    if delta != {"conflicts": 1, "commit_requests": 1}:
        raise RuntimeError(f"expected exactly one rejected OPTIMIZE dispatch: {delta}")
    print("RWM17B_OPTIMIZE_SPEC_GUARD_OK")


def readback(table, evidence, binding):
    old_head = int(binding["original"]["head"])
    new_spec = int(binding["changed"]["default"])
    marker = "RWM17B_OPTIMIZE_SPEC_READBACK_OK"
    spark(f"""
val name = "ice_rest.{table}"
val table = Iru5SparkCommitInterop.table(spark, name)
table.refresh()
val latest = table.currentSnapshot()
require(latest.operation() == "replace" && latest.parentId().longValue() == {old_head}L,
  "The new OPTIMIZE must replace the original main snapshot")
require(table.spec().specId() == {new_spec}, "Default spec did not survive the rejected OPTIMIZE")
val scan = table.newScan().planFiles()
val outputSpecs = try scan.asScala.map(_.spec().specId()).toSet finally scan.close()
require(outputSpecs == Set({new_spec}), s"unexpected output specs $outputSpecs")
val actual = spark.sql(s"SELECT id, value FROM $name ORDER BY id").collect()
  .map(row => (row.getLong(0), row.getLong(1))).toSeq
require(actual == Seq((1L, 10L), (2L, 20L), (3L, 30L)), s"unexpected rows $actual")
""", marker, evidence / "spark-spec-readback.log")
    print(marker)


def main():
    action, uri, run, table = sys.argv[1:]
    check_table_name(table)
    evidence = evidence_dir(CASE, run)
    if action == "evolve":
        evolve(uri, table, evidence)
        return
    binding = json.loads((evidence / "spec-evolution.json").read_text())
    if action == "rejected":
        rejected(uri, table, evidence, binding)
    elif action == "readback":
        readback(table, evidence, binding)
    else:
        raise ValueError(f"unexpected guard operation: {action}")


if __name__ == "__main__":
    main()
