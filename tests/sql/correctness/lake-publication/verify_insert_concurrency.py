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

"""Check exact INSERT effects against the held REST commit and Spark."""

import json
import re
import sys

from verify_interleaving import (
    WORKSPACE,
    check_table_name,
    spark,
    spark_dml,
    traffic,
)
from verify_optimize_default_spec import load_metadata

MARKER = "novarocks.write.session.v1"
MODES = ("contend", "schema", "spec", "replace", "unknown")


def write(path, value):
    with path.open("x") as target:
        json.dump(value, target, sort_keys=True)


def snapshots(metadata):
    return {snapshot["snapshot-id"]: snapshot for snapshot in metadata["snapshots"]}


def head(metadata):
    return metadata["refs"]["main"]["snapshot-id"]


def assert_append(original, result, added_rows, nova):
    old, current = snapshots(original), snapshots(result)
    assert len(current) == len(old) + 1, "expected one new snapshot"
    assert all(current.get(key) == snapshot for key, snapshot in old.items()), "history changed"
    new = current[head(result)]
    assert new["parent-snapshot-id"] == head(original), "append did not extend the observed head"
    assert new["summary"]["operation"] == "append", "no append snapshot"
    assert new["summary"]["added-records"] == str(added_rows), "append row count differs"
    assert new["added-rows"] == added_rows, "v3 added row count differs"
    assert new["first-row-id"] == original["next-row-id"], "v3 row lineage did not extend"
    assert result["next-row-id"] == original["next-row-id"] + added_rows, "row high-water changed"
    if nova:
        marker = new["summary"][MARKER]
        assert marker and sum(s["summary"].get(MARKER) == marker for s in current.values()) == 1, (
            "expected one exact write session marker"
        )
    return new


def companion(uri, table, mode, root, original):
    qualified = f"ice_rest.{table}"
    log = root / "spark-companion.log"
    if mode == "replace":
        spark(f'''
val name = "{qualified}"
spark.sql(s"DROP TABLE $name").collect()
require(!spark.catalog.tableExists(name), "target was not dropped")
spark.sql(s"CREATE TABLE $name (id BIGINT, value BIGINT) USING iceberg " +
  "TBLPROPERTIES ('format-version'='3')").collect()
spark.sql(s"INSERT INTO $name VALUES (8, 80)").collect()
''', "RWM17A_INSERT_REPLACEMENT_CREATED", log)
    else:
        statement = {
            "contend": f"INSERT INTO {qualified} VALUES (4, 40)",
            "schema": f"ALTER TABLE {qualified} ADD COLUMN new_value BIGINT",
            "spec": f"ALTER TABLE {qualified} ADD PARTITION FIELD bucket(4, id)",
        }[mode]
        spark_dml(statement, log)
    observed = load_metadata(uri, table)
    if mode == "contend":
        assert original["properties"]["commit.retry.num-retries"] == "0"
        assert_append(original, observed, 1, nova=False)
    elif mode == "schema":
        assert head(observed) == head(original) and snapshots(observed) == snapshots(original)
        assert observed["current-schema-id"] != original["current-schema-id"]
        schema = next(
            s for s in observed["schemas"] if s["schema-id"] == observed["current-schema-id"]
        )
        field = next(field for field in schema["fields"] if field["name"] == "new_value")
        assert field["type"] == "long" and field["required"] is False
    elif mode == "spec":
        assert head(observed) == head(original) and snapshots(observed) == snapshots(original)
        assert (original["default-spec-id"], observed["default-spec-id"]) == (0, 1)
        specs = {spec["spec-id"]: spec for spec in observed["partition-specs"]}
        assert 0 in specs and any(field["transform"] == "bucket[4]" for field in specs[1]["fields"])
    elif mode == "replace":
        assert observed["table-uuid"] != original["table-uuid"]
        assert len(snapshots(observed)) == 1
        assert snapshots(observed)[head(observed)]["summary"]["added-records"] == "1"
    write(root / "companion.json", observed)
    print("RWM17A_INSERT_COMPANION_OK")


def verify(uri, table, mode, root, original, original_traffic):
    observed = load_metadata(uri, table)
    final_traffic = traffic(uri)
    write(root / "final.json", {"table": observed, "traffic": final_traffic})
    assert final_traffic["table_commit_requests"] - original_traffic["table_commit_requests"] == 1, (
        "held INSERT must dispatch exactly one commit request"
    )
    conflicts = final_traffic["by_status"].get("409", 0) - original_traffic["by_status"].get("409", 0)
    assert conflicts == int(mode in ("contend", "spec", "replace")), "unexpected Catalog conflicts"
    concurrent = json.loads((root / "companion.json").read_text()) if mode != "unknown" else None
    if mode in ("contend", "spec", "replace"):
        assert observed == concurrent, "rejected INSERT changed the external winner"
    elif mode == "schema":
        assert observed["current-schema-id"] == concurrent["current-schema-id"]
        assert observed["schemas"] == concurrent["schemas"], "append rolled back the added field"
        assert_append(concurrent, observed, 1, nova=True)
    elif mode == "unknown":
        new = assert_append(original, observed, 2, nova=True)
        previous = head(original)
        assert new["summary"]["total-records"] == "5"
        spark(f'''
val qualified = "ice_rest.{table}"
val table = Iru5SparkCommitInterop.table(spark, qualified)
val current = table.currentSnapshot()
val original = table.snapshot({previous}L)
require(current.snapshotId() == {new["snapshot-id"]}L && original != null,
  "Spark read a different append or lost the original head")
for (snapshot <- Seq(original, current)) {{
  require(table.io().newInputFile(snapshot.manifestListLocation()).getLength() > 0L,
    "Published manifest list is missing")
  Iru5SparkCommitInterop.assertManifestLengths(table, snapshot)
  val scan = table.newScan().useSnapshot(snapshot.snapshotId()).planFiles()
  try {{
    scan.asScala.foreach {{ task =>
      val file = task.file()
      require(table.io().newInputFile(file.location().toString).getLength() == file.fileSizeInBytes(),
        "A retained data object does not match its published size")
    }}
  }} finally scan.close()
}}
val historic = spark.sql(s"SELECT id FROM $qualified VERSION AS OF {previous} ORDER BY id")
  .collect().toVector.map(_.getLong(0))
require(historic == Vector(1L, 2L, 3L), s"unexpected historic bag $historic")
val live = spark.sql(s"SELECT id, value FROM $qualified ORDER BY id")
  .collect().toVector.map(row => (row.getLong(0), row.getLong(1)))
require(live == Vector((1L,10L), (2L,20L), (3L,30L), (5L,50L), (6L,60L)),
  s"unexpected live bag $live")
''', "RWM17A_INSERT_UNKNOWN_STORAGE_OK", root / "spark-unknown-readback.log")
    print("RWM17A_INSERT_EFFECTS_OK")


def main():
    action, uri, token, table, mode = sys.argv[1:]
    check_table_name(table)
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", token) or mode not in MODES:
        raise SystemExit("invalid INSERT acceptance run or mode")
    if table != f"ns_{token}.insert_{mode}":
        raise SystemExit("INSERT acceptance action must use its exact run-owned table")
    root = WORKSPACE / "logs/rwm17a/sql-insert-concurrency" / token / mode
    baseline = root / "baseline.json"
    if action == "baseline":
        root.mkdir(parents=True, exist_ok=True)
        write(baseline, {"table": load_metadata(uri, table), "traffic": traffic(uri)})
        print("RWM17A_INSERT_BASE_READY")
        return
    facts = json.loads(baseline.read_text())
    if action == "companion":
        companion(uri, table, mode, root, facts["table"])
    elif action == "verify":
        verify(uri, table, mode, root, facts["table"], facts["traffic"])
    else:
        raise SystemExit(f"unknown INSERT acceptance action {action}")


if __name__ == "__main__":
    main()
