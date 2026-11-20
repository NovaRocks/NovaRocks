#!/usr/bin/env python3
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

"""Observe the held Catalog interleaving and verify Spark readback."""

import json
import re
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parents[4]
SPARK_ORACLE = WORKSPACE / "tests/sql/fixtures/iru5-commit-interop/SparkCommitInterop.scala"
ROLLBACK_CASE = "rwm12_rollback_start"


def traffic(uri):
    with urllib.request.urlopen(uri.rstrip("/") + "/_fixture/catalog-traffic", timeout=10) as response:
        return json.load(response)


def spark(source, marker, log):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".scala") as command:
        command.write(SPARK_ORACLE.read_text())
        command.write(f'\nIru5SparkCommitInterop.checked("{marker}") {{\n{source}\n}}\n')
        command.flush()
        completed = subprocess.run(
            [str(WORKSPACE / "docker/iceberg-rest/spark-shell.sh"), command.name],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    with log.open("x") as saved:
        saved.write(completed.stdout)
    if completed.returncode != 0 or marker not in completed.stdout.splitlines():
        raise RuntimeError(f"Spark oracle {marker} did not pass (see {log})")
    return completed.stdout


def check_table_name(table):
    if not re.fullmatch(r"[a-zA-Z_0-9]+\.[a-zA-Z_0-9]+", table):
        raise ValueError(f"expected an exact owned Spark namespace and table: {table!r}")


def evidence_dir(case, run):
    if not all(re.fullmatch(r"[a-zA-Z_0-9-]+", value) for value in (case, run)):
        raise ValueError("invalid fixture run name")
    return WORKSPACE / "logs/rwm12/sql-interleavings" / run / case


def companion(case, run, table):
    check_table_name(table)
    location = evidence_dir(case, run)
    initialize = f"""
val qualified = "ice_rest.{table}"
val t = Spark3Util.loadIcebergTable(spark, qualified)
t.refresh()
val e = t.currentSnapshot()
require(e != null, "The held mutation must have an original snapshot")
"""
    if case == ROLLBACK_CASE:
        source = initialize + """
val b = e.parentId()
require(b != null && b.longValue() != e.snapshotId(), "The original snapshot needs a distinct parent")
t.manageSnapshots().rollbackTo(b.longValue()).commit()
t.refresh()
require(t.currentSnapshot().snapshotId() == b.longValue(), "The concurrent rollback did not move main to B")
println(s"RWM12_ROLLBACK_ENDPOINTS ${e.snapshotId()} ${b.longValue()}")
"""
    else:
        source = initialize + """
val scan = t.newScan().useSnapshot(e.snapshotId()).planFiles()
val files = try scan.asScala.map(task =>
  (task.file().location().toString, task.file().recordCount(), task.deletes().size())).toVector
  finally scan.close()
val membership = spark.sql(s"SELECT id, _file FROM $qualified ORDER BY id").collect()
  .toVector.map(row => (row.getLong(0), row.getString(1))).toMap
val target = membership(1L)
require(membership(2L) == target && files.forall(_._3 == 0),
  "Rows 1 and 2 must share one live original file with no deletes")
println(s"RWM12_ORIGINAL_FILES ${e.snapshotId()} $files $membership")
"""
        if case == "rwm12_rewrite_removes_target":
            source += f"""
require(membership.keySet == Set(1L, 2L, 3L) && membership(3L) != target &&
  files.size == 2 && files.exists(file => file._1 == target && file._2 == 2L),
  "A10 requires the two-row target and one separate inactive file")
val report = spark.sql("CALL ice_rest.system.rewrite_data_files(table => '{table}', " +
  "options => map('rewrite-all', 'true'))").collect()
require(report.nonEmpty && report.head.getAs[java.lang.Number]("rewritten_data_files_count").longValue() >= 2L,
  "The concurrent procedure did not rewrite both files")
t.refresh()
val b = t.currentSnapshot()
require(b.snapshotId() != e.snapshotId() && b.parentId().longValue() == e.snapshotId() &&
  b.operation() == "replace" &&
  b.removedDataFiles(t.io()).asScala.exists(_.location().toString == target),
  "The concurrent REPLACE did not remove the original target")
println(s"RWM12_REWRITE_REMOVED ${{b.snapshotId()}} $target")
"""
        elif case == "rwm12_format_upgrade":
            source += """
require(Iru5SparkCommitInterop.metadata(t).formatVersion() == 2 &&
  membership.keySet == Set(1L, 2L, 3L) &&
  files == Vector((target, 3L, 0)),
  "A12 requires one v2 data file with three rows")
spark.sql(s"ALTER TABLE $qualified SET TBLPROPERTIES ('format-version'='3')").collect()
t.refresh()
require(Iru5SparkCommitInterop.metadata(t).formatVersion() == 3 &&
  t.currentSnapshot().snapshotId() == e.snapshotId(), "Upgrade must retain E before the append")
spark.sql(s"INSERT INTO $qualified VALUES (4, 40, 'outside')").collect()
t.refresh()
val b = t.currentSnapshot()
val added = b.addedDataFiles(t.io()).asScala.toVector
require(b.parentId().longValue() == e.snapshotId() && b.operation() == "append" &&
  added.nonEmpty && added.forall(_.partition().get(0, classOf[CharSequence]).toString == "outside"),
  "A12 needs a v3 append with a genuinely disjoint partition")
println(s"RWM12_FORMAT_UPGRADE ${e.snapshotId()} ${b.snapshotId()}")
"""
        elif case == "rwm12_dangling_dv":
            source += """
require(membership.keySet == Set(1L, 2L) && files == Vector((target, 2L, 0)),
  "A17 requires exactly one two-row file without a deletion vector")
spark.sql(s"DELETE FROM $qualified WHERE id = 2").collect()
t.refresh()
val b = t.currentSnapshot()
val dvs = Iru5SparkCommitInterop.dvs(t, b)
val remaining = spark.sql(s"SELECT id, _file FROM $qualified").collect().toVector
require(b.parentId().longValue() == e.snapshotId() && b.operation() == "delete" &&
  dvs.size == 1 && dvs.forall(dv => dv.reference == target && dv.offset >= 0L && dv.size > 0L) &&
  remaining.size == 1 && remaining.head.getLong(0) == 1L && remaining.head.getString(1) == target,
  "The concurrent DELETE must leave F live with a new Puffin DV targeting F")
println(s"RWM12_CONCURRENT_DV ${b.snapshotId()} $dvs")
"""
        else:
            raise ValueError(f"no checked Spark companion for {case!r}")

    marker = "RWM12_COMPANION_OK"
    output = spark(source, marker, location / "spark-companion.log")
    if case == ROLLBACK_CASE:
        endpoints = [
            match.groups()
            for line in output.splitlines()
            if (match := re.fullmatch(r"RWM12_ROLLBACK_ENDPOINTS (-?\d+) (-?\d+)", line))
        ]
        if len(endpoints) != 1:
            raise RuntimeError("Spark did not prove one exact rollback endpoint pair")
        start, parent = map(int, endpoints[0])
        with (location / "rollback-endpoints.json").open("x") as saved:
            json.dump({"start_snapshot_id": start, "parent_snapshot_id": parent}, saved)
    print(marker)


def record(uri, case, run):
    location = evidence_dir(case, run)
    location.mkdir(parents=True, exist_ok=True)
    with (location / "traffic-before.json").open("x") as saved:
        json.dump(traffic(uri), saved, sort_keys=True)
    print("RWM12_TRAFFIC_BASELINE_OK")


def verify(uri, case, run, table, commit_requests, columns, expected_text, no_live_entries):
    check_table_name(table)
    location = evidence_dir(case, run)
    before = json.loads((location / "traffic-before.json").read_text())
    after = traffic(uri)
    with (location / "traffic-after.json").open("x") as saved:
        json.dump(after, saved, sort_keys=True)
    actual_delta = {
        "conflicts": after["by_status"].get("409", 0) - before["by_status"].get("409", 0),
        "commit_requests": after["table_commit_requests"] - before["table_commit_requests"],
    }
    print("RWM12_TRAFFIC_DELTA " + json.dumps(actual_delta, sort_keys=True))
    if actual_delta != {"conflicts": 1, "commit_requests": int(commit_requests)}:
        raise RuntimeError(f"unexpected forwarded Catalog traffic: {actual_delta}")

    if any(not re.fullmatch(r"[a-z_]+", column) for column in columns.split(",")):
        raise ValueError(f"invalid column list {columns!r}")
    expected = json.loads(expected_text)
    expected_rows = ", ".join(
        "Seq(" + ", ".join(json.dumps(str(value)) for value in row) + ")" for row in expected
    )
    expected_rows = "Seq(" + expected_rows + ")" if expected_rows else "Seq.empty[Seq[String]]"
    extra = ""
    if no_live_entries == "true":
        extra = f"""
require(spark.sql("SELECT * FROM ice_rest.{table}.files").count() == 0L &&
  spark.sql("SELECT * FROM ice_rest.{table}.delete_files").count() == 0L,
  "The removed data file or concurrent deletion vector remains live")
"""
    marker = "RWM12_SPARK_READBACK_OK"
    spark(f"""
val expected = {expected_rows}
val actual = spark.sql("SELECT {columns} FROM ice_rest.{table} ORDER BY id, value")
  .collect().toSeq.map(row => row.toSeq.map(_.toString))
require(actual == expected, s"unexpected rows: $actual, expected: $expected")
{extra}
""", marker, location / "spark-readback.log")
    print(marker)


def matches_known_uncommitted_message(message, core):
    return re.fullmatch(
        re.escape(core)
        + r" \(lake publication id=[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}"
        r" family=data_mutation disposition=KnownUncommitted\)"
        r" \(external effect disposition=KnownUncommitted\)",
        message,
    ) is not None


def verify_rollback_error(results_jsonl, endpoints_json):
    """Bind A11's exact dynamic E/B error to the preserved runner wire evidence."""
    endpoints = json.loads(Path(endpoints_json).read_text())
    expected_message = (
        f"Failed to commit the transaction during write: Snapshot {endpoints['start_snapshot_id']}"
        f" is not an ancestor of {endpoints['parent_snapshot_id']}"
    )
    with Path(results_jsonl).open() as results:
        records = [json.loads(line) for line in results if line.strip()]
    cases = [
        row for row in records
        if row.get("kind") == "case" and row.get("suite") == "lake-publication"
        and row.get("case") == ROLLBACK_CASE
    ]
    if len(cases) != 1 or cases[0]["status"] != "pass":
        raise RuntimeError("expected one passing lake-publication rollback case")
    checks = [
        step for step in cases[0]["oracle"]["executed"]
        if step["step"] == 3 and step["kind"] == "expected_error" and step["outcome"] == "pass"
    ]
    if len(checks) != 1:
        raise RuntimeError("rollback case lacks one exact successful step-3 error assertion")
    attempt = checks[0]["policy"]["sql_outcome"]["attempts"][-1]
    statement, = attempt["statements"]
    failure, = statement["failures"]
    error = failure["Client"]["Trino"]
    if (
        (error["errorCode"], error["errorName"], error["errorType"])
        != (84148236, "ICEBERG_COMMIT_ERROR", "EXTERNAL")
        or not matches_known_uncommitted_message(error["message"], expected_message)
    ):
        raise RuntimeError(f"A11 wire error differs from the proved rollback: {error['message']}")
    print("RWM12_ROLLBACK_ERROR_OK")


if __name__ == "__main__":
    method, *arguments = sys.argv[1:]
    {
        "baseline": record,
        "companion": companion,
        "finish": verify,
        "verify-rollback": verify_rollback_error,
    }[method](*arguments)
