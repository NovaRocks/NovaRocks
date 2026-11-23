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
import os
import re
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parents[4]
SPARK_ORACLE = WORKSPACE / "tests/sql/fixtures/iru5-commit-interop/SparkCommitInterop.scala"
ROLLBACK_CASE = "rwm12_rollback_start"
METADATA_DELETE_COMPANIONS = {
    "rwm18_metadata_delete_append": ("append", "append", 2),
    "rwm18_metadata_delete_concurrent_delete": ("delete_partition", "delete", 2),
    "rwm18_metadata_delete_evolution_conflict": ("evolve_and_append_mixed", "append", 1),
}


def traffic(uri):
    with urllib.request.urlopen(uri.rstrip("/") + "/_fixture/catalog-traffic", timeout=10) as response:
        return json.load(response)


def spark(source, marker, log, *, script_dir=None):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".scala", dir=script_dir) as command:
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
    owner = "rwm18" if case in METADATA_DELETE_COMPANIONS else "rwm12"
    return WORKSPACE / "logs" / owner / "sql-interleavings" / run / case


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


TRUNCATE_COMPANIONS = {
    "rwm17b_truncate_append": ("append", "INSERT INTO {qualified} VALUES (4, 40)"),
    "rwm17b_truncate_concurrent_delete": ("delete", "DELETE FROM {qualified} WHERE TRUE"),
}


def main_snapshot(uri, table):
    namespace, name = table.split(".")
    path = f"/v1/namespaces/{namespace}/tables/{name}"
    with urllib.request.urlopen(uri.rstrip("/") + path, timeout=10) as response:
        metadata = json.load(response)["metadata"]
    ref = metadata["refs"]["main"]["snapshot-id"]
    snapshot, = (item for item in metadata["snapshots"] if item["snapshot-id"] == ref)
    return snapshot


def spark_dml(sql, log):
    env_file = str(Path(os.environ["NOVA_ENV_REST_ENV_FILE"]).resolve(strict=True))
    original = subprocess.check_output(
        ["bash", "-c", 'set -e; source "$1"; env -0', "spark-fixture", env_file]
    )
    env = dict(os.fsdecode(item).split("=", 1) for item in original.split(b"\0") if item)
    helper = [sys.executable, str(WORKSPACE / "docker/iceberg-rest/fixture_run.py")]
    fixture_args = ["--env-file", env_file]
    for variable, operation in (("NOVA_FIXTURE_RUN_CONTEXT", "context"),
                                ("NOVA_FIXTURE_RUN_STORAGE", "storage")):
        env[variable] = subprocess.check_output(
            [*helper, operation, *fixture_args], env=env, text=True
        ).strip()
    compose = [
        *helper, "compose", *fixture_args, "--",
        "--env-file", env["NOVA_ENV_COMPOSE_ENV"],
        "-p", env["NOVA_ENV_COMPOSE_PROJECT"],
        "-f", env["NOVA_ENV_COMPOSE_FILE"],
        "exec", "-T", "spark",
    ]
    # Use the original pinned REST catalog configuration without starting the
    # Spark SQL CLI's Hive metastore. The DML remains Spark SQL.
    defaults = Path(env["NOVAROCKS_SPARK_DEFAULTS"]).read_text()
    if env["NOVA_FIXTURE_RUN_CONTEXT"] != "null":
        identity = json.loads(env["NOVA_FIXTURE_RUN_CONTEXT"])["identity"]
        storage = json.dumps(json.loads(env["NOVA_FIXTURE_RUN_STORAGE"]), separators=(",", ":"))
        defaults += (f"\nspark.novarocks.fixture.scopeRoot {identity['scope_root']}"
                     f"\nspark.novarocks.fixture.runId {identity['run_id']}"
                     f"\nspark.novarocks.fixture.storage {storage}\n")
    for extra in filter(None, env.get("NOVAROCKS_SPARK_EXTRA_DEFAULTS", "").split(":")):
        defaults += "\n" + Path(extra).read_text()

    program = """from pyspark.sql import SparkSession
import sys
spark = SparkSession.builder.config("spark.sql.catalogImplementation", "in-memory").getOrCreate()
try:
    spark.sql(sys.argv[1]).collect()
finally:
    spark.stop()
"""
    with log.open("x") as saved:
        subprocess.run([*helper, "sweep-start", *fixture_args], env=env, check=True,
                       stdout=saved, stderr=subprocess.STDOUT)
        folder = subprocess.check_output(
            [*helper, "temp-dir", *fixture_args], env=env, text=True
        ).strip()
        try:
            # Send the credential-bearing defaults through the owned stdin.
            # The same observed Docker exec runs spark-submit after staging.
            bootstrap = """
import json
import os
from pathlib import Path
import sys
os.umask(0o077)
folder = Path(sys.argv[1])
defaults, program = json.load(sys.stdin)
(folder / "spark-defaults.conf").write_text(defaults)
(folder / "companion.py").write_text(program)
submit = "/opt/spark/bin/spark-submit"
os.execv(submit, [submit, "--properties-file", str(folder / "spark-defaults.conf"),
                   str(folder / "companion.py"), sys.argv[2]])
"""
            subprocess.run(
                [*compose, "python3", "-c", bootstrap, folder, sql],
                env=env, input=json.dumps([defaults, program]), text=True, check=True,
                stdout=saved, stderr=subprocess.STDOUT,
            )
        finally:
            subprocess.run([*helper, "cleanup-dir", *fixture_args, "--path", folder],
                           env=env, check=True, stdout=saved, stderr=subprocess.STDOUT)


def truncate_companion(case, table, action, uri, location):
    operation, write = TRUNCATE_COMPANIONS[case]
    if action != ("append" if operation == "append" else "truncate") or uri is None:
        raise ValueError(f"unexpected TRUNCATE companion {action!r} for {case!r}")
    start = main_snapshot(uri, table)["snapshot-id"]
    # The held phase runs one original Spark mutation. Exact metadata requests
    # bracket its actual exit, and the independent Spark reader checks both snapshots.
    log = location / "spark-companion.log"
    spark_dml(write.format(qualified=f"ice_rest.{table}"), log)

    snapshot = main_snapshot(uri, table)
    committed = snapshot["snapshot-id"]
    if (committed == start or snapshot.get("parent-snapshot-id") != start
            or snapshot["summary"]["operation"] != operation):
        raise RuntimeError(f"Spark did not commit {operation} on its original main head (see {log})")
    with (location / "truncate-companion.json").open("x") as saved:
        json.dump({"start": start, "committed": committed}, saved, sort_keys=True)
    print("RWM17B_COMPANION_OK")


def metadata_delete_scope(run, table):
    check_table_name(table)
    # The SQL runner rewrites the table namespace and the case's evidence
    # name separately. Validate both against its inherited ownership.
    sys.path.insert(0, str(WORKSPACE / "docker/iceberg-rest"))
    try:
        from scratch import RUN_STORAGE_ENV, RunStorageScope
    finally:
        sys.path.pop(0)
    scope = RunStorageScope.from_dict(json.loads(os.environ[RUN_STORAGE_ENV]))
    context = json.loads(os.environ["NOVA_FIXTURE_RUN_CONTEXT"])
    if context["identity"] != scope.identity.as_dict():
        raise ValueError("The original fixture run and its storage binding disagree")
    namespace, name = table.split(".")
    if name != "rows_under_test" or not scope.owns_namespace(namespace) or not scope.owns_namespace(run):
        raise ValueError("RWM-18 requires the original case-owned namespace and table")


def metadata_delete_spark_source(table):
    return f"""
val qualified = "ice_rest.{table}"
val t = Iru5SparkCommitInterop.table(spark, qualified)
def members(at: Snapshot): Map[Long, String] = {{
  val rows = spark.sql(s"SELECT id, _file FROM $qualified VERSION AS OF ${{at.snapshotId()}}")
    .collect().map(row => row.getLong(0) -> row.getString(1))
  val keyed = rows.toMap
  require(keyed.size == rows.length, s"Duplicate fixture ids in snapshot ${{at.snapshotId()}}")
  keyed
}}
def liveDataPaths(at: Snapshot): Set[String] = {{
  val scan = t.newScan().useSnapshot(at.snapshotId()).planFiles()
  try scan.asScala.map(_.file().location()).toSet finally scan.close()
}}
def liveDeleteCount: Long =
  spark.sql(s"SELECT count(*) FROM $qualified.delete_files").head().getLong(0)
"""


def metadata_delete_companion(case, run, table, action, uri, location):
    metadata_delete_scope(run, table)
    expected_action, operation, _ = METADATA_DELETE_COMPANIONS[case]
    if action != expected_action or uri is None:
        raise ValueError(f"unexpected RWM-18 companion action {action!r} for {case!r}")
    e_id = int(main_snapshot(uri, table)["snapshot-id"])
    source = metadata_delete_spark_source(table) + f"""
val e = t.currentSnapshot()
require(e != null && e.snapshotId() == {e_id}L && e.operation() == "append",
  "The held DELETE requires the original seed append snapshot")
val oldSpec = t.spec()
val seeded = members(e)
require(seeded.values.toSet == liveDataPaths(e) && liveDeleteCount == 0L,
  "The original scan must expose exactly the seed data files with no deletes")
require(oldSpec.fields().size() == 1, "Expected a single original partition field")
val partition = oldSpec.fields().get(0)
"""
    if action == "evolve_and_append_mixed":
        mutate = """
require(seeded.keySet == Set(1L, 2L) && seeded(1L) != seeded(2L) &&
  partition.transform().toString == "day" && partition.sourceId() == t.schema().findField("ts").fieldId(),
  "The original two rows must be in distinct day partitions")
val oldSnapshots = t.snapshots().asScala.map(_.snapshotId()).toSet
val bucket = org.apache.iceberg.expressions.Expressions.bucket[java.lang.Integer]("id", 1)
t.updateSpec().removeField(partition.name()).addField(bucket).commit()
t.refresh()
val newSpec = t.spec()
require(t.currentSnapshot().snapshotId() == e.snapshotId() &&
  t.snapshots().asScala.map(_.snapshotId()).toSet == oldSnapshots,
  "Partition evolution must publish metadata while leaving the original snapshots intact")
require(newSpec.specId() != oldSpec.specId() && t.specs().containsKey(oldSpec.specId()) &&
  newSpec.fields().size() == 1 && newSpec.fields().get(0).transform().toString == "bucket[1]" &&
  newSpec.fields().get(0).sourceId() == t.schema().findField("id").fieldId(),
  "The new write spec must bucket id and retain the historical day spec")
spark.sql(s"INSERT INTO $qualified VALUES " +
  "(3, TIMESTAMP_NTZ '2024-01-01 12:00:00'), (4, TIMESTAMP_NTZ '2024-01-02 12:00:00')").collect()
"""
        verify = """
require(added.size == 1, "A single new-spec file must contain the concurrent append")
val mixed = added.head
require(mixed.specId() == newSpec.specId() && mixed.recordCount() == 2L &&
  after.keySet == Set(1L, 2L, 3L, 4L) &&
  seeded.forall { case (id, path) => after(id) == path } &&
  after(3L) == mixed.location() && after(4L) == mixed.location() &&
  !seeded.values.toSet.contains(mixed.location()) && removed.isEmpty,
  "The concurrent append must put both new rows in one new-spec file")
val matching = spark.sql(s"SELECT id FROM $qualified WHERE " +
  "ts >= TIMESTAMP_NTZ '2024-01-01 00:00:00' AND ts < TIMESTAMP_NTZ '2024-01-02 00:00:00' ORDER BY id")
  .collect().map(_.getLong(0)).toVector
require(matching == Vector(1L, 3L),
  "Exactly one row of the concurrent file must match the held DELETE")
println("RWM18_SPARK_MIXED_FILE spec=" + newSpec.specId() + " path=" + mixed.location())
"""
    else:
        source += """
require(seeded.keySet == Set(1L, 2L, 3L) && seeded(1L) != seeded(3L) &&
  seeded(2L) != seeded(3L) && partition.transform().isIdentity() &&
  partition.sourceId() == t.schema().findField("part").fieldId(),
  "The seed requires identity partition data on both sides of the held predicate")
"""
        if action == "append":
            mutate = 'spark.sql(s"INSERT INTO $qualified VALUES (4, 10, 400)").collect()\n'
            verify = """
require(added.size == 1, "A single data file must hold the matching append")
val appended = added.head
require(appended.specId() == oldSpec.specId() &&
  appended.partition().get(0, classOf[java.lang.Integer]).intValue() == 10 &&
  appended.recordCount() == 1L && after.keySet == Set(1L, 2L, 3L, 4L) &&
  seeded.forall { case (id, path) => after(id) == path } &&
  after(4L) == appended.location() && !seeded.values.toSet.contains(appended.location()) &&
  removed.isEmpty,
  "Spark must append a separate matching data file and leave the seed files live")
println("RWM18_SPARK_APPENDED_FILE " + appended.location())
"""
        else:
            mutate = 'spark.sql(s"DELETE FROM $qualified WHERE part = 10").collect()\n'
            verify = """
require(removed == Set(seeded(1L), seeded(2L)) && added.isEmpty &&
  b.summary().get("deleted-records") == "2" && after == Map(3L -> seeded(3L)),
  "Spark must delete both matching rows by removing their data files")
println("RWM18_SPARK_DELETED_FILES " + removed.toVector.sorted.mkString(","))
"""
    source += mutate + f"""
t.refresh()
val b = t.currentSnapshot()
require(b != null && b.snapshotId() != e.snapshotId() && b.parentId() != null &&
  b.parentId().longValue() == e.snapshotId() && b.sequenceNumber() > e.sequenceNumber() &&
  b.operation() == "{operation}", "The Spark snapshot must be the exact expected child of E")
val after = members(b)
val added = b.addedDataFiles(t.io()).asScala.toVector
val removed = b.removedDataFiles(t.io()).asScala.map(_.location()).toSet
require(b.addedDeleteFiles(t.io()).asScala.isEmpty && liveDeleteCount == 0L,
  "The companion must not leave row-level delete files")
""" + verify + """
println("RWM18_SPARK_COMPANION_ENDPOINTS " + e.snapshotId() + " " + b.snapshotId() + " " + t.spec().specId())
"""
    marker = "RWM18_SPARK_COMPANION_OK"
    log = location / "spark-companion.log"
    output = spark(source, marker, log, script_dir=location)
    endpoints = [
        match.groups() for line in output.splitlines()
        if (match := re.fullmatch(r"RWM18_SPARK_COMPANION_ENDPOINTS (-?\d+) (-?\d+) (\d+)", line))
    ]
    if len(endpoints) != 1:
        raise RuntimeError(f"expected one exact E/B/spec observation (see {log})")
    start, committed, spec = map(int, endpoints[0])
    head = main_snapshot(uri, table)
    if (start != e_id or committed == start or head["snapshot-id"] != committed
            or head.get("parent-snapshot-id") != start or head["summary"]["operation"] != operation):
        raise RuntimeError(f"Spark evidence and the held Catalog target disagree (see {log})")
    with (location / "metadata-delete-companion.json").open("x") as saved:
        json.dump({"table": table, "start": start, "committed": committed, "spec": spec},
                  saved, sort_keys=True)
    print(marker)


def verify_maintenance_traffic(uri, location, commit_requests, conflicts):
    before = json.loads((location / "traffic-before.json").read_text())
    after = traffic(uri)
    with (location / "traffic-after.json").open("x") as saved:
        json.dump(after, saved, sort_keys=True)
    actual = {
        "conflicts": after["by_status"].get("409", 0) - before["by_status"].get("409", 0),
        "commit_requests": after["table_commit_requests"] - before["table_commit_requests"],
    }
    expected = {"conflicts": conflicts, "commit_requests": commit_requests}
    if actual != expected:
        raise RuntimeError(f"Catalog traffic {actual} differs from {expected}")
    return actual


def metadata_delete_finish(uri, case, run, table):
    metadata_delete_scope(run, table)
    action, operation, requests = METADATA_DELETE_COMPANIONS[case]
    location = evidence_dir(case, run)
    binding = json.loads((location / "metadata-delete-companion.json").read_text())
    if binding["table"] != table:
        raise RuntimeError("The companion evidence belongs to a different Catalog target")
    actual = verify_maintenance_traffic(uri, location, requests, 1)
    print("RWM18_TRAFFIC_DELTA " + json.dumps(actual, sort_keys=True))
    start, committed, spec = (int(binding[key]) for key in ("start", "committed", "spec"))
    source = metadata_delete_spark_source(table) + f"""
val e = t.snapshot({start}L)
val b = t.snapshot({committed}L)
val head = t.currentSnapshot()
require(e != null && b != null && b.parentId() != null &&
  b.parentId().longValue() == e.snapshotId() && b.operation() == "{operation}" &&
  t.spec().specId() == {spec}, "The exact companion snapshot or its partition spec was lost")
require(liveDeleteCount == 0L, "The current table may not retain a delete file")
val original = members(e)
val after = members(head)
"""
    if action == "evolve_and_append_mixed":
        source += """
val added = b.addedDataFiles(t.io()).asScala.toVector
require(head.snapshotId() == b.snapshotId() && after.keySet == Set(1L, 2L, 3L, 4L) &&
  original.keySet == Set(1L, 2L) && original.forall { case (id, path) => after(id) == path } &&
  added.size == 1 && added.head.specId() == t.spec().specId() &&
  after(3L) == added.head.location() && after(4L) == added.head.location() &&
  liveDataPaths(head) == original.values.toSet + added.head.location(),
  "The rejected DELETE must leave the exact mixed-file Spark snapshot live")
val rows = spark.sql(s"SELECT id, date_format(ts, 'yyyy-MM-dd HH:mm:ss') FROM $qualified ORDER BY id")
  .collect().map(row => (row.getLong(0), row.getString(1))).toVector
require(rows == Vector((1L, "2024-01-01 04:00:00"), (2L, "2024-01-02 01:00:00"),
  (3L, "2024-01-01 12:00:00"), (4L, "2024-01-02 12:00:00")),
  s"The failed statement changed the Spark companion rows: $rows")
"""
    else:
        source += """
require(head.operation() == "delete" && head.parentId() != null &&
  head.parentId().longValue() == b.snapshotId() && head.snapshotId() != b.snapshotId() &&
  head.sequenceNumber() > b.sequenceNumber(),
  "NovaRocks must publish its own delete snapshot after the exact Spark companion")
val removed = head.removedDataFiles(t.io()).asScala.map(_.location()).toSet
require(head.addedDataFiles(t.io()).asScala.isEmpty && head.addedDeleteFiles(t.io()).asScala.isEmpty &&
  after == Map(3L -> original(3L)) && liveDataPaths(head) == Set(original(3L)),
  "NovaRocks must preserve the original file outside the selected partition")
val rows = spark.sql(s"SELECT id, part, value FROM $qualified ORDER BY id").collect()
  .map(row => (row.getLong(0), row.getInt(1), row.getLong(2))).toVector
require(rows == Vector((3L, 20, 300L)), s"Unexpected final Spark readback: $rows")
"""
        if action == "append":
            source += """
val appended = b.addedDataFiles(t.io()).asScala.map(_.location()).toSet
val selected = Set(original(1L), original(2L))
require(appended.nonEmpty && appended.intersect(selected).isEmpty &&
  removed == selected.union(appended) && head.summary().get("deleted-records") == "3",
  "The winning DELETE must remove both the original and the concurrent matching files")
"""
        else:
            source += """
require(b.removedDataFiles(t.io()).asScala.map(_.location()).toSet == Set(original(1L), original(2L)) &&
  removed.isEmpty && !head.summary().containsKey("deleted-records"),
  "The second successful DELETE must publish an empty snapshot")
"""
    source += 'println("RWM18_SPARK_CURRENT_HEAD " + head.snapshotId())\n'
    marker = "RWM18_SPARK_READBACK_OK"
    spark(source, marker, location / "spark-readback.log", script_dir=location)
    print(marker)


def maintenance_companion(case, run, table, action, uri=None):
    check_table_name(table)
    location = evidence_dir(case, run)
    if case in TRUNCATE_COMPANIONS:
        return truncate_companion(case, table, action, uri, location)
    if case in METADATA_DELETE_COMPANIONS:
        return metadata_delete_companion(case, run, table, action, uri, location)
    source = f"""
val qualified = "ice_rest.{table}"
val t = Iru5SparkCommitInterop.table(spark, qualified)
val e = t.currentSnapshot()
val scan = t.newScan().useSnapshot(e.snapshotId()).planFiles()
val files = try scan.asScala.map(_.file().location().toString).toSet finally scan.close()
require(files.size == 2, "Maintenance fixture requires two distinct seed files")
val membership = spark.sql(s"SELECT id, _file FROM $qualified ORDER BY id").collect()
  .map(row => row.getLong(0) -> row.getString(1)).toMap
require(membership.keySet == Set(1L, 2L, 3L) && membership(1L) == membership(2L) &&
  membership(1L) != membership(3L), "Unexpected placement of the original rows")
"""
    if action in ("append", "expire"):
        values = "(4, 40, 'outside')" if case.endswith("optimize_disjoint_append") else "(4, 40)"
        source += f"""
spark.sql(s"INSERT INTO $qualified VALUES {values}").collect()
t.refresh()
val b = t.currentSnapshot()
require(b.operation() == "append" && b.parentId().longValue() == e.snapshotId() &&
  b.addedDataFiles(t.io()).asScala.nonEmpty,
  "The external writer must add a new live data file after E")
println(s"RWM17B_EXTERNAL_APPEND ${{b.snapshotId()}}")
"""
        if action == "expire":
            source += """
t.expireSnapshots().expireSnapshotId(e.snapshotId()).cleanExpiredFiles(false).commit()
t.refresh()
require(t.currentSnapshot().snapshotId() == b.snapshotId() &&
  t.currentSnapshot().parentId().longValue() == e.snapshotId() &&
  t.snapshot(e.snapshotId()) == null,
  "E must be expired while B retains its exact parent pointer")
"""
    elif action == "position_delete":
        source += """
spark.sql(s"DELETE FROM $qualified WHERE id = 2").collect()
t.refresh()
val b = t.currentSnapshot()
val dvs = Iru5SparkCommitInterop.dvs(t, b)
require(b.operation() == "delete" && b.parentId().longValue() == e.snapshotId() &&
  dvs.size == 1 && dvs.forall(_.reference == membership(1L)),
  "The competing position delete must introduce a DV against a selected file")
"""
    elif action == "rewrite":
        source += f"""
spark.sql("CALL ice_rest.system.rewrite_data_files(table => '{table}', " +
  "options => map('rewrite-all', 'true'))").collect()
t.refresh()
val b = t.currentSnapshot()
require(b.operation() == "replace" && b.parentId().longValue() == e.snapshotId() &&
  b.removedDataFiles(t.io()).asScala.map(_.location().toString).toSet == files,
  "The competing REPLACE must remove both selected files")
"""
    elif action == "add_column":
        source += """
spark.sql(s"ALTER TABLE $qualified ADD COLUMN new_value BIGINT").collect()
t.refresh()
require(t.schema().findField("new_value") != null &&
  t.currentSnapshot().snapshotId() == e.snapshotId(),
  "The competing schema change must preserve the snapshot head")
"""
    else:
        raise ValueError(f"unsupported maintenance companion: {action!r}")
    spark(source, "RWM17B_COMPANION_OK", location / "spark-companion.log")


def maintenance_finish(uri, case, run, table, commit_requests, conflicts, columns,
                       expected_text, operation):
    check_table_name(table)
    location = evidence_dir(case, run)
    actual = verify_maintenance_traffic(uri, location, int(commit_requests), int(conflicts))
    print("RWM17B_TRAFFIC_DELTA " + json.dumps(actual, sort_keys=True))

    if any(not re.fullmatch(r"[a-z_]+", column) for column in columns.split(",")):
        raise ValueError(f"invalid column list {columns!r}")
    if operation not in ("replace", "delete", "append"):
        raise ValueError(f"unexpected snapshot operation {operation!r}")
    expected = json.loads(expected_text)
    rows = ", ".join(
        "Seq(" + ", ".join(json.dumps(str(value)) for value in row) + ")" for row in expected
    )
    expected_rows = "Seq(" + rows + ")" if rows else "Seq.empty[Seq[String]]"
    extra = ""
    if case.endswith(("optimize_disjoint_append", "optimize_expired_start")):
        lines = (location / "spark-companion.log").read_text().splitlines()
        ids = [match[1] for line in lines
               if (match := re.fullmatch(r"RWM17B_EXTERNAL_APPEND ([0-9]+)", line))]
        if len(ids) != 1:
            raise RuntimeError("Spark did not witness exactly one external append")
        extra = f"""
val b = t.snapshot({ids[0]}L)
val retained = b.addedDataFiles(t.io()).asScala.map(_.location().toString).toSet
val scan = t.newScan().planFiles()
val current = try scan.asScala.map(_.file().location().toString).toSet finally scan.close()
require(retained.nonEmpty && retained.subsetOf(current) &&
  t.currentSnapshot().parentId().longValue() == b.snapshotId(),
  "The winning replacement must keep the actual concurrent files")
"""
    if case == "rwm17b_optimize_disjoint_append":
        # B was bound to the original E by the companion before releasing the hold.
        extra += """
val e = t.snapshot(b.parentId().longValue())
val replacement = t.currentSnapshot()
require(e != null && e.sequenceNumber() < b.sequenceNumber() &&
  b.sequenceNumber() < replacement.sequenceNumber(),
  "B7 must distinguish the original, concurrent append and replacement sequences")
case class SeedRow(id: Long, rowId: Long, lastUpdated: Long, path: String) {
  def lineage: (Long, Long, Long) = (id, rowId, lastUpdated)
}
def seedRowsAt(snapshotId: Long): Vector[SeedRow] =
  spark.read.format("iceberg").option("snapshot-id", snapshotId.toString).load(qualified)
    .where("p = 'seed'").selectExpr("id", "_row_id", "_last_updated_sequence_number", "_file")
    .orderBy("id").collect().toVector.map { row =>
      require(!row.isNullAt(1) && !row.isNullAt(2), "B7 requires complete row lineage")
      SeedRow(row.getLong(0), row.getLong(1), row.getLong(2), row.getString(3))
    }
val oldRows = seedRowsAt(e.snapshotId())
val replacedRows = seedRowsAt(replacement.snapshotId())
require(oldRows.map(_.id) == Vector(1L, 2L, 3L) &&
  oldRows.map(_.rowId).distinct.size == oldRows.size &&
  oldRows.map(_.lastUpdated).distinct.size == 2 &&
  oldRows.map(_.lineage) == replacedRows.map(_.lineage),
  s"B7 changed the selected rows' lineage: before=$oldRows after=$replacedRows")
val originalFiles = oldRows.map(_.path).toSet
val removedFiles = replacement.removedDataFiles(t.io()).asScala.map(_.location().toString).toSet
val replacementFiles = replacement.addedDataFiles(t.io()).asScala.map(_.location().toString).toSet
require(originalFiles.size == 2 && removedFiles == originalFiles && replacementFiles.size == 1 &&
  replacedRows.map(_.path).toSet == replacementFiles &&
  replacementFiles.intersect(originalFiles ++ retained).isEmpty,
  "B7 must observe selected rows on the actual newly added replacement file")
val authoredManifests = replacement.dataManifests(t.io()).asScala
  .filter(_.snapshotId() == Long.box(replacement.snapshotId()))
require(authoredManifests.nonEmpty &&
  authoredManifests.forall(_.sequenceNumber() == replacement.sequenceNumber()),
  "Replacement manifests must inherit the replacement snapshot sequence")
val addedEntries = spark.sql(s"SELECT data_file.file_path, sequence_number, file_sequence_number " +
  s"FROM $qualified.entries WHERE status=1 AND snapshot_id=${replacement.snapshotId()} " +
  "AND data_file.content=0").collect().toVector
require(addedEntries.size == replacementFiles.size &&
  addedEntries.map(_.getString(0)).toSet == replacementFiles &&
  addedEntries.forall(row => !row.isNullAt(1) && !row.isNullAt(2) &&
    row.getLong(1) == e.sequenceNumber() && row.getLong(2) == replacement.sequenceNumber()),
  "Replacement files must carry E's data sequence and the replacement's file sequence")
println(s"RWM17B_SPARK_B7_LINEAGE_OK E=${e.snapshotId()}:${e.sequenceNumber()} " +
  s"B=${b.snapshotId()}:${b.sequenceNumber()} T=${replacement.snapshotId()}:${replacement.sequenceNumber()} " +
  s"rows=${replacedRows.map(_.lineage)}")
"""
    if case.endswith("optimize_add_column"):
        extra += """
require(spark.sql(s"SELECT new_value FROM $qualified WHERE new_value IS NOT NULL").count() == 0,
  "Rows written with the original schema must read NULL for its new field")
"""
    if case in TRUNCATE_COMPANIONS:
        binding = json.loads((location / "truncate-companion.json").read_text())
        start, committed = binding["start"], binding["committed"]
        companion_operation, _ = TRUNCATE_COMPANIONS[case]
        extra += f"""
val e = t.snapshot({start}L)
val b = t.snapshot({committed}L)
require(e != null && b != null && b.operation() == "{companion_operation}" &&
  b.parentId().longValue() == e.snapshotId() && t.currentSnapshot().parentId().longValue() == b.snapshotId() &&
  t.currentSnapshot().snapshotId() != b.snapshotId(),
  "The winning TRUNCATE must follow the exact Spark companion commit")
val seedScan = t.newScan().useSnapshot(e.snapshotId()).planFiles()
val seedFiles = try seedScan.asScala.map(_.file().location().toString).toSet finally seedScan.close()
require(seedFiles.size == 2, "Maintenance fixture requires two distinct seed files")
val membership = spark.sql(s"SELECT id, _file FROM $qualified VERSION AS OF {start} ORDER BY id").collect()
  .map(row => row.getLong(0) -> row.getString(1)).toMap
require(membership.keySet == Set(1L, 2L, 3L) && membership(1L) == membership(2L) &&
  membership(1L) != membership(3L), "Unexpected placement of the original rows")
"""
        if companion_operation == "append":
            extra += """
require(b.addedDataFiles(t.io()).asScala.nonEmpty,
  "The external writer must add a new live data file after E")
"""
        else:
            extra += f"""
require(spark.sql(s"SELECT * FROM $qualified VERSION AS OF {committed}").count() == 0L,
  "The competing Spark delete must commit and empty the table")
"""
    if not expected:
        extra += """
require(spark.sql(s"SELECT * FROM $qualified.files").count() == 0L &&
  spark.sql(s"SELECT * FROM $qualified.delete_files").count() == 0L,
  "TRUNCATE must retire every live data and delete entry")
"""
    marker = "RWM17B_SPARK_READBACK_OK"
    spark(f"""
val qualified = "ice_rest.{table}"
val t = Iru5SparkCommitInterop.table(spark, qualified)
require(t.currentSnapshot().operation() == "{operation}", "Unexpected final snapshot kind")
val expected = {expected_rows}
val actual = spark.sql(s"SELECT {columns} FROM $qualified ORDER BY id").collect()
  .toSeq.map(row => row.toSeq.map(_.toString))
require(actual == expected, s"Unexpected rows: $actual, expected: $expected")
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
        "maintenance-companion": maintenance_companion,
        "maintenance-finish": maintenance_finish,
        "metadata-delete-finish": metadata_delete_finish,
    }[method](*arguments)
