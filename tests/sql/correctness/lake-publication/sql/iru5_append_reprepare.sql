-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.


-- @sequential=true
-- @order_sensitive=true
-- Real Java CAS rejection of a held append, then canonical reprepare on main.
-- Run only in runner-owned 1FE+3BE verify topology, with -j 1.

-- query 1
-- @skip_result_check=true
CREATE DATABASE lake_publication_${suite_uuid0}.ns_${uuid0};
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.append_rows (
  id BIGINT, writer VARCHAR(16)
) PARTITION BY (id) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true",
  "commit.manifest.target-size-bytes" = "1024",
  "commit.retry.num-retries" = "3",
  "commit.retry.min-wait-ms" = "0",
  "commit.retry.max-wait-ms" = "0",
  "commit.retry.total-timeout-ms" = "60000"
);
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.append_rows VALUES (1, 'seed');

-- query 2
-- @result_contains=IRU5_APPEND_TRAFFIC_BASELINE_OK
shell: set -eu
artifact_dir="${NOVAROCKS_WORKSPACE_ROOT:-.}/logs/iru-5/native-lake-${uuid0}"
mkdir -p "$artifact_dir"
python3 - '${iceberg_rest_uri}' "$artifact_dir/traffic-before.json" <<'PYTHON'
import json, sys, urllib.request
with urllib.request.urlopen(sys.argv[1].rstrip('/') + '/_fixture/catalog-traffic', timeout=10) as response:
    traffic = json.load(response)
with open(sys.argv[2], 'w') as saved:
    json.dump(traffic, saved)
print('IRU5_APPEND_TRAFFIC_BASELINE_OK')
PYTHON

-- query 3
-- Spark uses the physical fixture's downstream REST endpoint. The runner
-- waits for the exact Nova request to enter before running this shell.
-- @publication_catalog_fault=table-commit,before-requirement-check-hold-for-concurrent-shell
-- @publication_catalog_concurrent_shell=set -eu; tmp_sql=$(mktemp "${TMPDIR:-/tmp}/iru5-append-companion-XXXXXX.sql"); trap 'rm -f "$tmp_sql"' EXIT; printf '%s\n' "INSERT INTO ice_rest.ns_${uuid0}.append_rows VALUES (2, 'spark');" > "$tmp_sql"; "${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
-- @skip_result_check=true
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.append_rows VALUES
  (3, 'nova'), (4, 'nova'), (5, 'nova'), (6, 'nova'), (7, 'nova'), (8, 'nova'), (9, 'nova'), (10, 'nova'),
  (11, 'nova'), (12, 'nova'), (13, 'nova'), (14, 'nova'), (15, 'nova'), (16, 'nova'), (17, 'nova'), (18, 'nova'),
  (19, 'nova'), (20, 'nova'), (21, 'nova'), (22, 'nova'), (23, 'nova'), (24, 'nova'), (25, 'nova'), (26, 'nova'),
  (27, 'nova'), (28, 'nova'), (29, 'nova'), (30, 'nova'), (31, 'nova'), (32, 'nova'), (33, 'nova'), (34, 'nova');

-- query 4
SELECT id, writer FROM lake_publication_${suite_uuid0}.ns_${uuid0}.append_rows ORDER BY id;

-- query 5
-- One real 409 and precisely two forwarded table-commit requests distinguish
-- rejected frozen bytes from the successful fresh attempt. Spark readback
-- independently proves the actual parent, sequences and row allocation.
-- @result_contains=IRU5_APPEND_REPREPARE_OK
shell: set -eu
artifact_dir="${NOVAROCKS_WORKSPACE_ROOT:-.}/logs/iru-5/native-lake-${uuid0}"
traffic_file="$artifact_dir/traffic-before.json"
tmp_scala=$(mktemp "${TMPDIR:-/tmp}/iru5-append-readback-XXXXXX.scala")
trap 'rm -f "$tmp_scala"' EXIT
python3 - '${iceberg_rest_uri}' "$traffic_file" "$artifact_dir/traffic-after.json" <<'PYTHON'
import json, sys, urllib.request
with open(sys.argv[2]) as saved:
    before = json.load(saved)
with urllib.request.urlopen(sys.argv[1].rstrip('/') + '/_fixture/catalog-traffic', timeout=10) as response:
    after = json.load(response)
with open(sys.argv[3], 'w') as saved:
    json.dump(after, saved, indent=2, sort_keys=True)
print('IRU5_TRAFFIC_BEFORE ' + json.dumps(before, sort_keys=True))
print('IRU5_TRAFFIC_AFTER ' + json.dumps(after, sort_keys=True))
assert after['by_status'].get('409', 0) - before['by_status'].get('409', 0) == 1, (before, after)
assert after['table_commit_requests'] - before['table_commit_requests'] == 2, (before, after)
print('IRU5_APPEND_REAL_409_OK')
PYTHON
cat > "$tmp_scala" <<'SPARK_SCALA'
import scala.jdk.CollectionConverters._
import org.apache.iceberg.spark.Spark3Util
val name = "ice_rest.ns_${uuid0}.append_rows"
val table = Spark3Util.loadIcebergTable(spark, name)
val snapshots = table.snapshots().asScala.toSeq.sortBy(_.sequenceNumber())
snapshots.foreach(s => println(s"IRU5_APPEND_SNAPSHOT id=${s.snapshotId()} parent=${s.parentId()} sequence=${s.sequenceNumber()} operation=${s.operation()}"))
require(snapshots.size == 3, "a rejected Nova attempt committed a stale snapshot")
require(snapshots.forall(_.operation() == "append"), "unexpected mutation in append race")
val current = table.currentSnapshot()
val parent = table.snapshot(current.parentId())
require(parent.snapshotId() == snapshots(1).snapshotId(), "successful Nova snapshot has the wrong parent")
require(current.sequenceNumber() == parent.sequenceNumber() + 1, "fresh append sequence is not actual Java successor")
val rows = spark.sql(s"SELECT id, writer, _row_id, _last_updated_sequence_number FROM $name ORDER BY id").collect().toSeq
val expected = Seq((1L, "seed"), (2L, "spark")) ++ (3L to 34L).map(id => (id, "nova"))
require(rows.map(r => (r.getLong(0), r.getString(1))) == expected, "lost or repeated append rows")
require(rows.size == 34 && rows.map(_.getLong(0)).sum == 595L, "the published rows do not satisfy the independent aggregate")
require(rows.forall(r => !r.isNullAt(2) && !r.isNullAt(3)), "unassigned row lineage")
require(rows.map(_.getLong(2)).distinct.size == rows.size, "conflicting attempts duplicated row IDs")
require(rows(1).getLong(3) == parent.sequenceNumber(), "Spark row does not belong to actual parent")
require(rows.drop(2).forall(_.getLong(3) == current.sequenceNumber()), "Nova rows inherited a predicted or stale sequence")
val entries = spark.sql(s"SELECT sequence_number, file_sequence_number FROM $name.entries WHERE status = 1 AND snapshot_id = ${current.snapshotId()}").collect().toSeq
entries.foreach(r => println(s"IRU5_APPEND_ENTRY data_sequence=${r.getLong(0)} file_sequence=${r.getLong(1)}"))
require(entries.size == 32, "the successful Nova attempt must publish one file for each of its partitions")
require(entries.forall(r => r.getLong(0) == current.sequenceNumber() && r.getLong(1) == current.sequenceNumber()), "added manifest entry sequence inheritance differs from actual publication")
val newManifests = spark.sql(s"SELECT count(*) FROM $name.manifests WHERE added_snapshot_id = ${current.snapshotId()} AND content = 0").head().getLong(0)
require(newManifests >= 2, "the successful attempt did not publish rolled data manifests")
println("IRU5_APPEND_REPREPARE_OK")
SPARK_SCALA
spark_status=0
spark_out=$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1) || spark_status=$?
printf '%s\n' "$spark_out" > "$artifact_dir/spark-readback.log"
printf '%s\n' "$spark_out"
[ "$spark_status" -eq 0 ]
printf '%s\n' "$spark_out" | grep -F IRU5_APPEND_REPREPARE_OK

-- query 6
-- @skip_result_check=true
DROP DATABASE lake_publication_${suite_uuid0}.ns_${uuid0} FORCE;
