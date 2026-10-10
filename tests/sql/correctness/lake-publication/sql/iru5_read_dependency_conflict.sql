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
-- Read-derived DELETE and OPTIMIZE cannot rebase computed output onto a new
-- main. REST OPTIMIZE is detached; its terminal job is the failure authority.

-- query 1
-- @skip_result_check=true
CREATE DATABASE lake_publication_${suite_uuid0}.ns_${uuid0};
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.delete_rows (
  id BIGINT, value BIGINT
) TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true", "commit.retry.num-retries" = "3", "commit.retry.min-wait-ms" = "0", "commit.retry.max-wait-ms" = "0");
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.optimize_rows (
  id BIGINT, value BIGINT
) TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true", "commit.retry.num-retries" = "3", "commit.retry.min-wait-ms" = "0", "commit.retry.max-wait-ms" = "0");
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.delete_rows VALUES (1, 10), (2, 20);
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.optimize_rows VALUES (1, 10);
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.optimize_rows VALUES (2, 20);

-- query 2
-- @result_contains=IRU5_READ_TRAFFIC_BASELINE_OK
shell: set -eu
artifact_dir="${NOVAROCKS_WORKSPACE_ROOT:-.}/logs/iru-5/native-lake-${uuid0}"
mkdir -p "$artifact_dir"
python3 - '${iceberg_rest_uri}' "$artifact_dir/traffic-before.json" <<'PYTHON'
import json, sys, urllib.request
with urllib.request.urlopen(sys.argv[1].rstrip('/') + '/_fixture/catalog-traffic', timeout=10) as response:
    traffic = json.load(response)
with open(sys.argv[2], 'w') as saved:
    json.dump(traffic, saved)
print('IRU5_READ_TRAFFIC_BASELINE_OK')
PYTHON

-- query 3
-- @publication_catalog_fault=table-commit,before-requirement-check-hold-for-concurrent-shell
-- @publication_catalog_concurrent_shell=set -eu; tmp_sql=$(mktemp "${TMPDIR:-/tmp}/iru5-delete-companion-XXXXXX.sql"); trap 'rm -f "$tmp_sql"' EXIT; printf '%s\n' "INSERT INTO ice_rest.ns_${uuid0}.delete_rows VALUES (3, 30);" > "$tmp_sql"; "${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
-- @expect_error=dependency: RefUnchanged
DELETE FROM lake_publication_${suite_uuid0}.ns_${uuid0}.delete_rows WHERE id = 1;

-- query 4
-- The rejected DELETE neither removes its read row nor loses the Spark row.
SELECT id, value FROM lake_publication_${suite_uuid0}.ns_${uuid0}.delete_rows ORDER BY id;

-- query 5
-- Two seed files ensure this is an actual rewrite, not a no-op.
-- @publication_catalog_fault=table-commit,before-requirement-check-hold-for-concurrent-shell
-- @publication_catalog_concurrent_shell=set -eu; tmp_sql=$(mktemp "${TMPDIR:-/tmp}/iru5-optimize-companion-XXXXXX.sql"); trap 'rm -f "$tmp_sql"' EXIT; printf '%s\n' "INSERT INTO ice_rest.ns_${uuid0}.optimize_rows VALUES (3, 30);" > "$tmp_sql"; "${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
-- @skip_result_check=true
ALTER TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.optimize_rows OPTIMIZE;

-- query 6
-- Poll only the read-only job presentation. Generic wait_alter_optimize
-- expects FINISHED and would wrongly fail this intended typed refusal.
-- @retry_count=60
-- @retry_interval_ms=500
-- @result_contains=KNOWN_UNCOMMITTED
-- @result_contains=dependency: RefUnchanged
-- @skip_result_check=true
SHOW ALTER TABLE OPTIMIZE FROM lake_publication_${suite_uuid0}.ns_${uuid0} WHERE TableName = 'optimize_rows';

-- query 7
SELECT id, value FROM lake_publication_${suite_uuid0}.ns_${uuid0}.optimize_rows ORDER BY id;

-- query 8
-- @result_contains=IRU5_READ_DEPENDENCY_CONFLICT_OK
shell: set -eu
artifact_dir="${NOVAROCKS_WORKSPACE_ROOT:-.}/logs/iru-5/native-lake-${uuid0}"
traffic_file="$artifact_dir/traffic-before.json"
tmp_scala=$(mktemp "${TMPDIR:-/tmp}/iru5-read-conflict-XXXXXX.scala")
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
assert after['by_status'].get('409', 0) - before['by_status'].get('409', 0) == 2, (before, after)
assert after['table_commit_requests'] - before['table_commit_requests'] == 2, (before, after)
print('IRU5_READ_TWO_REAL_409_OK')
PYTHON
cat > "$tmp_scala" <<'SPARK_SCALA'
import scala.jdk.CollectionConverters._
import org.apache.iceberg.spark.Spark3Util
for ((suffix, expectedSnapshots) <- Seq(("delete_rows", 2), ("optimize_rows", 3))) {
  val name = s"ice_rest.ns_${uuid0}.$suffix"
  val table = Spark3Util.loadIcebergTable(spark, name)
  val snapshots = table.snapshots().asScala.toSeq.sortBy(_.sequenceNumber())
  snapshots.foreach(s => println(s"IRU5_READ_SNAPSHOT table=$suffix id=${s.snapshotId()} parent=${s.parentId()} sequence=${s.sequenceNumber()} operation=${s.operation()}"))
  require(snapshots.size == expectedSnapshots, s"stale Nova $suffix result was published")
  require(snapshots.forall(_.operation() == "append"), s"unexpected read-derived mutation reached $suffix")
  val current = table.currentSnapshot()
  require(current.snapshotId() == snapshots.last.snapshotId(), s"main lost Spark's actual $suffix snapshot")
  require(current.parentId() == java.lang.Long.valueOf(snapshots(expectedSnapshots - 2).snapshotId()), s"Spark parent changed in $suffix")
  val rows = spark.sql(s"SELECT id, value, _row_id, _last_updated_sequence_number FROM $name ORDER BY id").collect().toSeq
  require(rows.map(r => (r.getLong(0), r.getLong(1))) == Seq((1L, 10L), (2L, 20L), (3L, 30L)), s"lost or resurrected rows in $suffix")
  require(rows.forall(r => !r.isNullAt(2) && !r.isNullAt(3)), s"lost stored lineage in $suffix")
  require(rows.map(_.getLong(2)).distinct.size == 3, s"duplicate row IDs in $suffix")
  require(rows.last.getLong(3) == current.sequenceNumber(), s"Spark new row has wrong actual sequence in $suffix")
}
println("IRU5_READ_DEPENDENCY_CONFLICT_OK")
SPARK_SCALA
spark_status=0
spark_out=$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1) || spark_status=$?
printf '%s\n' "$spark_out" > "$artifact_dir/spark-readback.log"
printf '%s\n' "$spark_out"
[ "$spark_status" -eq 0 ]
printf '%s\n' "$spark_out" | grep -F IRU5_READ_DEPENDENCY_CONFLICT_OK

-- query 9
-- @skip_result_check=true
DROP DATABASE lake_publication_${suite_uuid0}.ns_${uuid0} FORCE;
