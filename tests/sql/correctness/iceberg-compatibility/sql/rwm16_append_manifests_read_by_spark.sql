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

-- @order_sensitive=true
-- @sequential=true

-- query 1
-- @skip_result_check=true
CREATE DATABASE IF NOT EXISTS iceberg_compat_${suite_uuid0}.${case_db};
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm16_append_v1_${uuid0} (id BIGINT, part BIGINT)
PARTITION BY (part)
TBLPROPERTIES ("format-version" = "1", "commit.manifest.target-size-bytes" = "1024");
INSERT INTO iceberg_compat_${suite_uuid0}.${case_db}.rwm16_append_v1_${uuid0} VALUES
  (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8),
  (9, 9), (10, 10), (11, 11), (12, 12), (13, 13), (14, 14), (15, 15), (16, 16),
  (17, 17), (18, 18), (19, 19), (20, 20), (21, 21), (22, 22), (23, 23), (24, 24),
  (25, 25), (26, 26), (27, 27), (28, 28), (29, 29), (30, 30), (31, 31), (32, 32);
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm16_append_v2_${uuid0} (id BIGINT, part BIGINT)
PARTITION BY (part)
TBLPROPERTIES ("format-version" = "2", "commit.manifest.target-size-bytes" = "1024");
INSERT INTO iceberg_compat_${suite_uuid0}.${case_db}.rwm16_append_v2_${uuid0} VALUES
  (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8),
  (9, 9), (10, 10), (11, 11), (12, 12), (13, 13), (14, 14), (15, 15), (16, 16),
  (17, 17), (18, 18), (19, 19), (20, 20), (21, 21), (22, 22), (23, 23), (24, 24),
  (25, 25), (26, 26), (27, 27), (28, 28), (29, 29), (30, 30), (31, 31), (32, 32);
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm16_append_v3_${uuid0} (id BIGINT, part BIGINT)
PARTITION BY (part)
TBLPROPERTIES ("format-version" = "3", "commit.manifest.target-size-bytes" = "1024");
INSERT INTO iceberg_compat_${suite_uuid0}.${case_db}.rwm16_append_v3_${uuid0} VALUES
  (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8),
  (9, 9), (10, 10), (11, 11), (12, 12), (13, 13), (14, 14), (15, 15), (16, 16),
  (17, 17), (18, 18), (19, 19), (20, 20), (21, 21), (22, 22), (23, 23), (24, 24),
  (25, 25), (26, 26), (27, 27), (28, 28), (29, 29), (30, 30), (31, 31), (32, 32);

-- query 2
SELECT count(*) = 32 AND count(DISTINCT part) = 32 AND sum(id) = 528 AS rows_match
FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm16_append_v1_${uuid0};

-- query 3
SELECT count(*) = 32 AND count(DISTINCT part) = 32 AND sum(id) = 528 AS rows_match
FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm16_append_v2_${uuid0};

-- query 4
SELECT count(*) = 32 AND count(DISTINCT part) = 32 AND sum(id) = 528 AS rows_match
FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm16_append_v3_${uuid0};

-- query 5
-- @result_contains=SPARK_RWM16_APPEND_OK
shell: set -eu
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-rolling-append-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat > "$tmp_scala" <<'SPARK_SCALA'
import org.apache.iceberg.spark.Spark3Util
Seq("ice_rest.${case_db}.rwm16_append_v1_${uuid0}", "ice_rest.${case_db}.rwm16_append_v2_${uuid0}", "ice_rest.${case_db}.rwm16_append_v3_${uuid0}").foreach { name =>
  val table = Spark3Util.loadIcebergTable(spark, name)
  val snapshot = table.currentSnapshot().snapshotId()
  val rows = spark.sql(s"SELECT count(*), count(DISTINCT part), sum(id) FROM $name").head()
  require(rows.getLong(0) == 32 && rows.getLong(1) == 32 && rows.getLong(2) == 528, s"unexpected rows for $name")
  val manifests = spark.sql(s"SELECT partition_spec_id, content, length FROM $name.manifests WHERE added_snapshot_id = $snapshot").collect().toSeq
  require(manifests.size > 1 && manifests.forall(_.getInt(1) == 0), s"the append did not roll data manifests for $name")
  val groups = manifests.groupBy(row => (row.getInt(0), row.getInt(1)))
  require(groups.values.forall(_.count(_.getLong(2) < 1024L) <= 1), s"too many below-target manifests in $name")
}
println("SPARK_RWM16_APPEND_OK")
SPARK_SCALA
spark_status=0
spark_out=$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1) || spark_status=$?
printf '%s\n' "$spark_out"
[ "$spark_status" -eq 0 ]
printf '%s\n' "$spark_out" | tr '\r' '\n' | grep -Fx SPARK_RWM16_APPEND_OK

-- query 6
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.rwm16_append_v1_${uuid0} FORCE;
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.rwm16_append_v2_${uuid0} FORCE;
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.rwm16_append_v3_${uuid0} FORCE;
