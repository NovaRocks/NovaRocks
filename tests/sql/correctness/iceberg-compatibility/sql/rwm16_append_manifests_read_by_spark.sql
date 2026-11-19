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
CREATE SCHEMA IF NOT EXISTS iceberg_compat_${suite_uuid0}.${case_db};
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm16_append_v1_${uuid0} (id BIGINT, part BIGINT) WITH (partitioning = ARRAY['part'], format_version = 1, extra_properties = MAP(ARRAY['commit.manifest.target-size-bytes'], ARRAY['1024']));
INSERT INTO iceberg_compat_${suite_uuid0}.${case_db}.rwm16_append_v1_${uuid0} VALUES
  ROW(1, 1), ROW(2, 2), ROW(3, 3), ROW(4, 4), ROW(5, 5), ROW(6, 6), ROW(7, 7), ROW(8, 8),
  ROW(9, 9), ROW(10, 10), ROW(11, 11), ROW(12, 12), ROW(13, 13), ROW(14, 14), ROW(15, 15), ROW(16, 16),
  ROW(17, 17), ROW(18, 18), ROW(19, 19), ROW(20, 20), ROW(21, 21), ROW(22, 22), ROW(23, 23), ROW(24, 24),
  ROW(25, 25), ROW(26, 26), ROW(27, 27), ROW(28, 28), ROW(29, 29), ROW(30, 30), ROW(31, 31), ROW(32, 32);
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm16_append_v2_${uuid0} (id BIGINT, part BIGINT) WITH (partitioning = ARRAY['part'], format_version = 2, extra_properties = MAP(ARRAY['commit.manifest.target-size-bytes'], ARRAY['1024']));
INSERT INTO iceberg_compat_${suite_uuid0}.${case_db}.rwm16_append_v2_${uuid0} VALUES
  ROW(1, 1), ROW(2, 2), ROW(3, 3), ROW(4, 4), ROW(5, 5), ROW(6, 6), ROW(7, 7), ROW(8, 8),
  ROW(9, 9), ROW(10, 10), ROW(11, 11), ROW(12, 12), ROW(13, 13), ROW(14, 14), ROW(15, 15), ROW(16, 16),
  ROW(17, 17), ROW(18, 18), ROW(19, 19), ROW(20, 20), ROW(21, 21), ROW(22, 22), ROW(23, 23), ROW(24, 24),
  ROW(25, 25), ROW(26, 26), ROW(27, 27), ROW(28, 28), ROW(29, 29), ROW(30, 30), ROW(31, 31), ROW(32, 32);
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm16_append_v3_${uuid0} (id BIGINT, part BIGINT) WITH (partitioning = ARRAY['part'], format_version = 3, extra_properties = MAP(ARRAY['commit.manifest.target-size-bytes'], ARRAY['1024']));
INSERT INTO iceberg_compat_${suite_uuid0}.${case_db}.rwm16_append_v3_${uuid0} VALUES
  ROW(1, 1), ROW(2, 2), ROW(3, 3), ROW(4, 4), ROW(5, 5), ROW(6, 6), ROW(7, 7), ROW(8, 8),
  ROW(9, 9), ROW(10, 10), ROW(11, 11), ROW(12, 12), ROW(13, 13), ROW(14, 14), ROW(15, 15), ROW(16, 16),
  ROW(17, 17), ROW(18, 18), ROW(19, 19), ROW(20, 20), ROW(21, 21), ROW(22, 22), ROW(23, 23), ROW(24, 24),
  ROW(25, 25), ROW(26, 26), ROW(27, 27), ROW(28, 28), ROW(29, 29), ROW(30, 30), ROW(31, 31), ROW(32, 32);

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
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.rwm16_append_v1_${uuid0} ;
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.rwm16_append_v2_${uuid0} ;
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.rwm16_append_v3_${uuid0} ;
