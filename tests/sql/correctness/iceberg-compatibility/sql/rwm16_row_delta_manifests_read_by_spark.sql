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
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm16_dml_v2_${uuid0} (id BIGINT, part BIGINT, keep BOOLEAN) WITH (partitioning = ARRAY['part'], format_version = 2, extra_properties = MAP(ARRAY['commit.manifest.target-size-bytes'], ARRAY['1024']));
INSERT INTO iceberg_compat_${suite_uuid0}.${case_db}.rwm16_dml_v2_${uuid0} VALUES
  ROW(1, 1, true), ROW(2, 1, false), ROW(3, 2, true), ROW(4, 2, false), ROW(5, 3, true), ROW(6, 3, false),
  ROW(7, 4, true), ROW(8, 4, false), ROW(9, 5, true), ROW(10, 5, false), ROW(11, 6, true), ROW(12, 6, false),
  ROW(13, 7, true), ROW(14, 7, false), ROW(15, 8, true), ROW(16, 8, false), ROW(17, 9, true), ROW(18, 9, false),
  ROW(19, 10, true), ROW(20, 10, false), ROW(21, 11, true), ROW(22, 11, false), ROW(23, 12, true), ROW(24, 12, false),
  ROW(25, 13, true), ROW(26, 13, false), ROW(27, 14, true), ROW(28, 14, false), ROW(29, 15, true), ROW(30, 15, false),
  ROW(31, 16, true), ROW(32, 16, false), ROW(33, 17, true), ROW(34, 17, false), ROW(35, 18, true), ROW(36, 18, false),
  ROW(37, 19, true), ROW(38, 19, false), ROW(39, 20, true), ROW(40, 20, false), ROW(41, 21, true), ROW(42, 21, false),
  ROW(43, 22, true), ROW(44, 22, false), ROW(45, 23, true), ROW(46, 23, false), ROW(47, 24, true), ROW(48, 24, false);
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.rwm16_dml_v3_${uuid0} (id BIGINT, part BIGINT, keep BOOLEAN) WITH (partitioning = ARRAY['part'], format_version = 3, extra_properties = MAP(ARRAY['commit.manifest.target-size-bytes'], ARRAY['1024']));
INSERT INTO iceberg_compat_${suite_uuid0}.${case_db}.rwm16_dml_v3_${uuid0} VALUES
  ROW(1, 1, true), ROW(2, 1, false), ROW(3, 2, true), ROW(4, 2, false), ROW(5, 3, true), ROW(6, 3, false),
  ROW(7, 4, true), ROW(8, 4, false), ROW(9, 5, true), ROW(10, 5, false), ROW(11, 6, true), ROW(12, 6, false),
  ROW(13, 7, true), ROW(14, 7, false), ROW(15, 8, true), ROW(16, 8, false), ROW(17, 9, true), ROW(18, 9, false),
  ROW(19, 10, true), ROW(20, 10, false), ROW(21, 11, true), ROW(22, 11, false), ROW(23, 12, true), ROW(24, 12, false),
  ROW(25, 13, true), ROW(26, 13, false), ROW(27, 14, true), ROW(28, 14, false), ROW(29, 15, true), ROW(30, 15, false),
  ROW(31, 16, true), ROW(32, 16, false), ROW(33, 17, true), ROW(34, 17, false), ROW(35, 18, true), ROW(36, 18, false),
  ROW(37, 19, true), ROW(38, 19, false), ROW(39, 20, true), ROW(40, 20, false), ROW(41, 21, true), ROW(42, 21, false),
  ROW(43, 22, true), ROW(44, 22, false), ROW(45, 23, true), ROW(46, 23, false), ROW(47, 24, true), ROW(48, 24, false);

-- query 2
-- @skip_result_check=true
DELETE FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm16_dml_v2_${uuid0} WHERE keep = false;
DELETE FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm16_dml_v3_${uuid0} WHERE keep = false;

-- query 3
SELECT count(*) = 24 AND count(DISTINCT part) = 24 AND sum(id) = 576 AS rows_match
FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm16_dml_v2_${uuid0};

-- query 4
SELECT count(*) = 24 AND count(DISTINCT part) = 24 AND sum(id) = 576 AS rows_match
FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm16_dml_v3_${uuid0};

-- query 5
-- @skip_result_check=true
UPDATE iceberg_compat_${suite_uuid0}.${case_db}.rwm16_dml_v3_${uuid0} SET id = id + 1000 WHERE keep = true;

-- query 6
SELECT count(*) = 24 AND count(DISTINCT part) = 24 AND sum(id) = 24576 AS rows_match
FROM iceberg_compat_${suite_uuid0}.${case_db}.rwm16_dml_v3_${uuid0};

-- query 7
-- @result_contains=SPARK_RWM16_ROW_DELTA_OK
shell: set -eu
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-rolling-dml-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat > "$tmp_scala" <<'SPARK_SCALA'
import org.apache.iceberg.spark.Spark3Util
val v2 = "ice_rest.${case_db}.rwm16_dml_v2_${uuid0}"
val v3 = "ice_rest.${case_db}.rwm16_dml_v3_${uuid0}"
val v2Table = Spark3Util.loadIcebergTable(spark, v2)
val v3Table = Spark3Util.loadIcebergTable(spark, v3)
val v2Delete = v2Table.currentSnapshot().snapshotId()
val v3Delete = v3Table.currentSnapshot().parentId().longValue()
def checkRows(name: String, snapshot: Long, expectedSum: Long): Unit = {
  val result = spark.sql(s"SELECT count(*), count(DISTINCT part), sum(id) FROM $name VERSION AS OF $snapshot").head()
  require(result.getLong(0) == 24 && result.getLong(1) == 24 && result.getLong(2) == expectedSum, s"wrong rows at $name snapshot $snapshot")
}
def checkDeleteManifests(name: String, snapshot: Long): Unit = {
  val count = spark.sql(s"SELECT count(DISTINCT path) FROM $name.all_manifests WHERE content = 1 AND added_snapshot_id = $snapshot").head().getLong(0)
  require(count > 1, s"the original DELETE did not roll delete manifests for $name")
}
checkRows(v2, v2Delete, 576L)
checkRows(v3, v3Delete, 576L)
checkRows(v3, v3Table.currentSnapshot().snapshotId(), 24576L)
checkDeleteManifests(v2, v2Delete)
checkDeleteManifests(v3, v3Delete)
println("SPARK_RWM16_ROW_DELTA_OK")
SPARK_SCALA
spark_status=0
spark_out=$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1) || spark_status=$?
printf '%s\n' "$spark_out"
[ "$spark_status" -eq 0 ]
printf '%s\n' "$spark_out" | tr '\r' '\n' | grep -Fx SPARK_RWM16_ROW_DELTA_OK

-- query 8
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.rwm16_dml_v2_${uuid0} ;
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.rwm16_dml_v3_${uuid0} ;
