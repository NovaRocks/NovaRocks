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
-- @tags=iceberg,mv,strict_recovery,external_maintenance

-- query 1
-- @skip_result_check=true
USE iceberg_compat_${suite_uuid0}.${case_db};
SET SESSION enable_materialized_view_rewrite = false;
CREATE TABLE recovery_source (rid BIGINT, k BIGINT, visible BOOLEAN, payload VARCHAR)
WITH (format_version = 3);
INSERT INTO recovery_source
SELECT -id, CAST(NULL AS BIGINT), false, to_hex(sha256(to_utf8(CAST(id AS VARCHAR))))
FROM TABLE(sequence(1, 4096)) AS cold(id);
INSERT INTO recovery_source VALUES ROW(1, 10, true, 'old'), ROW(2, 20, true, 'kept');
CREATE MATERIALIZED VIEW pending_equivalent_mv AS SELECT k FROM recovery_source WHERE visible;
REFRESH MATERIALIZED VIEW pending_equivalent_mv;
DELETE FROM recovery_source WHERE rid = 1;
INSERT INTO recovery_source VALUES ROW(3, 30, true, 'next');

-- query 2
-- @retry_count=1
-- @query_lifecycle_fault=mv-content-target-enumeration-close,0
-- @expect_error=MV refresh did not prove complete scan enumeration and delivery
REFRESH MATERIALIZED VIEW pending_equivalent_mv;

-- query 3
-- @skip_result_check=true
-- @result_contains=SPARK_PENDING_REPLACE_OK
shell: set -eu
workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
out="$workspace/logs/sql-mv-content-interop/${uuid0}"
mkdir -p "$out"
scala="$out/rewrite-pending-target.scala"
cat "$workspace/tests/sql/fixtures/iru5-commit-interop/SparkCommitInterop.scala" > "$scala"
cat >> "$scala" <<'SPARK_SCALA'
Iru5SparkCommitInterop.checked("SPARK_PENDING_REPLACE_OK") {
  val target = "ice_rest.${case_db}.pending_equivalent_mv"
  val before = Iru5SparkCommitInterop.table(spark, target).currentSnapshot().snapshotId()
  spark.sql("CALL ice_rest.system.rewrite_data_files(table => '${case_db}.pending_equivalent_mv', options => map('rewrite-all', 'true'))").collect()
  val after = Iru5SparkCommitInterop.table(spark, target).currentSnapshot()
  require(after.snapshotId() != before && after.operation() == "replace", "Expected an actual Spark physical replacement")
  val rows = spark.sql(s"SELECT k FROM $target ORDER BY k").collect().map(_.getLong(0)).toVector
  require(rows == Vector(10L, 20L), "Physical replacement must retain the original S bag")
}
SPARK_SCALA
"$workspace/docker/iceberg-rest/spark-shell.sh" "$scala" > "$out/rewrite-pending-target.log" 2>&1 || {
  cat "$out/rewrite-pending-target.log"
  exit 1
}
grep -Fx SPARK_PENDING_REPLACE_OK "$out/rewrite-pending-target.log"

-- query 4
-- @skip_result_check=true
-- @result_contains=MV refresh: StrictRecovery
EXPLAIN REFRESH MATERIALIZED VIEW pending_equivalent_mv;

-- query 5
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW pending_equivalent_mv;

-- query 6
-- @imv_equivalence_check=pending_equivalent_mv
SELECT k FROM pending_equivalent_mv ORDER BY k;

-- query 7
-- @skip_result_check=true
CREATE MATERIALIZED VIEW pending_changed_mv AS SELECT k FROM recovery_source WHERE visible;
REFRESH MATERIALIZED VIEW pending_changed_mv;
DELETE FROM recovery_source WHERE rid = 3;
INSERT INTO recovery_source VALUES ROW(4, 40, true, 'replacement');

-- query 8
-- @retry_count=1
-- @query_lifecycle_fault=mv-content-target-enumeration-close,0
-- @expect_error=MV refresh did not prove complete scan enumeration and delivery
REFRESH MATERIALIZED VIEW pending_changed_mv;

-- query 9
-- @skip_result_check=true
-- @result_contains=SPARK_PENDING_APPEND_OK
shell: set -eu
workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
out="$workspace/logs/sql-mv-content-interop/${uuid0}"
mkdir -p "$out"
scala="$out/append-pending-target.scala"
cat "$workspace/tests/sql/fixtures/iru5-commit-interop/SparkCommitInterop.scala" > "$scala"
cat >> "$scala" <<'SPARK_SCALA'
Iru5SparkCommitInterop.checked("SPARK_PENDING_APPEND_OK") {
  val target = "ice_rest.${case_db}.pending_changed_mv"
  val before = Iru5SparkCommitInterop.table(spark, target).currentSnapshot().snapshotId()
  spark.sql(s"INSERT INTO $target VALUES (999)")
  val after = Iru5SparkCommitInterop.table(spark, target).currentSnapshot()
  require(after.snapshotId() != before && after.operation() == "append", "Expected an actual Spark external append")
  val rows = spark.sql(s"SELECT k FROM $target ORDER BY k").collect().map(_.getLong(0)).toVector
  require(rows == Vector(20L, 30L, 999L), "Expected the original S rows plus the external append")
}
SPARK_SCALA
"$workspace/docker/iceberg-rest/spark-shell.sh" "$scala" > "$out/append-pending-target.log" 2>&1 || {
  cat "$out/append-pending-target.log"
  exit 1
}
grep -Fx SPARK_PENDING_APPEND_OK "$out/append-pending-target.log"

-- query 10
-- @expect_error=explicit rebuild is required
REFRESH MATERIALIZED VIEW pending_changed_mv;

-- query 11
-- @skip_result_check=true
CREATE MATERIALIZED VIEW retired_mv AS SELECT k FROM recovery_source WHERE visible;
REFRESH MATERIALIZED VIEW retired_mv;
DELETE FROM recovery_source WHERE rid = 2;
INSERT INTO recovery_source VALUES ROW(5, 50, true, 'lost');

-- query 12
-- @skip_result_check=true
-- @query_lifecycle_fault=mv-content-apply-drop-insert,0
-- @query_lifecycle_fault=mv-content-apply-drop-insert,1
-- @query_lifecycle_fault=mv-content-apply-drop-insert,2
-- @be_log_contains=NOVAROCKS_MV_CONTENT_APPLY_INSERT_DROPPED
REFRESH MATERIALIZED VIEW retired_mv;

-- query 13
-- @skip_result_check=true
DELETE FROM recovery_source WHERE rid = 5;

-- query 14
-- @expect_error=MV delete verification failed and the target requires an explicit rebuild
REFRESH MATERIALIZED VIEW retired_mv;

-- query 15
-- @skip_result_check=true
-- @result_contains=SPARK_RETIRED_REPLACE_OK
shell: set -eu
workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
out="$workspace/logs/sql-mv-content-interop/${uuid0}"
mkdir -p "$out"
scala="$out/rewrite-retired-target.scala"
cat "$workspace/tests/sql/fixtures/iru5-commit-interop/SparkCommitInterop.scala" > "$scala"
cat >> "$scala" <<'SPARK_SCALA'
Iru5SparkCommitInterop.checked("SPARK_RETIRED_REPLACE_OK") {
  val target = "ice_rest.${case_db}.retired_mv"
  val before = Iru5SparkCommitInterop.table(spark, target).currentSnapshot().snapshotId()
  spark.sql("CALL ice_rest.system.rewrite_data_files(table => '${case_db}.retired_mv', options => map('rewrite-all', 'true'))").collect()
  val after = Iru5SparkCommitInterop.table(spark, target).currentSnapshot()
  require(after.snapshotId() != before && after.operation() == "replace", "Expected an actual Spark rewrite of retired storage")
  val rows = spark.sql(s"SELECT k FROM $target ORDER BY k").collect().map(_.getLong(0)).toVector
  require(rows == Vector(40L), "Maintenance must preserve the retired target's visible bag")
}
SPARK_SCALA
"$workspace/docker/iceberg-rest/spark-shell.sh" "$scala" > "$out/rewrite-retired-target.log" 2>&1 || {
  cat "$out/rewrite-retired-target.log"
  exit 1
}
grep -Fx SPARK_RETIRED_REPLACE_OK "$out/rewrite-retired-target.log"

-- query 16
-- @expect_error=requires an explicit rebuild
REFRESH MATERIALIZED VIEW retired_mv;

-- query 17
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.retired_mv;
DROP MATERIALIZED VIEW IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.pending_changed_mv;
DROP MATERIALIZED VIEW IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.pending_equivalent_mv;
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.recovery_source;
