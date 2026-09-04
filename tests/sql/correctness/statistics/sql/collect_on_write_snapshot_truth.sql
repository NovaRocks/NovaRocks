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
-- This native 1FE+3BE case reads REST-catalog metadata through Spark so it
-- proves the StatisticsFile attachment, not merely a frontend presentation.

-- query 1
-- @skip_result_check=true
CREATE DATABASE IF NOT EXISTS statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0};

-- query 2
-- Start with maintenance disabled so this nonempty parent deliberately has no
-- Puffin. Enabling it for the second append must not publish an NDV that only
-- describes the second row.
-- @skip_result_check=true
CREATE TABLE statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0} (
    id BIGINT,
    k BIGINT
) TBLPROPERTIES ('novarocks.statistics.collect-on-write' = 'false');

-- query 3
-- @skip_result_check=true
INSERT INTO statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0} VALUES (1, 10);

-- query 4
-- @skip_result_check=true
ALTER TABLE statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0}
SET TBLPROPERTIES ('novarocks.statistics.collect-on-write' = 'true');

-- query 5
-- @skip_result_check=true
INSERT INTO statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0} VALUES (2, 20);

-- query 6
-- @result_contains=COLLECT_ON_WRITE_MISSING_PARENT_OK
shell: set -eu
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-collect-on-write-missing-parent-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat > "$tmp_scala" <<'SPARK_SCALA'
import scala.jdk.CollectionConverters._
import org.apache.iceberg.spark.Spark3Util

val table = Spark3Util.loadIcebergTable(spark, "ice_rest.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0}")
val current = table.currentSnapshot().snapshotId()
require(table.statisticsFiles().asScala.forall(_.snapshotId() != current), "nonempty parent without statistics gained a partial current-snapshot StatisticsFile")
println("COLLECT_ON_WRITE_MISSING_PARENT_OK")
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)"
printf '%s\n' "$spark_out"
printf '%s\n' "$spark_out" | grep -F "COLLECT_ON_WRITE_MISSING_PARENT_OK"

-- query 7
-- Seed authoritative parent statistics through the ordinary aggregate ANALYZE
-- path. The following DELETE is ineligible for collect-on-write, so this case
-- isolates the rule that an ancestor StatisticsFile is not reseated onto a
-- snapshot whose visible row set it does not describe.
-- @skip_result_check=true
ANALYZE TABLE statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0};

-- query 8
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=SUCCEEDED
-- @skip_result_check=true
SHOW ANALYZE JOBS;

-- query 9
-- With a compatible parent sketch, collect-on-write must publish the union for
-- the same newly committed snapshot. Duplicate values exercise Theta union
-- instead of making the expected NDV equal to the number of written rows.
-- @skip_result_check=true
INSERT INTO statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0}
VALUES (2, 20), (3, 30);

-- query 10
-- @result_contains=COLLECT_ON_WRITE_INCREMENTAL_OK
shell: set -eu
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-collect-on-write-incremental-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat > "$tmp_scala" <<'SPARK_SCALA'
import scala.jdk.CollectionConverters._
import org.apache.iceberg.puffin.StandardBlobTypes
import org.apache.iceberg.spark.Spark3Util

val table = Spark3Util.loadIcebergTable(spark, "ice_rest.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0}")
val current = table.currentSnapshot().snapshotId()
val currentStatistics = table.statisticsFiles().asScala.filter(_.snapshotId() == current).toSeq
require(currentStatistics.size == 1, "collect-on-write did not publish exactly one current-snapshot StatisticsFile")
val theta = currentStatistics.flatMap(_.blobMetadata().asScala).filter(
  _.`type`() == StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1
)
require(theta.size == 2, "collect-on-write did not publish one Theta blob for each supported field")
require(theta.forall(_.properties().get("ndv") == "3"), "collect-on-write parent union did not publish NDV 3")
println("COLLECT_ON_WRITE_INCREMENTAL_OK")
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)"
printf '%s\n' "$spark_out"
printf '%s\n' "$spark_out" | grep -F "COLLECT_ON_WRITE_INCREMENTAL_OK"

-- query 11
-- Repair from the full table and then append again. This alternation exercises
-- both producers without changing the published Puffin contract.
-- @skip_result_check=true
ANALYZE TABLE statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0};

-- query 12
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=SUCCEEDED
-- @skip_result_check=true
SHOW ANALYZE JOBS;

-- query 13
-- @skip_result_check=true
INSERT INTO statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0}
VALUES (4, 40);

-- query 14
-- @result_contains=COLLECT_ON_WRITE_ALTERNATING_OK
shell: set -eu
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-collect-on-write-alternating-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat > "$tmp_scala" <<'SPARK_SCALA'
import scala.jdk.CollectionConverters._
import org.apache.iceberg.puffin.StandardBlobTypes
import org.apache.iceberg.spark.Spark3Util

val table = Spark3Util.loadIcebergTable(spark, "ice_rest.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0}")
val current = table.currentSnapshot().snapshotId()
val theta = table.statisticsFiles().asScala
  .filter(_.snapshotId() == current)
  .flatMap(_.blobMetadata().asScala)
  .filter(_.`type`() == StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1)
  .toSeq
require(theta.size == 2, "alternating ANALYZE and INSERT did not publish both Theta blobs")
require(theta.forall(_.properties().get("ndv") == "4"), "alternating ANALYZE and INSERT did not publish NDV 4")
println("COLLECT_ON_WRITE_ALTERNATING_OK")
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)"
printf '%s\n' "$spark_out"
printf '%s\n' "$spark_out" | grep -F "COLLECT_ON_WRITE_ALTERNATING_OK"

-- query 15
-- DELETE must not reseat this current StatisticsFile onto its new snapshot.
-- @skip_result_check=true
DELETE FROM statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0} WHERE id = 1;

-- query 16
-- @result_contains=COLLECT_ON_WRITE_DELETE_BASIS_OK
shell: set -eu
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-collect-on-write-delete-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat > "$tmp_scala" <<'SPARK_SCALA'
import scala.jdk.CollectionConverters._
import org.apache.iceberg.spark.Spark3Util

val table = Spark3Util.loadIcebergTable(spark, "ice_rest.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0}")
val current = table.currentSnapshot().snapshotId()
require(table.statisticsFiles().asScala.forall(_.snapshotId() != current), "DELETE reseated a StatisticsFile onto the new snapshot")
require(table.statisticsFiles().asScala.exists(_.snapshotId() != current), "DELETE discarded the ancestor StatisticsFile")
println("COLLECT_ON_WRITE_DELETE_BASIS_OK")
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)"
printf '%s\n' "$spark_out"
printf '%s\n' "$spark_out" | grep -F "COLLECT_ON_WRITE_DELETE_BASIS_OK"

-- query 17
-- @skip_result_check=true
DROP TABLE statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.cow_missing_parent_${uuid0} FORCE;
