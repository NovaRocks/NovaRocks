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
-- A native 1FE+3BE ANALYZE publishes an Iceberg-standard StatisticsFile.
-- Spark loads it through its Iceberg Table API and verifies that the Puffin
-- exposes standard Apache DataSketches Theta metadata, not a NovaRocks-only
-- catalog side channel.

-- query 1
-- Spark first creates a standard REST-catalog table. Native ANALYZE must then
-- resolve that same Iceberg snapshot, not a NovaRocks-only catalog object.
-- @result_contains=SPARK_STATISTICS_SOURCE_READY
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-statistics-source-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
CREATE NAMESPACE IF NOT EXISTS ice_rest.nr_statistics_${suite_uuid0};
DROP TABLE IF EXISTS ice_rest.nr_statistics_${suite_uuid0}.puffin_spark_${uuid0};
CREATE TABLE ice_rest.nr_statistics_${suite_uuid0}.puffin_spark_${uuid0} (
  id BIGINT,
  value BIGINT
) USING iceberg
TBLPROPERTIES ('format-version' = '3');
INSERT INTO ice_rest.nr_statistics_${suite_uuid0}.puffin_spark_${uuid0} VALUES (1, 10);
INSERT INTO ice_rest.nr_statistics_${suite_uuid0}.puffin_spark_${uuid0} VALUES (2, 20);
INSERT INTO ice_rest.nr_statistics_${suite_uuid0}.puffin_spark_${uuid0} VALUES (3, 30);
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql" >/dev/null
printf 'SPARK_STATISTICS_SOURCE_READY\n'

-- query 2
-- @skip_result_check=true
ANALYZE TABLE statistics_cat_${suite_uuid0}.nr_statistics_${suite_uuid0}.puffin_spark_${uuid0};

-- query 3
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=SUCCEEDED
-- @be_log_be_count_at_least=NOVAROCKS_STATISTICS_FRAGMENT_COLLECTED,3
-- @skip_result_check=true
SHOW ANALYZE JOBS;

-- query 4
-- @result_contains=SPARK_PUFFIN_STATISTICS_OK
shell: set -eu
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-statistics-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat > "$tmp_scala" <<'SPARK_SCALA'
import scala.jdk.CollectionConverters._
import org.apache.iceberg.puffin.StandardBlobTypes
import org.apache.iceberg.spark.Spark3Util

val table = Spark3Util.loadIcebergTable(
  spark,
  "ice_rest.nr_statistics_${suite_uuid0}.puffin_spark_${uuid0}"
)
val statisticsFiles = table.statisticsFiles().asScala.toSeq
require(statisticsFiles.nonEmpty, "Spark did not observe an Iceberg StatisticsFile")
val theta = statisticsFiles.flatMap(_.blobMetadata().asScala).filter(
  _.`type`() == StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1
)
require(theta.nonEmpty, "Spark did not observe standard Apache DataSketches Theta Puffin metadata")
println("SPARK_PUFFIN_STATISTICS_OK statistics_files=" + statisticsFiles.size + " theta_blobs=" + theta.size)
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)"
printf '%s\n' "$spark_out"
printf '%s\n' "$spark_out" | grep -F "SPARK_PUFFIN_STATISTICS_OK"

-- query 5
-- @result_contains=SPARK_STATISTICS_SOURCE_DROPPED
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-statistics-drop-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
DROP TABLE ice_rest.nr_statistics_${suite_uuid0}.puffin_spark_${uuid0};
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql" >/dev/null
printf 'SPARK_STATISTICS_SOURCE_DROPPED\n'
