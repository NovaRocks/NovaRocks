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
-- @tags=mv,iceberg,rest,minio,storage-contract,concurrency
-- The runner's private REST proxy holds only the MV's target commit. Spark
-- advances the real target through the downstream catalog before that exact
-- request can reach the catalog requirement check.
-- The external commit has no MV P attachment, so this case uses its own
-- disposable REST fixture instead of leaving that target for recovery cases.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mvocc_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "static",
  "credential.object-store-metadata.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "static",
  "credential.object-store-data.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);

-- query 2
-- @skip_result_check=true
CREATE DATABASE mvocc_${uuid0}.ns_${uuid0};

-- query 3
-- @skip_result_check=true
CREATE TABLE mvocc_${uuid0}.ns_${uuid0}.fact (k STRING, v BIGINT)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
INSERT INTO mvocc_${uuid0}.ns_${uuid0}.fact VALUES ('east', 10);

-- query 4
-- @skip_result_check=true
SET CATALOG mvocc_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW mv_conflict
DISTRIBUTED BY HASH(k) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k, v FROM fact;

-- query 5
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.mv_conflict,publications=1
REFRESH MATERIALIZED VIEW mv_conflict WITH SYNC MODE;

-- query 6
-- @skip_result_check=true
INSERT INTO mvocc_${uuid0}.ns_${uuid0}.fact VALUES ('west', 20);

-- query 7
-- Spark's committed DELETE changes main after this refresh freezes its
-- original expected main. The old publication must fail without rebasing or
-- replaying its computed rows onto the external snapshot.
-- @publication_catalog_fault=table-commit,before-requirement-check-hold-for-concurrent-shell
-- @publication_catalog_concurrent_shell=tmp_sql=$(mktemp "${TMPDIR:-/tmp}/novarocks-mv-physical-conflict-XXXXXX.sql"); trap 'rm -f "$tmp_sql"' EXIT; printf '%s\n' "DELETE FROM ice_rest.ns_${uuid0}.mv_conflict WHERE k = 'east';" > "$tmp_sql"; "${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
-- @expect_error=dependency: RefUnchanged
SET CATALOG mvocc_${uuid0};
USE ns_${uuid0};
REFRESH MATERIALIZED VIEW mv_conflict FULL WITH SYNC MODE;

-- query 8
-- The private REST fixture is disposable because the external writer does
-- not attach an MV P to its new snapshot. Exactly two snapshots remain: the
-- first MV publication and the external DELETE, with no stale MV commit.
-- @result_contains=MV_PHYSICAL_CONFLICT_OK
shell: set -eu
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-mv-physical-conflict-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat > "$tmp_scala" <<'SPARK_SCALA'
import scala.jdk.CollectionConverters._
import org.apache.iceberg.spark.Spark3Util

val table = Spark3Util.loadIcebergTable(spark, "ice_rest.ns_${uuid0}.mv_conflict")
require(table.snapshots().asScala.size == 2, "stale MV publication committed after the external DELETE")
println("MV_PHYSICAL_CONFLICT_OK")
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)"
printf '%s\n' "$spark_out"
printf '%s\n' "$spark_out" | grep -F "MV_PHYSICAL_CONFLICT_OK"
