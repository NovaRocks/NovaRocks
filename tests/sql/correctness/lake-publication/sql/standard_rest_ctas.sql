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
-- The proxy recognizes only ordinary Iceberg REST stage-create and table
-- commit requests. It never persists operation state or offers a recovery
-- endpoint. Each assertion below reads the table through the real Catalog.

-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS lake_publication_${suite_uuid0}.ns_${uuid0} FORCE;
CREATE DATABASE lake_publication_${suite_uuid0}.ns_${uuid0};
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.source_rows (
  id INT,
  value VARCHAR(16)
)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.source_rows VALUES
  (1, 'alpha'), (2, 'beta'), (3, 'gamma');

-- query 2
-- Frontier dispatch never occurred: the target must remain absent and the
-- statement must report a definite non-commit outcome.
-- @publication_catalog_fault=stage-create,before-dispatch
-- @expect_error_code=CommitKnownUncommitted
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.before_dispatch AS
  SELECT id, value FROM lake_publication_${suite_uuid0}.ns_${uuid0}.source_rows;

-- query 3
-- The single standard NotExist table commit succeeded at the Catalog but its
-- response was lost. The current statement may use its one same-session,
-- read-only adjudication to observe the exact marker and report committed;
-- the following restart must not replay or mutate the completed attempt.
-- @publication_catalog_fault=table-commit,after-commit-before-response
-- @skip_result_check=true
-- @restart_fe_after_step=true
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.response_lost AS
  SELECT id, value FROM lake_publication_${suite_uuid0}.ns_${uuid0}.source_rows;

-- query 4
-- The table commit has succeeded in the real Catalog, but the proxy keeps its
-- response open. The runner must kill and restart the only FE while this SQL
-- client is still blocked, then a later read proves there was no durable DML
-- recovery/replay owner.
-- @publication_catalog_fault=table-commit,after-commit-hold-for-frontend-kill
-- @skip_result_check=true
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.inflight_fe_kill AS
  SELECT id, value FROM lake_publication_${suite_uuid0}.ns_${uuid0}.source_rows;

-- query 5
-- The external-state observation is the authority, not the prior error text.
-- @retry_count=30
-- @retry_interval_ms=1000
SELECT id, value FROM lake_publication_${suite_uuid0}.ns_${uuid0}.inflight_fe_kill ORDER BY id;

-- query 6
-- Success followed by an FE restart exercises the finalization boundary. The
-- restart must not resume a completed CTAS as a second Catalog mutation.
-- @skip_result_check=true
-- @restart_fe_after_step=true
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.restart_after_success AS
  SELECT id, value FROM lake_publication_${suite_uuid0}.ns_${uuid0}.source_rows;

-- query 7
SELECT COUNT(*) AS n
  FROM lake_publication_${suite_uuid0}.ns_${uuid0}.restart_after_success;

-- query 8
-- Seed a current-snapshot parent sketch. The held append below freezes this
-- snapshot and its ordinary aggregate result before the cross-engine DELETE
-- advances the table.
-- @skip_result_check=true
CREATE TABLE lake_publication_${suite_uuid0}.ns_${uuid0}.concurrent_append_mutation (
  id BIGINT,
  k BIGINT
)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true",
  "novarocks.statistics.collect-on-write" = "true"
);
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.concurrent_append_mutation VALUES
  (1, 10), (2, 20);

-- query 9
-- The proxy stops this commit before dispatch. Spark then commits DELETE(1)
-- directly through the real REST catalog. Releasing the request forces Nova
-- to observe an actual OCC conflict and build a fresh eager attempt from the
-- mutation snapshot while reusing its already-written file and sketch body.
-- @publication_catalog_fault=table-commit,before-dispatch-hold-for-concurrent-shell
-- @publication_catalog_concurrent_shell=tmp_sql=$(mktemp "${TMPDIR:-/tmp}/novarocks-concurrent-mutation-XXXXXX.sql"); trap 'rm -f "$tmp_sql"' EXIT; printf '%s\n' "DELETE FROM ice_rest.ns_${uuid0}.concurrent_append_mutation WHERE id = 1;" > "$tmp_sql"; "${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
-- @skip_result_check=true
INSERT INTO lake_publication_${suite_uuid0}.ns_${uuid0}.concurrent_append_mutation VALUES
  (3, 30);

-- query 10
-- Both external mutations are visible exactly once. In particular, a fresh
-- attempt must not replay the data file or resurrect the deleted row.
SELECT id, k
FROM lake_publication_${suite_uuid0}.ns_${uuid0}.concurrent_append_mutation
ORDER BY id;

-- query 11
-- The current snapshot is the append rebased on top of Spark's DELETE. The
-- mutation invalidates the original incremental parent: the fresh attempt must
-- not relabel that sketch as current-snapshot evidence. The original ancestor
-- statistics remain readable with their original basis.
-- @result_contains=CONCURRENT_APPEND_MUTATION_OK
shell: set -eu
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-concurrent-append-mutation-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat > "$tmp_scala" <<'SPARK_SCALA'
import scala.jdk.CollectionConverters._
import org.apache.iceberg.puffin.StandardBlobTypes
import org.apache.iceberg.spark.Spark3Util

val table = Spark3Util.loadIcebergTable(spark, "ice_rest.ns_${uuid0}.concurrent_append_mutation")
val current = table.currentSnapshot()
val parent = table.snapshot(current.parentId())
require(current.operation() == "append", "current snapshot is not the rebased Nova append")
require(parent.operation() == "overwrite", "Nova append did not rebase on the concurrent copy-on-write DELETE")
require(table.snapshots().asScala.size == 3, "conflict created an extra committed snapshot")
val statistics = table.statisticsFiles().asScala.toSeq
require(statistics.forall(_.snapshotId() != current.snapshotId()), "mutation conflict produced false current-snapshot statistics")
require(statistics.size == 1, "original ancestor StatisticsFile was not preserved exactly once")
val measured = table.snapshot(statistics.head.snapshotId())
val theta = statistics.head.blobMetadata().asScala
  .filter(_.`type`() == StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1)
  .toSeq
require(theta.size == 2, "ancestor StatisticsFile did not preserve both Theta fields")
require(theta.forall(_.sourceSnapshotId() == measured.snapshotId()), "Theta blob lost its ancestor snapshot basis")
require(theta.forall(_.sourceSnapshotSequenceNumber() == measured.sequenceNumber()), "Theta blob lost its ancestor sequence basis")
require(theta.forall(_.properties().get("ndv") == "2"), "ancestor NDV was changed by the conflicting append")
println("CONCURRENT_APPEND_MUTATION_OK")
SPARK_SCALA
spark_out="$("${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" 2>&1)"
printf '%s\n' "$spark_out"
printf '%s\n' "$spark_out" | grep -F "CONCURRENT_APPEND_MUTATION_OK"

-- query 12
-- @skip_result_check=true
DROP DATABASE lake_publication_${suite_uuid0}.ns_${uuid0} FORCE;
