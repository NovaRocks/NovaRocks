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
-- @tags=iceberg,ivm,projection,optimize,truncate
-- Exercise both replacement and full-file deletion with the same projection MV.

-- query 1
-- @skip_result_check=true
CREATE CATALOG ivm_optimize_truncate_ice_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA ivm_optimize_truncate_ice_${uuid0}.ns_${uuid0};
USE ivm_optimize_truncate_ice_${uuid0}.ns_${uuid0};
SET SESSION enable_materialized_view_rewrite = false;
CREATE TABLE orders (
  id BIGINT NOT NULL,
  customer VARCHAR,
  amount BIGINT
) WITH (format_version = 3);
INSERT INTO orders VALUES ROW(1, 'A', 10), ROW(2, 'A', 20);
INSERT INTO orders VALUES ROW(3, 'B', 30);
CREATE MATERIALIZED VIEW orders_maintenance_mv AS
SELECT customer, amount FROM orders WHERE amount >= 10;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_maintenance_mv;

-- query 3
-- @imv_equivalence_check=orders_maintenance_mv
SELECT customer, amount FROM orders_maintenance_mv ORDER BY customer, amount;

-- query 4
-- The metric golden requires an actual two-file rewrite.
ALTER TABLE orders EXECUTE optimize;

-- query 5
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_maintenance_mv;

-- query 6
-- @imv_equivalence_check=orders_maintenance_mv
SELECT customer, amount FROM orders_maintenance_mv ORDER BY customer, amount;

-- query 7
-- @skip_result_check=true
TRUNCATE TABLE orders;

-- query 8
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_maintenance_mv;

-- query 9
-- @imv_equivalence_check=orders_maintenance_mv
SELECT count(*) AS live_rows FROM orders_maintenance_mv;

-- query 10
SELECT count(*) AS live_rows FROM orders;

-- query 11
-- @skip_result_check=true
-- Seed two fresh files, including repeated projected rows, after the TRUNCATE control.
INSERT INTO orders VALUES ROW(11, 'A', 10), ROW(12, 'A', 10);
INSERT INTO orders VALUES ROW(13, 'B', 30), ROW(14, 'B', 30), ROW(15, 'X', 0);
REFRESH MATERIALIZED VIEW orders_maintenance_mv;

-- query 12
-- @skip_result_check=true
-- @expect_update_count=1
DELETE FROM orders WHERE id = 11;

-- query 13
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_maintenance_mv;
CREATE TABLE refresh_checkpoints (
  phase VARCHAR NOT NULL,
  snapshot_id BIGINT NOT NULL,
  total_records BIGINT,
  publication VARCHAR
) WITH (format_version = 3);
INSERT INTO refresh_checkpoints
SELECT 'a8_source_before', s.snapshot_id, CAST(element_at(s.summary, 'total-records') AS BIGINT), CAST(NULL AS VARCHAR)
FROM "orders$snapshots" AS s JOIN "orders$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';
INSERT INTO refresh_checkpoints
SELECT 'a8_target_before', s.snapshot_id, CAST(element_at(s.summary, 'total-records') AS BIGINT),
       element_at(s.summary, 'novarocks.doc.v2.record.novarocks.mv~publication')
FROM "orders_maintenance_mv$snapshots" AS s
JOIN "orders_maintenance_mv$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 14
-- @result_contains=RWM10A_SCHEMA_AND_REPLACE_OK
-- Spark compacts the existing files under the evolved schema and applies the old DV.
shell: set -eu
sql_file="$(mktemp "${TMPDIR:-/tmp}/novarocks-rwm10a-replace-XXXXXX.sql")"
trap 'rm -f "$sql_file"' EXIT
cat > "$sql_file" <<'SPARK_SQL'
ALTER TABLE ice_rest.ns_${uuid0}.orders ADD COLUMN optional_note STRING;
CALL ice_rest.system.rewrite_data_files(table => 'ns_${uuid0}.orders', options => map('rewrite-all', 'true'));
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$sql_file"
printf 'RWM10A_SCHEMA_AND_REPLACE_OK\n'

-- query 15
SELECT s.operation, s.parent_id = p.snapshot_id AS direct_child,
       CAST(element_at(s.summary, 'total-records') AS BIGINT) < p.total_records AS applied_delete
FROM "orders$snapshots" AS s JOIN "orders$refs" AS r ON s.snapshot_id = r.snapshot_id
CROSS JOIN refresh_checkpoints AS p
WHERE r.name = 'main' AND p.phase = 'a8_source_before';

-- query 16
-- @skip_result_check=true
INSERT INTO refresh_checkpoints
SELECT 'a8_source_to', s.snapshot_id, CAST(element_at(s.summary, 'total-records') AS BIGINT), CAST(NULL AS VARCHAR)
FROM "orders$snapshots" AS s JOIN "orders$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';
SET SESSION explain_detail = 'VERBOSE';

-- query 17
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_maintenance_mv;

-- query 18
SELECT s.parent_id = p.snapshot_id AS direct_child,
       p.publication IS NOT NULL
         AND element_at(s.summary, 'novarocks.doc.v2.record.novarocks.mv~publication') <> p.publication AS new_publication,
       CAST(element_at(s.summary, 'total-records') AS BIGINT) = p.total_records AS same_record_count,
       COALESCE(CAST(element_at(s.summary, 'added-data-files') AS BIGINT), 0) = 0
         AND COALESCE(CAST(element_at(s.summary, 'deleted-data-files') AS BIGINT), 0) = 0
         AND COALESCE(CAST(element_at(s.summary, 'added-delete-files') AS BIGINT), 0) = 0
         AND COALESCE(CAST(element_at(s.summary, 'removed-delete-files') AS BIGINT), 0) = 0 AS no_file_changes
FROM "orders_maintenance_mv$snapshots" AS s
JOIN "orders_maintenance_mv$refs" AS r ON s.snapshot_id = r.snapshot_id
CROSS JOIN refresh_checkpoints AS p
WHERE r.name = 'main' AND p.phase = 'a8_target_before';

-- query 19
-- @imv_equivalence_check=orders_maintenance_mv
SELECT customer, amount FROM orders_maintenance_mv ORDER BY customer, amount;

-- query 20
-- @skip_result_check=true
INSERT INTO orders (id, customer, amount) VALUES ROW(16, 'A', 10), ROW(17, 'B', 30);

-- query 21
-- @skip_result_check=true
-- @result_contains=MV refresh: FullReplace
-- @result_contains=MV optimizer cost AppendOnly:
-- @result_contains=exact change-window volume and source manifests
-- @result_contains=MV optimizer cost FullReplace:
-- @result_contains=complete source manifests
EXPLAIN REFRESH MATERIALIZED VIEW orders_maintenance_mv;

-- query 22
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_maintenance_mv;
INSERT INTO refresh_checkpoints
SELECT 'a9_source_before', s.snapshot_id, CAST(element_at(s.summary, 'total-records') AS BIGINT), CAST(NULL AS VARCHAR)
FROM "orders$snapshots" AS s JOIN "orders$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';
INSERT INTO refresh_checkpoints
SELECT 'a9_target_before', s.snapshot_id, CAST(element_at(s.summary, 'total-records') AS BIGINT),
       element_at(s.summary, 'novarocks.doc.v2.record.novarocks.mv~publication')
FROM "orders_maintenance_mv$snapshots" AS s
JOIN "orders_maintenance_mv$refs" AS r ON s.snapshot_id = r.snapshot_id
WHERE r.name = 'main';

-- query 23
-- @skip_result_check=true
-- @expect_update_count=1
DELETE FROM orders WHERE id = 13;

-- query 24
-- @skip_result_check=true
ALTER TABLE orders EXECUTE optimize;

-- query 25
SELECT d.operation AS delete_operation, s.operation AS optimize_operation,
       d.parent_id = p.snapshot_id AS directly_after_refresh,
       COALESCE(CAST(element_at(d.summary, 'added-position-delete-files') AS BIGINT), 0) > 0 AS added_dv,
       CAST(element_at(s.summary, 'total-records') AS BIGINT) < p.total_records AS applied_delete
FROM "orders$snapshots" AS s JOIN "orders$refs" AS r ON s.snapshot_id = r.snapshot_id
JOIN "orders$snapshots" AS d ON s.parent_id = d.snapshot_id
CROSS JOIN refresh_checkpoints AS p
WHERE r.name = 'main' AND p.phase = 'a9_source_before';

-- query 26
-- @skip_result_check=true
-- @result_contains_any=MV refresh: ContentLocate
-- @result_contains_any=MV refresh: FullReplace
-- @result_contains=MV optimizer cost ContentLocate:
-- @result_contains=exact change-window volume, source and target manifests
-- @result_contains=MV optimizer cost FullReplace:
-- @result_contains=complete source manifests
EXPLAIN REFRESH MATERIALIZED VIEW orders_maintenance_mv;

-- query 27
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_maintenance_mv;

-- query 28
-- ContentLocate publishes one row delta. FullReplace publishes a witnessed
-- delete and append in one commit under the same original parent.
WITH publication AS (
    SELECT s.snapshot_id, s.parent_id, s.operation, s.summary,
           p.snapshot_id AS baseline, p.publication AS old_publication,
           d.parent_id AS delete_parent, d.operation AS delete_operation, d.summary AS delete_summary,
           element_at(s.summary, 'novarocks.write.session.v1') AS write_session,
           CAST(json_parse(element_at(d.summary, 'novarocks.mv.full-refresh-stage.v1')) AS ROW(
               version BIGINT, asserted_parent BIGINT, delete_snapshot BIGINT,
               append_snapshot BIGINT, operation_id ARRAY(BIGINT)
           )) AS stage_witness
    FROM "orders_maintenance_mv$snapshots" AS s
    JOIN "orders_maintenance_mv$refs" AS r ON s.snapshot_id = r.snapshot_id
    LEFT JOIN "orders_maintenance_mv$snapshots" AS d ON s.parent_id = d.snapshot_id
    CROSS JOIN refresh_checkpoints AS p
    WHERE r.name = 'main' AND p.phase = 'a9_target_before'
)
SELECT write_session IS NOT NULL AND (
           (parent_id = baseline AND operation IN ('delete', 'overwrite')
            AND COALESCE(CAST(element_at(summary, 'added-position-delete-files') AS BIGINT), 0) > 0)
           OR (delete_parent = baseline AND delete_operation = 'delete' AND operation = 'append'
               AND stage_witness.version = 1
               AND stage_witness.asserted_parent = baseline
               AND stage_witness.delete_snapshot = parent_id
               AND stage_witness.append_snapshot = snapshot_id
               AND cardinality(stage_witness.operation_id) = 16
               AND all_match(stage_witness.operation_id, b -> b BETWEEN 0 AND 255)
               AND array_join(transform(stage_witness.operation_id, b ->
                   substr('0123456789abcdef', b / 16 + 1, 1)
                   || substr('0123456789abcdef', b % 16 + 1, 1)), '') = replace(write_session, '-', '')
               AND element_at(delete_summary, 'novarocks.write.session.v1') IS NULL
               AND element_at(delete_summary, 'novarocks.doc.v2.record.novarocks.mv~publication') IS NULL
               AND CAST(element_at(delete_summary, 'total-records') AS BIGINT) = 0
               AND CAST(element_at(delete_summary, 'total-data-files') AS BIGINT) = 0
               AND CAST(element_at(delete_summary, 'total-delete-files') AS BIGINT) = 0)
       ) AS exact_publication_chain,
       old_publication IS NOT NULL
         AND element_at(summary, 'novarocks.doc.v2.record.novarocks.mv~publication') <> old_publication AS new_publication,
       COALESCE(CAST(element_at(summary, 'added-data-files') AS BIGINT), 0) > 0
         OR COALESCE(CAST(element_at(summary, 'deleted-data-files') AS BIGINT), 0) > 0
         OR COALESCE(CAST(element_at(summary, 'added-delete-files') AS BIGINT), 0) > 0
         OR COALESCE(CAST(element_at(summary, 'removed-delete-files') AS BIGINT), 0) > 0 AS changed_files
FROM publication;

-- query 29
-- @imv_equivalence_check=orders_maintenance_mv
SELECT customer, amount FROM orders_maintenance_mv ORDER BY customer, amount;

-- query 30
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW orders_maintenance_mv;
DROP TABLE IF EXISTS ivm_optimize_truncate_ice_${uuid0}.ns_${uuid0}.refresh_checkpoints;
DROP TABLE ivm_optimize_truncate_ice_${uuid0}.ns_${uuid0}.orders;
DROP SCHEMA ivm_optimize_truncate_ice_${uuid0}.ns_${uuid0};
DROP CATALOG ivm_optimize_truncate_ice_${uuid0};
