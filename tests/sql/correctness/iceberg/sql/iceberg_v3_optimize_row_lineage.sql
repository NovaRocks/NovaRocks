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
-- @tags=write_path,iceberg,optimize,row_lineage
-- Test Point:
--   OPTIMIZE on a v3 row-lineage Iceberg table preserves `_row_id` and
--   `_last_updated_sequence_number` for every surviving row, even after
--   intervening UPDATE / DELETE that changes the live row set.
-- Method:
--   Build a v3 row-lineage table, mutate it with INSERT / UPDATE / DELETE,
--   check the independent surviving seed rows, and tag the pre-OPTIMIZE state.
--   Confirm multiple active data files and active delete inputs before the
--   rewrite. Then check the current replace snapshot directly descends from
--   the tag, the covered delete inputs exit, and row values and lineage match
--   in both directions via FOR VERSION AS OF + EXCEPT.
-- Scope:
--   Native SQL over a private unpartitioned table, without concurrent writes.
--   The replacement/delete assertions apply to this deterministic rewrite;
--   OPTIMIZE may legitimately do nothing for other inputs.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG opt_rl_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_test_warehouse}/opt_rl_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "${iceberg_object_store_credential_mode}",
  "credential.object-store-metadata.name" = "${iceberg_object_store_metadata_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "${iceberg_object_store_credential_mode}",
  "credential.object-store-data.name" = "${iceberg_object_store_data_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.enable_path_style_access" = "${oss_path_style}"
);
CREATE DATABASE opt_rl_${uuid0}.ns_${uuid0};
CREATE TABLE opt_rl_${uuid0}.ns_${uuid0}.olineage (id INT, v INT)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
INSERT INTO opt_rl_${uuid0}.ns_${uuid0}.olineage VALUES (1, 10), (2, 20);
INSERT INTO opt_rl_${uuid0}.ns_${uuid0}.olineage VALUES (3, 30), (4, 40);
INSERT INTO opt_rl_${uuid0}.ns_${uuid0}.olineage VALUES (5, 50), (6, 60);
UPDATE opt_rl_${uuid0}.ns_${uuid0}.olineage SET v = 99 WHERE id = 2;
DELETE FROM opt_rl_${uuid0}.ns_${uuid0}.olineage WHERE id = 4;

-- query 2
-- @skip_result_check=true
-- Tag the pre-OPTIMIZE state so we can compare row values and lineage after
-- the rewrite via FOR VERSION AS OF.
ALTER TABLE opt_rl_${uuid0}.ns_${uuid0}.olineage CREATE TAG pre_opt;

-- query 3
-- BEFORE OPTIMIZE: 5 surviving rows, each with a unique `_row_id`.
-- @db=opt_rl_${uuid0}.ns_${uuid0}
SELECT count(*) AS n_rows, count(DISTINCT _row_id) AS n_unique_row_ids
  FROM olineage;

-- query 4
-- Independent oracle: id=2 was updated to 99 and id=4 was deleted.
-- @db=opt_rl_${uuid0}.ns_${uuid0}
SELECT id, v FROM olineage ORDER BY id;

-- query 5
-- Establish actual rewrite inputs, rather than inferring them from row counts.
-- All seed files are tiny and belong to one unpartitioned rewrite group.
-- @db=opt_rl_${uuid0}.ns_${uuid0}
SELECT
  sum(CASE WHEN content = 0 THEN 1 ELSE 0 END) > 1 AS multiple_data_files,
  sum(CASE WHEN content <> 0 THEN 1 ELSE 0 END) > 0 AS has_active_delete_files
  FROM olineage$files;

-- query 6
-- The saved tag is the current baseline immediately before OPTIMIZE.
-- @db=opt_rl_${uuid0}.ns_${uuid0}
SELECT count(*) AS main_matches_pre_opt
  FROM olineage$refs AS current_ref
  JOIN olineage$refs AS baseline_ref
    ON current_ref.snapshot_id = baseline_ref.snapshot_id
  WHERE current_ref.name = 'main' AND baseline_ref.name = 'pre_opt';

-- query 7
-- @skip_result_check=true
-- @wait_alter_optimize=olineage
-- @db=opt_rl_${uuid0}.ns_${uuid0}
ALTER TABLE olineage OPTIMIZE;

-- query 8
-- AFTER OPTIMIZE: same row count, same number of unique row-ids.
-- @db=opt_rl_${uuid0}.ns_${uuid0}
SELECT count(*) AS n_rows, count(DISTINCT _row_id) AS n_unique_row_ids
  FROM olineage;

-- query 9
-- Compare with the declared seed oracle again, not just with the saved tag.
-- @db=opt_rl_${uuid0}.ns_${uuid0}
SELECT id, v FROM olineage ORDER BY id;

-- query 10
-- No post-rewrite row value or lineage may be absent from the baseline.
-- @db=opt_rl_${uuid0}.ns_${uuid0}
SELECT count(*) AS rows_changed_by_optimize
  FROM (
    SELECT id, v, _row_id, _last_updated_sequence_number FROM olineage
    EXCEPT
    SELECT id, v, _row_id, _last_updated_sequence_number
      FROM olineage FOR VERSION AS OF 'pre_opt'
  ) diff;

-- query 11
-- The reverse direction also rejects losing a baseline row or lineage.
-- @db=opt_rl_${uuid0}.ns_${uuid0}
SELECT count(*) AS baseline_rows_missing_after_optimize
  FROM (
    SELECT id, v, _row_id, _last_updated_sequence_number
      FROM olineage FOR VERSION AS OF 'pre_opt'
    EXCEPT
    SELECT id, v, _row_id, _last_updated_sequence_number FROM olineage
  ) diff;

-- query 12
-- This private table has no replace snapshot before this deterministic rewrite.
-- @db=opt_rl_${uuid0}.ns_${uuid0}
SELECT count(*) AS n_replace_snapshots
  FROM olineage$snapshots
  WHERE operation = 'replace';

-- query 13
-- Resolve the new current snapshot through refs instead of timestamps. Its
-- parent must be the exact pre-OPTIMIZE snapshot, proving this publication.
-- @db=opt_rl_${uuid0}.ns_${uuid0}
SELECT count(*) AS current_replace_child_of_pre_opt
  FROM olineage$snapshots AS rewritten
  JOIN olineage$refs AS current_ref
    ON rewritten.snapshot_id = current_ref.snapshot_id
  JOIN olineage$refs AS baseline_ref
    ON rewritten.parent_id = baseline_ref.snapshot_id
  WHERE current_ref.name = 'main' AND baseline_ref.name = 'pre_opt'
    AND rewritten.operation = 'replace'
    AND rewritten.snapshot_id <> baseline_ref.snapshot_id;

-- query 14
-- All active delete inputs belonged to this table's complete rewrite group.
-- This does not require OPTIMIZE to clear deletes outside a selected rewrite.
-- @db=opt_rl_${uuid0}.ns_${uuid0}
SELECT count(*) AS active_delete_files_after_optimize
  FROM olineage$files
  WHERE content <> 0;

-- query 15
-- @cleanup=true
-- @skip_result_check=true
ALTER TABLE opt_rl_${uuid0}.ns_${uuid0}.olineage DROP TAG pre_opt;
DROP TABLE opt_rl_${uuid0}.ns_${uuid0}.olineage FORCE;
DROP DATABASE opt_rl_${uuid0}.ns_${uuid0};
DROP CATALOG opt_rl_${uuid0};
