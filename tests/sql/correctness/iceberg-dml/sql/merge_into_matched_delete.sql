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
-- Test Point: Iceberg v3 MERGE INTO with a single `WHEN MATCHED THEN DELETE`
--             branch removes exactly the matched rows and preserves row lineage.
-- Method: load a v3 row-lineage table with three rows (ids 1,2,3). MERGE in a
--         single-row source (id=2) with only a matched-DELETE clause. Verify
--         rows 1 and 3 remain (row 2 is removed via its deletion vector) and
--         that `_row_id` values stay unique.
-- Scope:  standalone Iceberg DDL/DML, MERGE INTO WHEN MATCHED THEN DELETE.
-- Phase 3 M2: matched-DELETE writes its deletion vector on the BE
--         (DeletionVectors sink), committed via RowDeltaDvFromFiles. FE commits
--         metadata only; the coordinator no longer materializes position groups.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_merge_matched_delete ;
DROP TABLE IF EXISTS ${case_db}.s_v3_merge_matched_delete ;
CREATE TABLE ${case_db}.t_v3_merge_matched_delete (
  id BIGINT,
  v VARCHAR
) WITH (format_version = 3, extra_properties = MAP(ARRAY['novarocks.update.mode'], ARRAY['merge-on-read']));
INSERT INTO ${case_db}.t_v3_merge_matched_delete VALUES
  ROW(1, 'a'),
  ROW(2, 'b'),
  ROW(3, 'c');
CREATE TABLE ${case_db}.s_v3_merge_matched_delete (
  id BIGINT,
  v VARCHAR
) WITH (format_version = 3);
INSERT INTO ${case_db}.s_v3_merge_matched_delete VALUES
  ROW(2, 'b');

-- query 2
SELECT id, v
FROM ${case_db}.t_v3_merge_matched_delete
ORDER BY id;

-- query 3
-- Atomicity proof: capture the snapshot count immediately before the MERGE.
SELECT count(*) AS snaps_before
FROM ${case_db}."t_v3_merge_matched_delete$snapshots";

-- query 4
-- @skip_result_check=true
MERGE INTO ${case_db}.t_v3_merge_matched_delete AS t
USING ${case_db}.s_v3_merge_matched_delete AS s
ON t.id = s.id
WHEN MATCHED THEN DELETE;

-- query 5
-- Atomicity proof: a single-branch matched-DELETE MERGE commits one
-- RowDeltaDvFromFiles snapshot, so snaps_after - snaps_before MUST be 1.
SELECT count(*) AS snaps_after
FROM ${case_db}."t_v3_merge_matched_delete$snapshots";

-- query 6
SELECT id, v
FROM ${case_db}.t_v3_merge_matched_delete
ORDER BY id;

-- query 7
SELECT COUNT(DISTINCT _row_id) = COUNT(*) AS row_ids_unique
FROM ${case_db}.t_v3_merge_matched_delete;

-- query 8
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_merge_matched_delete ;
DROP TABLE ${case_db}.s_v3_merge_matched_delete ;
