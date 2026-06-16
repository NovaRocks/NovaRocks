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
DROP TABLE IF EXISTS ${case_db}.t_v3_merge_matched_delete FORCE;
DROP TABLE IF EXISTS ${case_db}.s_v3_merge_matched_delete FORCE;
CREATE TABLE ${case_db}.t_v3_merge_matched_delete (
  id BIGINT,
  v STRING
)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true",
  "novarocks.update.mode" = "merge-on-read"
);
INSERT INTO ${case_db}.t_v3_merge_matched_delete VALUES
  (1, 'a'),
  (2, 'b'),
  (3, 'c');
CREATE TABLE ${case_db}.s_v3_merge_matched_delete (
  id BIGINT,
  v STRING
)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ${case_db}.s_v3_merge_matched_delete VALUES
  (2, 'b');

-- query 2
SELECT id, v
FROM ${case_db}.t_v3_merge_matched_delete
ORDER BY id;

-- query 3
-- @skip_result_check=true
MERGE INTO ${case_db}.t_v3_merge_matched_delete AS t
USING ${case_db}.s_v3_merge_matched_delete AS s
ON t.id = s.id
WHEN MATCHED THEN DELETE;

-- query 4
SELECT id, v
FROM ${case_db}.t_v3_merge_matched_delete
ORDER BY id;

-- query 5
SELECT COUNT(DISTINCT _row_id) = COUNT(*) AS row_ids_unique
FROM ${case_db}.t_v3_merge_matched_delete;

-- query 6
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_merge_matched_delete FORCE;
DROP TABLE ${case_db}.s_v3_merge_matched_delete FORCE;
