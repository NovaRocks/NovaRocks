-- @order_sensitive=true
-- @tags=iceberg_dml,merge,delete
-- Test Point: Iceberg v3 MERGE INTO that combines `WHEN MATCHED THEN DELETE`
--             with `WHEN NOT MATCHED THEN INSERT` commits as a SINGLE snapshot
--             and preserves row lineage.
-- Method: load a v3 row-lineage merge-on-read table with two rows (ids 1,2).
--         MERGE in a source with id=2 (matched -> DELETE) and id=3 (not
--         matched -> INSERT). The matched-DELETE writes a deletion vector and
--         the NOT-MATCHED INSERT appends a data file; both fold into ONE
--         RowDeltaDvFromFiles commit. Verify row 1 survives, row 2 is removed,
--         row 3 is appended, every `_row_id` is unique (the inserted row gets a
--         fresh id), and the `$snapshots` count advances by exactly 1.
-- Scope:  standalone Iceberg DDL/DML, MERGE INTO WHEN MATCHED THEN DELETE +
--         WHEN NOT MATCHED THEN INSERT. Phase-3 invariant: one MERGE = one
--         Iceberg snapshot. This is the case that most directly exercises the
--         DV + INSERT fold into a single snapshot.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_merge_matched_delete_insert FORCE;
DROP TABLE IF EXISTS ${case_db}.s_v3_merge_matched_delete_insert FORCE;
CREATE TABLE ${case_db}.t_v3_merge_matched_delete_insert (
  id BIGINT,
  v STRING
)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true",
  "novarocks.update.mode" = "merge-on-read"
);
INSERT INTO ${case_db}.t_v3_merge_matched_delete_insert VALUES
  (1, 'a'),
  (2, 'b');
CREATE TABLE ${case_db}.s_v3_merge_matched_delete_insert (
  id BIGINT,
  v STRING
)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ${case_db}.s_v3_merge_matched_delete_insert VALUES
  (2, 'b'),
  (3, 'c');

-- query 2
SELECT id, v
FROM ${case_db}.t_v3_merge_matched_delete_insert
ORDER BY id;

-- query 3
-- Atomicity proof: capture the snapshot count immediately before the MERGE.
SELECT count(*) AS snaps_before
FROM ${case_db}.t_v3_merge_matched_delete_insert$snapshots;

-- query 4
-- @skip_result_check=true
MERGE INTO ${case_db}.t_v3_merge_matched_delete_insert AS t
USING ${case_db}.s_v3_merge_matched_delete_insert AS s
ON t.id = s.id
WHEN MATCHED THEN DELETE
WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);

-- query 5
-- Atomicity proof: the matched-DELETE (DV) and NOT-MATCHED INSERT fold into a
-- single RowDeltaDvFromFiles snapshot, so snaps_after - snaps_before MUST be 1.
SELECT count(*) AS snaps_after
FROM ${case_db}.t_v3_merge_matched_delete_insert$snapshots;

-- query 6
-- Row content: id=2 removed via its deletion vector, id=3 appended, id=1 kept.
SELECT id, v
FROM ${case_db}.t_v3_merge_matched_delete_insert
ORDER BY id;

-- query 7
-- Fresh-row-id gate: every surviving/added row carries a unique `_row_id`.
SELECT COUNT(DISTINCT _row_id) AS distinct_row_ids, COUNT(*) AS total_rows
FROM ${case_db}.t_v3_merge_matched_delete_insert;

-- query 8
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_merge_matched_delete_insert FORCE;
DROP TABLE ${case_db}.s_v3_merge_matched_delete_insert FORCE;
