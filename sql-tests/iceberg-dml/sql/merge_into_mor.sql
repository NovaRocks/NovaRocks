-- @order_sensitive=true
-- Test Point: Iceberg v3 merge-on-read MERGE INTO covers MATCHED UPDATE +
--             NOT MATCHED INSERT atomically, in a SINGLE snapshot, and
--             preserves row lineage.
-- Method: load a v3 row-lineage table whose update mode is merge-on-read
--         with two rows. MERGE in a source that updates id=2 ("b" -> "bb")
--         and inserts id=3. Verify the updated value is visible exactly
--         once (DV deletes the old row, the rewritten row appears via the
--         added data file), the new row is appended, and `_row_id` values
--         remain unique.
-- Scope:  standalone Iceberg DDL/DML, MERGE INTO. Phase-3 invariant: one
--         MERGE = one Iceberg snapshot. The MOR UPDATE (DV) and the
--         NOT-MATCHED INSERT fold into a single RowDelta commit, so the
--         `$snapshots` count advances by exactly 1 across the MERGE.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_merge_mor FORCE;
DROP TABLE IF EXISTS ${case_db}.s_v3_merge_mor FORCE;
CREATE TABLE ${case_db}.t_v3_merge_mor (
  id BIGINT,
  v STRING
)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true",
  "novarocks.update.mode" = "merge-on-read"
);
INSERT INTO ${case_db}.t_v3_merge_mor VALUES
  (1, 'a'),
  (2, 'b');
CREATE TABLE ${case_db}.s_v3_merge_mor (
  id BIGINT,
  v STRING
)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ${case_db}.s_v3_merge_mor VALUES
  (2, 'bb'),
  (3, 'c');

-- query 2
SELECT id, v
FROM ${case_db}.t_v3_merge_mor
ORDER BY id;

-- query 3
-- Atomicity proof: capture the snapshot count immediately before the MERGE.
SELECT count(*) AS snaps_before
FROM ${case_db}.t_v3_merge_mor$snapshots;

-- query 4
-- @skip_result_check=true
MERGE INTO ${case_db}.t_v3_merge_mor AS t
USING ${case_db}.s_v3_merge_mor AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET v = s.v
WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);

-- query 5
-- Atomicity proof: the MATCHED UPDATE (DV) and NOT-MATCHED INSERT fold into a
-- single Iceberg snapshot, so snaps_after - snaps_before MUST be exactly 1.
SELECT count(*) AS snaps_after
FROM ${case_db}.t_v3_merge_mor$snapshots;

-- query 6
SELECT id, v
FROM ${case_db}.t_v3_merge_mor
ORDER BY id;

-- query 7
SELECT COUNT(DISTINCT _row_id) AS distinct_row_ids, COUNT(*) AS total_rows
FROM ${case_db}.t_v3_merge_mor;

-- query 8
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_merge_mor FORCE;
DROP TABLE ${case_db}.s_v3_merge_mor FORCE;
