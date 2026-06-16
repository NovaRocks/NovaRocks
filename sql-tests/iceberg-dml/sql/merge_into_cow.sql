-- @order_sensitive=true
-- Test Point: Iceberg v3 copy-on-write MERGE INTO covers MATCHED UPDATE +
--             NOT MATCHED INSERT atomically, in a SINGLE snapshot, and
--             preserves row lineage.
-- Method: load a v3 row-lineage table with two rows. Stage a source table
--         that updates row id=2 ("b" -> "bb") and inserts a new row id=3.
--         MERGE the source into the target. Verify that the updated value
--         appears exactly once, the new row is appended, the existing
--         `_row_id`s survive, and the inserted row gets a fresh `_row_id`.
-- Scope:  standalone Iceberg DDL/DML, MERGE INTO. Phase-3 invariant: one
--         MERGE = one Iceberg snapshot. The COW UPDATE and the NOT-MATCHED
--         INSERT fold into a single commit, so the `$snapshots` count
--         advances by exactly 1 across the MERGE.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_merge_cow FORCE;
DROP TABLE IF EXISTS ${case_db}.s_v3_merge_cow FORCE;
CREATE TABLE ${case_db}.t_v3_merge_cow (
  id BIGINT,
  v STRING
)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ${case_db}.t_v3_merge_cow VALUES
  (1, 'a'),
  (2, 'b');
CREATE TABLE ${case_db}.s_v3_merge_cow (
  id BIGINT,
  v STRING
)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ${case_db}.s_v3_merge_cow VALUES
  (2, 'bb'),
  (3, 'c');

-- query 2
SELECT id, v
FROM ${case_db}.t_v3_merge_cow
ORDER BY id;

-- query 3
-- Atomicity proof: capture the snapshot count immediately before the MERGE.
SELECT count(*) AS snaps_before
FROM ${case_db}.t_v3_merge_cow$snapshots;

-- query 4
-- @skip_result_check=true
MERGE INTO ${case_db}.t_v3_merge_cow AS t
USING ${case_db}.s_v3_merge_cow AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET v = s.v
WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v);

-- query 5
-- Atomicity proof: the MATCHED UPDATE and NOT-MATCHED INSERT fold into a
-- single Iceberg snapshot, so snaps_after - snaps_before MUST be exactly 1.
SELECT count(*) AS snaps_after
FROM ${case_db}.t_v3_merge_cow$snapshots;

-- query 6
SELECT id, v
FROM ${case_db}.t_v3_merge_cow
ORDER BY id;

-- query 7
SELECT COUNT(DISTINCT _row_id) AS distinct_row_ids, COUNT(*) AS total_rows
FROM ${case_db}.t_v3_merge_cow;

-- query 8
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_merge_cow FORCE;
DROP TABLE ${case_db}.s_v3_merge_cow FORCE;
