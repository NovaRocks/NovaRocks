-- @sequential=true
-- @order_sensitive=true
-- @tags=write_path,iceberg,optimize,row_lineage
-- Test Point:
--   OPTIMIZE on a v3 row-lineage Iceberg table preserves `_row_id` and
--   `_last_updated_sequence_number` for every surviving row, even after
--   intervening UPDATE / DELETE that changes the live row set.
-- Method:
--   Build a v3 row-lineage table, mutate it with INSERT / UPDATE / DELETE,
--   capture (id, _row_id, _last_updated_sequence_number) before OPTIMIZE
--   into a snapshot tag, run OPTIMIZE, and assert the post-OPTIMIZE state
--   matches the pre-OPTIMIZE tag exactly via FOR VERSION AS OF + EXCEPT.
-- Scope:
--   Phase B (OPTIMIZE preserves row-lineage) end-to-end via SQL surface.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG opt_rl_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/opt_rl_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
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
-- Tag the pre-OPTIMIZE state so we can compare row-lineage triples after
-- the rewrite via FOR VERSION AS OF.
ALTER TABLE opt_rl_${uuid0}.ns_${uuid0}.olineage CREATE TAG pre_opt;

-- query 3
-- BEFORE OPTIMIZE: 5 surviving rows, each with a unique `_row_id`.
-- @db=opt_rl_${uuid0}.ns_${uuid0}
SELECT count(*) AS n_rows, count(DISTINCT _row_id) AS n_unique_row_ids
  FROM olineage;

-- query 4
-- @skip_result_check=true
-- @wait_alter_optimize=olineage
-- @db=opt_rl_${uuid0}.ns_${uuid0}
ALTER TABLE olineage OPTIMIZE;

-- query 5
-- AFTER OPTIMIZE: same row count, same number of unique row-ids.
-- @db=opt_rl_${uuid0}.ns_${uuid0}
SELECT count(*) AS n_rows, count(DISTINCT _row_id) AS n_unique_row_ids
  FROM olineage;

-- query 6
-- Cross-check: every (id, _row_id, _last_updated_sequence_number) triple in
-- the post-OPTIMIZE live set must also exist in the pre-OPTIMIZE tag. A
-- non-zero count here means OPTIMIZE rewrote a row identity that changed.
-- @db=opt_rl_${uuid0}.ns_${uuid0}
SELECT count(*) AS rows_changed_by_optimize
  FROM (
    SELECT id, _row_id, _last_updated_sequence_number FROM olineage
    EXCEPT
    SELECT id, _row_id, _last_updated_sequence_number
      FROM olineage FOR VERSION AS OF 'pre_opt'
  ) diff;

-- query 7
-- The latest snapshot is a Replace (the OPTIMIZE rewrite).
-- @db=opt_rl_${uuid0}.ns_${uuid0}
SELECT operation
  FROM olineage$snapshots
  ORDER BY committed_at DESC
  LIMIT 1;

-- query 8
-- @skip_result_check=true
ALTER TABLE opt_rl_${uuid0}.ns_${uuid0}.olineage DROP TAG pre_opt;
DROP TABLE opt_rl_${uuid0}.ns_${uuid0}.olineage FORCE;
DROP DATABASE opt_rl_${uuid0}.ns_${uuid0};
DROP CATALOG opt_rl_${uuid0};
