-- @sequential=true
-- @order_sensitive=true
-- @tags=write_path,iceberg,branch,row_lineage
-- Test Point:
--   Branch writes do not perturb main's row-lineage state, and a tag
--   captures a stable (id, _row_id) snapshot accessible via time travel.
-- Method:
--   Create a v3 row-lineage table, INSERT to main, tag, branch off, mutate
--   the branch (INSERT/UPDATE/DELETE), and verify that
--     1. main's row-lineage triples are unchanged after branch mutation;
--     2. the tag snapshot returns the original row-lineage triples;
--     3. the branch sees the mutated set.
-- Scope:
--   Phase C (row-lineage isolation across branch/tag) end-to-end.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG br_rl_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/br_rl_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE br_rl_${uuid0}.ns_${uuid0};
CREATE TABLE br_rl_${uuid0}.ns_${uuid0}.brlineage (id INT, v INT)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
INSERT INTO br_rl_${uuid0}.ns_${uuid0}.brlineage VALUES (1, 10), (2, 20), (3, 30);

-- query 2
-- @skip_result_check=true
-- Tag and branch off the initial state.
ALTER TABLE br_rl_${uuid0}.ns_${uuid0}.brlineage CREATE TAG snap0;
ALTER TABLE br_rl_${uuid0}.ns_${uuid0}.brlineage CREATE BRANCH feat;

-- query 3
-- @skip_result_check=true
-- Mutate the branch only.
INSERT INTO br_rl_${uuid0}.ns_${uuid0}.brlineage.branch_feat VALUES (4, 40);
UPDATE br_rl_${uuid0}.ns_${uuid0}.brlineage.branch_feat SET v = 99 WHERE id = 2;
DELETE FROM br_rl_${uuid0}.ns_${uuid0}.brlineage.branch_feat WHERE id = 3;

-- query 4
-- main is unchanged: still 3 rows with their original row-lineage triples.
-- @db=br_rl_${uuid0}.ns_${uuid0}
SELECT count(*) AS n_main_rows, count(DISTINCT _row_id) AS n_main_unique
  FROM brlineage;

-- query 5
-- main is identical to the snap0 tag (no diff in row-lineage triples).
-- @db=br_rl_${uuid0}.ns_${uuid0}
SELECT count(*) AS n_main_diverged
  FROM (
    SELECT id, _row_id, _last_updated_sequence_number FROM brlineage
    EXCEPT
    SELECT id, _row_id, _last_updated_sequence_number
      FROM brlineage FOR VERSION AS OF 'snap0'
  ) diff;

-- query 6
-- branch_feat sees: 1 (unchanged), 2 (UPDATEd, _row_id preserved), 4 (new).
-- @db=br_rl_${uuid0}.ns_${uuid0}
SELECT count(*) AS n_branch_rows, count(DISTINCT _row_id) AS n_branch_unique
  FROM brlineage FOR VERSION AS OF 'feat';

-- query 7
-- The UPDATEd row on branch retains its main-side _row_id (V3 row-lineage
-- preservation under UPDATE). Compare id=2's _row_id between snap0 and
-- branch_feat — they must be identical.
-- @db=br_rl_${uuid0}.ns_${uuid0}
SELECT
  (SELECT _row_id FROM brlineage FOR VERSION AS OF 'snap0' WHERE id = 2)
  =
  (SELECT _row_id FROM brlineage FOR VERSION AS OF 'feat' WHERE id = 2)
  AS row_id_preserved_under_branch_update;

-- query 8
-- @skip_result_check=true
ALTER TABLE br_rl_${uuid0}.ns_${uuid0}.brlineage DROP TAG snap0;
ALTER TABLE br_rl_${uuid0}.ns_${uuid0}.brlineage DROP BRANCH feat;
DROP TABLE br_rl_${uuid0}.ns_${uuid0}.brlineage FORCE;
DROP DATABASE br_rl_${uuid0}.ns_${uuid0};
DROP CATALOG br_rl_${uuid0};
