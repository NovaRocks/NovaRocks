-- @sequential=true
-- @order_sensitive=true
-- @tags=write_path,iceberg,row_lineage,uniqueness
-- Test Point:
--   Iceberg V3 row-lineage uniqueness invariants under realistic mutation
--   sequences, including multiple OPTIMIZE rewrites:
--     I1 (intra-snapshot): count(*) = count(DISTINCT _row_id)
--     I2 (cross-snapshot): for each logical row identity (id), the set of
--                          _row_id values across history has size <= 1.
-- Method:
--   Mutate the table through INSERT / DELETE / UPDATE / OPTIMIZE / INSERT /
--   OPTIMIZE while tagging each "interesting" snapshot, then assert both
--   invariants by joining over the tags.
-- Scope:
--   Phase C cross-snapshot _row_id uniqueness — the invariant the IVM
--   row-lineage pairing relies on.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG uniq_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/uniq_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE uniq_${uuid0}.ns_${uuid0};
CREATE TABLE uniq_${uuid0}.ns_${uuid0}.uniq (id INT, v INT)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 2
-- @skip_result_check=true
INSERT INTO uniq_${uuid0}.ns_${uuid0}.uniq VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
ALTER TABLE uniq_${uuid0}.ns_${uuid0}.uniq CREATE TAG t1;

-- query 3
-- @skip_result_check=true
DELETE FROM uniq_${uuid0}.ns_${uuid0}.uniq WHERE id = 4;
ALTER TABLE uniq_${uuid0}.ns_${uuid0}.uniq CREATE TAG t2;

-- query 4
-- @skip_result_check=true
UPDATE uniq_${uuid0}.ns_${uuid0}.uniq SET v = v + 1 WHERE id = 1;
ALTER TABLE uniq_${uuid0}.ns_${uuid0}.uniq CREATE TAG t3;

-- query 5
-- @skip_result_check=true
-- @wait_alter_optimize=uniq
-- @db=uniq_${uuid0}.ns_${uuid0}
ALTER TABLE uniq OPTIMIZE;

-- query 6
-- @skip_result_check=true
ALTER TABLE uniq_${uuid0}.ns_${uuid0}.uniq CREATE TAG t4;
INSERT INTO uniq_${uuid0}.ns_${uuid0}.uniq VALUES (10, 100), (20, 200);
ALTER TABLE uniq_${uuid0}.ns_${uuid0}.uniq CREATE TAG t5;

-- query 7
-- @skip_result_check=true
-- @wait_alter_optimize=uniq
-- @db=uniq_${uuid0}.ns_${uuid0}
ALTER TABLE uniq OPTIMIZE;

-- query 8
-- @skip_result_check=true
ALTER TABLE uniq_${uuid0}.ns_${uuid0}.uniq CREATE TAG t6;

-- query 9
-- I1 — current snapshot intra-snapshot uniqueness.
-- @db=uniq_${uuid0}.ns_${uuid0}
SELECT count(*) - count(DISTINCT _row_id) AS row_id_collisions FROM uniq;

-- query 10
-- I1 — same invariant on each historical tag.
-- @db=uniq_${uuid0}.ns_${uuid0}
SELECT
  (SELECT count(*) - count(DISTINCT _row_id) FROM uniq FOR VERSION AS OF 't1') AS t1_collisions,
  (SELECT count(*) - count(DISTINCT _row_id) FROM uniq FOR VERSION AS OF 't2') AS t2_collisions,
  (SELECT count(*) - count(DISTINCT _row_id) FROM uniq FOR VERSION AS OF 't3') AS t3_collisions,
  (SELECT count(*) - count(DISTINCT _row_id) FROM uniq FOR VERSION AS OF 't4') AS t4_collisions,
  (SELECT count(*) - count(DISTINCT _row_id) FROM uniq FOR VERSION AS OF 't5') AS t5_collisions,
  (SELECT count(*) - count(DISTINCT _row_id) FROM uniq FOR VERSION AS OF 't6') AS t6_collisions;

-- query 11
-- I2 — cross-snapshot row-id uniqueness per logical id.
-- For every id observed across all tags, the set of _row_id values must
-- have size 1 (id keeps the same row-lineage identity through its life).
-- @db=uniq_${uuid0}.ns_${uuid0}
SELECT max(distinct_row_ids) AS max_row_ids_per_id
  FROM (
    SELECT id, count(DISTINCT _row_id) AS distinct_row_ids
      FROM (
        SELECT id, _row_id FROM uniq FOR VERSION AS OF 't1'
        UNION ALL
        SELECT id, _row_id FROM uniq FOR VERSION AS OF 't2'
        UNION ALL
        SELECT id, _row_id FROM uniq FOR VERSION AS OF 't3'
        UNION ALL
        SELECT id, _row_id FROM uniq FOR VERSION AS OF 't4'
        UNION ALL
        SELECT id, _row_id FROM uniq FOR VERSION AS OF 't5'
        UNION ALL
        SELECT id, _row_id FROM uniq FOR VERSION AS OF 't6'
      ) hist
      GROUP BY id
  ) per_id;

-- query 12
-- @skip_result_check=true
ALTER TABLE uniq_${uuid0}.ns_${uuid0}.uniq DROP TAG t1;
ALTER TABLE uniq_${uuid0}.ns_${uuid0}.uniq DROP TAG t2;
ALTER TABLE uniq_${uuid0}.ns_${uuid0}.uniq DROP TAG t3;
ALTER TABLE uniq_${uuid0}.ns_${uuid0}.uniq DROP TAG t4;
ALTER TABLE uniq_${uuid0}.ns_${uuid0}.uniq DROP TAG t5;
ALTER TABLE uniq_${uuid0}.ns_${uuid0}.uniq DROP TAG t6;
DROP TABLE uniq_${uuid0}.ns_${uuid0}.uniq FORCE;
DROP DATABASE uniq_${uuid0}.ns_${uuid0};
DROP CATALOG uniq_${uuid0};
