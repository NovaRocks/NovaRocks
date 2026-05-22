-- @order_sensitive=true
-- @tags=iceberg_dml,resolve
-- Test Objective:
-- 1. Validate `INSERT INTO db.t1 SELECT FROM db.t2` resolves both 2-part refs
--    under the active catalog (no USE / no current database).
-- 2. Validate standalone `SELECT FROM db.t` resolves the 2-part ref too.
-- Regression guard for: 2-part `db.table` refs failing with "unknown database"
-- in the SELECT-FROM path before the resolver fix.
DROP TABLE IF EXISTS ${case_db}.t_two_part_src;
DROP TABLE IF EXISTS ${case_db}.t_two_part_sink;
CREATE TABLE ${case_db}.t_two_part_src (
  id BIGINT,
  v STRING
);
CREATE TABLE ${case_db}.t_two_part_sink (
  id BIGINT,
  v STRING
);
INSERT INTO ${case_db}.t_two_part_src VALUES
  (1, 'alpha'),
  (2, 'beta'),
  (3, 'gamma');

-- Path 1: INSERT INTO db.t1 SELECT FROM db.t2 (both sides 2-part).
INSERT INTO ${case_db}.t_two_part_sink
SELECT id, v FROM ${case_db}.t_two_part_src;

-- Path 2: standalone SELECT FROM db.t.
SELECT id, v FROM ${case_db}.t_two_part_sink ORDER BY id;
