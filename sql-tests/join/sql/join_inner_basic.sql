-- @order_sensitive=true
-- @tags=join,inner
-- Test Objective:
-- 1. Validate basic INNER JOIN equality semantics.
-- 2. Prevent regressions in matched-row output ordering.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert deterministic overlapping keys.
-- 3. Execute INNER JOIN and assert ordered matched rows.
DROP TABLE IF EXISTS ${case_db}.t_join_inner_basic_l;
DROP TABLE IF EXISTS ${case_db}.t_join_inner_basic_r;
CREATE TABLE ${case_db}.t_join_inner_basic_l (
  id INT,
  lv STRING
);
CREATE TABLE ${case_db}.t_join_inner_basic_r (
  id INT,
  rv STRING
);
INSERT INTO ${case_db}.t_join_inner_basic_l VALUES
  (1, 'L1'),
  (2, 'L2'),
  (3, 'L3');
INSERT INTO ${case_db}.t_join_inner_basic_r VALUES
  (2, 'R2'),
  (3, 'R3'),
  (4, 'R4');
SELECT l.id, l.lv, r.rv
FROM ${case_db}.t_join_inner_basic_l l
INNER JOIN ${case_db}.t_join_inner_basic_r r
  ON l.id = r.id
ORDER BY l.id;
