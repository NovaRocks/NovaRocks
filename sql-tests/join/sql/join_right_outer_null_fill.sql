-- @order_sensitive=true
-- @tags=join,right_outer
-- Test Objective:
-- 1. Validate RIGHT OUTER JOIN null-fill semantics for unmatched left rows.
-- 2. Prevent regressions where unmatched right rows are dropped.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert deterministic rows with one unmatched right key.
-- 3. Execute RIGHT OUTER JOIN and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_join_right_outer_l;
DROP TABLE IF EXISTS ${case_db}.t_join_right_outer_r;
CREATE TABLE ${case_db}.t_join_right_outer_l (
  id INT,
  lv STRING
);
CREATE TABLE ${case_db}.t_join_right_outer_r (
  id INT,
  rv STRING
);
INSERT INTO ${case_db}.t_join_right_outer_l VALUES
  (2, 'L2'),
  (3, 'L3');
INSERT INTO ${case_db}.t_join_right_outer_r VALUES
  (2, 'R2'),
  (3, 'R3'),
  (4, 'R4');
SELECT l.id AS lid, l.lv, r.id AS rid, r.rv
FROM ${case_db}.t_join_right_outer_l l
RIGHT OUTER JOIN ${case_db}.t_join_right_outer_r r
  ON l.id = r.id
ORDER BY rid;
