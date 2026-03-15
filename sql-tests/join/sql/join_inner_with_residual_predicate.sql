-- @order_sensitive=true
-- @tags=join,inner,residual
-- Test Objective:
-- 1. Validate INNER JOIN with additional residual predicate in ON clause.
-- 2. Prevent regressions where residual filters are ignored after key matching.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert deterministic rows with both passing and failing residual conditions.
-- 3. Execute INNER JOIN with residual predicate and assert output.
DROP TABLE IF EXISTS ${case_db}.t_join_inner_residual_l;
DROP TABLE IF EXISTS ${case_db}.t_join_inner_residual_r;
CREATE TABLE ${case_db}.t_join_inner_residual_l (
  id INT,
  lv INT
);
CREATE TABLE ${case_db}.t_join_inner_residual_r (
  id INT,
  rv INT
);
INSERT INTO ${case_db}.t_join_inner_residual_l VALUES
  (1, 5),
  (2, 20),
  (3, 7);
INSERT INTO ${case_db}.t_join_inner_residual_r VALUES
  (1, 10),
  (2, 15),
  (3, 7),
  (4, 100);
SELECT l.id, l.lv, r.rv
FROM ${case_db}.t_join_inner_residual_l l
INNER JOIN ${case_db}.t_join_inner_residual_r r
  ON l.id = r.id AND l.lv < r.rv
ORDER BY l.id;
