-- @order_sensitive=true
-- @tags=join,right_semi,residual
-- Test Objective:
-- 1. Validate RIGHT SEMI JOIN with residual predicate over non-key columns.
-- 2. Prevent regressions where build-side match marking ignores residual filtering.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert deterministic rows with both passing and failing residual conditions.
-- 3. Execute RIGHT SEMI JOIN with residual predicate and assert right-side output rows.
DROP TABLE IF EXISTS ${case_db}.t_join_right_semi_residual_l;
DROP TABLE IF EXISTS ${case_db}.t_join_right_semi_residual_r;
CREATE TABLE ${case_db}.t_join_right_semi_residual_l (
  id INT,
  score INT
);
CREATE TABLE ${case_db}.t_join_right_semi_residual_r (
  id INT,
  threshold INT,
  tag STRING
);
INSERT INTO ${case_db}.t_join_right_semi_residual_l VALUES
  (1, 10),
  (1, 1),
  (2, 5),
  (3, 7),
  (NULL, 100);
INSERT INTO ${case_db}.t_join_right_semi_residual_r VALUES
  (1, 5, 'r1_pass'),
  (1, 20, 'r1_fail'),
  (2, 3, 'r2_pass'),
  (3, 9, 'r3_fail'),
  (4, 1, 'r4_nomatch'),
  (NULL, 1, 'rnull');
SELECT r.id, r.threshold, r.tag
FROM ${case_db}.t_join_right_semi_residual_l l
RIGHT SEMI JOIN ${case_db}.t_join_right_semi_residual_r r
  ON l.id = r.id AND l.score > r.threshold
ORDER BY r.id, r.threshold;
