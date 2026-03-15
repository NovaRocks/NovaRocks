-- @order_sensitive=true
-- @tags=join,right_anti
-- Test Objective:
-- 1. Validate RIGHT ANTI JOIN non-existence semantics on right-side rows.
-- 2. Prevent regressions where matched right rows remain in anti output.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert deterministic overlapping and non-overlapping keys.
-- 3. Execute RIGHT ANTI JOIN and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_join_right_anti_l;
DROP TABLE IF EXISTS ${case_db}.t_join_right_anti_r;
CREATE TABLE ${case_db}.t_join_right_anti_l (
  id INT,
  lv STRING
);
CREATE TABLE ${case_db}.t_join_right_anti_r (
  id INT,
  rv STRING
);
INSERT INTO ${case_db}.t_join_right_anti_l VALUES
  (2, 'L2'),
  (4, 'L4');
INSERT INTO ${case_db}.t_join_right_anti_r VALUES
  (1, 'R1'),
  (2, 'R2'),
  (3, 'R3'),
  (4, 'R4');
SELECT r.id, r.rv
FROM ${case_db}.t_join_right_anti_l l
RIGHT ANTI JOIN ${case_db}.t_join_right_anti_r r
  ON l.id = r.id
ORDER BY r.id;
