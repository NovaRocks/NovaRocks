-- @order_sensitive=true
-- @tags=join,inner,duplicate
-- Test Objective:
-- 1. Validate duplicate-key multiplicity in INNER JOIN (cartesian per matched key).
-- 2. Prevent regressions that incorrectly deduplicate join output rows.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert duplicate rows for the same join key on both sides.
-- 3. Execute INNER JOIN and assert full multiplicity output.
CREATE DATABASE IF NOT EXISTS sql_tests_d05;
DROP TABLE IF EXISTS sql_tests_d05.t_join_duplicate_key_l;
DROP TABLE IF EXISTS sql_tests_d05.t_join_duplicate_key_r;
CREATE TABLE sql_tests_d05.t_join_duplicate_key_l (
  id INT,
  ltag STRING
);
CREATE TABLE sql_tests_d05.t_join_duplicate_key_r (
  id INT,
  rtag STRING
);
INSERT INTO sql_tests_d05.t_join_duplicate_key_l VALUES
  (1, 'L1a'),
  (1, 'L1b');
INSERT INTO sql_tests_d05.t_join_duplicate_key_r VALUES
  (1, 'R1a'),
  (1, 'R1b');
SELECT l.ltag, r.rtag
FROM sql_tests_d05.t_join_duplicate_key_l l
INNER JOIN sql_tests_d05.t_join_duplicate_key_r r
  ON l.id = r.id
ORDER BY l.ltag, r.rtag;
