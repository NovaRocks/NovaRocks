-- @order_sensitive=true
-- @tags=join,left_outer,null
-- Test Objective:
-- 1. Validate NULL-key behavior under LEFT OUTER JOIN.
-- 2. Prevent regressions where NULL-key left rows incorrectly match right NULL keys.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert rows including NULL and non-NULL keys.
-- 3. Execute LEFT OUTER JOIN and assert null-fill behavior.
DROP TABLE IF EXISTS ${case_db}.t_join_null_key_left_outer_l;
DROP TABLE IF EXISTS ${case_db}.t_join_null_key_left_outer_r;
CREATE TABLE ${case_db}.t_join_null_key_left_outer_l (
  k INT,
  vl STRING
);
CREATE TABLE ${case_db}.t_join_null_key_left_outer_r (
  k INT,
  vr STRING
);
INSERT INTO ${case_db}.t_join_null_key_left_outer_l VALUES
  (NULL, 'LN'),
  (2, 'L2');
INSERT INTO ${case_db}.t_join_null_key_left_outer_r VALUES
  (NULL, 'RN'),
  (2, 'R2');
SELECT
  COALESCE(CAST(l.k AS STRING), 'NULL') AS lk,
  l.vl,
  r.vr
FROM ${case_db}.t_join_null_key_left_outer_l l
LEFT OUTER JOIN ${case_db}.t_join_null_key_left_outer_r r
  ON l.k = r.k
ORDER BY (l.k IS NULL) ASC, l.k ASC;
