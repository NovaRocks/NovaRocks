-- @order_sensitive=true
-- @tags=join,inner,null
-- Test Objective:
-- 1. Validate that NULL join keys do not match under equality INNER JOIN.
-- 2. Prevent regressions where NULL=NULL is treated as a match in join keys.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert rows including NULL and non-NULL keys.
-- 3. Execute INNER JOIN and assert only non-NULL matched rows remain.
DROP TABLE IF EXISTS ${case_db}.t_join_null_key_inner_l;
DROP TABLE IF EXISTS ${case_db}.t_join_null_key_inner_r;
CREATE TABLE ${case_db}.t_join_null_key_inner_l (
  k INT,
  vl STRING
);
CREATE TABLE ${case_db}.t_join_null_key_inner_r (
  k INT,
  vr STRING
);
INSERT INTO ${case_db}.t_join_null_key_inner_l VALUES
  (NULL, 'LN'),
  (2, 'L2');
INSERT INTO ${case_db}.t_join_null_key_inner_r VALUES
  (NULL, 'RN'),
  (2, 'R2');
SELECT l.k, l.vl, r.vr
FROM ${case_db}.t_join_null_key_inner_l l
INNER JOIN ${case_db}.t_join_null_key_inner_r r
  ON l.k = r.k
ORDER BY l.k;
