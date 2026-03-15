-- @order_sensitive=true
-- @tags=runtime_filter,left_semi,null_key
-- Test Objective:
-- 1. Validate LEFT SEMI JOIN semantics for probe-side NULL keys.
-- 2. Prevent regressions where runtime-filter pruning changes NULL-key semi-join behavior.
-- Test Flow:
-- 1. Create/reset left/right tables.
-- 2. Insert deterministic rows including NULL keys.
-- 3. Execute LEFT SEMI JOIN and assert deterministic output.
DROP TABLE IF EXISTS ${case_db}.t_rf_left_semi_null_probe_l;
DROP TABLE IF EXISTS ${case_db}.t_rf_left_semi_null_probe_r;
CREATE TABLE ${case_db}.t_rf_left_semi_null_probe_l (
    id INT,
    k INT
);
CREATE TABLE ${case_db}.t_rf_left_semi_null_probe_r (
    k INT
);

INSERT INTO ${case_db}.t_rf_left_semi_null_probe_l VALUES
    (1, 10),
    (2, NULL),
    (3, 30),
    (4, 40);

INSERT INTO ${case_db}.t_rf_left_semi_null_probe_r VALUES
    (10),
    (NULL),
    (50);

SELECT l.id, l.k
FROM ${case_db}.t_rf_left_semi_null_probe_l l
LEFT SEMI JOIN ${case_db}.t_rf_left_semi_null_probe_r r
  ON l.k = r.k
ORDER BY l.id;
