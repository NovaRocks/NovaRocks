-- @order_sensitive=true
-- @tags=runtime_filter,anti_join,not_exists
-- Test Objective:
-- 1. Validate NOT EXISTS anti-join semantics.
-- 2. Prevent regressions where anti-join rows are incorrectly filtered out.
-- Test Flow:
-- 1. Create/reset source tables.
-- 2. Insert deterministic overlapping keys.
-- 3. Filter left rows via NOT EXISTS and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_rf_anti_join_not_exists_l;
DROP TABLE IF EXISTS ${case_db}.t_rf_anti_join_not_exists_r;
CREATE TABLE ${case_db}.t_rf_anti_join_not_exists_l (
    id INT,
    k INT
);
CREATE TABLE ${case_db}.t_rf_anti_join_not_exists_r (
    k INT
);

INSERT INTO ${case_db}.t_rf_anti_join_not_exists_l VALUES
    (1, 10),
    (2, 20),
    (3, 30),
    (4, NULL);

INSERT INTO ${case_db}.t_rf_anti_join_not_exists_r VALUES
    (20),
    (40);

SELECT l.id, l.k
FROM ${case_db}.t_rf_anti_join_not_exists_l l
WHERE NOT EXISTS (
    SELECT 1
    FROM ${case_db}.t_rf_anti_join_not_exists_r r
    WHERE r.k = l.k
)
ORDER BY l.id;
