-- @order_sensitive=true
-- @tags=aggregate,approx_top_k,min_n,avg,mann_whitney
-- Test Objective:
-- 1. Validate approx_top_k, min_n, average-on-expression, and mann_whitney_u_test semantics.
-- 2. Cover grouped and nullable inputs without relying on external schemas.
-- Test Flow:
-- 1. Create/reset a mixed scalar source table.
-- 2. Insert deterministic duplicates, NULLs, and boolean cohorts.
-- 3. Snapshot top-k, extrema arrays, aggregate expression averages, and Mann-Whitney outputs.
-- query 1
CREATE DATABASE IF NOT EXISTS sql_tests_d06;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_topk_extrema_misc;
CREATE TABLE sql_tests_d06.t_agg_topk_extrema_misc (
    id INT,
    grp INT,
    v INT,
    txt STRING,
    flag BOOLEAN,
    big_v BIGINT,
    w BIGINT
);

INSERT INTO sql_tests_d06.t_agg_topk_extrema_misc VALUES
    (1, 1, 10, 'delta', TRUE, 10000000, 3),
    (2, 1, 20, 'alpha', TRUE, 40000000, 5),
    (3, 1, 20, 'beta', FALSE, 40000000, 5),
    (4, 1, 30, 'gamma', FALSE, 40000000, 5),
    (5, 2, 10, 'zeta', TRUE, 10000000, 3),
    (6, 2, 10, 'eta', FALSE, 40000000, 5),
    (7, 2, 20, 'theta', FALSE, 40000000, 5),
    (8, 2, NULL, NULL, NULL, NULL, NULL),
    (9, 3, 40, 'iota', TRUE, 10000000, 3),
    (10, 3, 40, 'kappa', FALSE, 40000000, 5);
WITH w1 AS (
    SELECT approx_top_k(v, 3) AS x
    FROM sql_tests_d06.t_agg_topk_extrema_misc
)
SELECT array_sortby((x) -> x.item, x) AS top_items
FROM w1;

-- query 2
WITH w1 AS (
    SELECT
        grp,
        approx_top_k(v, 2) AS x
    FROM sql_tests_d06.t_agg_topk_extrema_misc
    GROUP BY grp
)
SELECT
    grp,
    array_sortby((x) -> x.item, x) AS top_items
FROM w1
ORDER BY grp;

-- query 3
SELECT
    min_n(v, 3) AS min_three_values,
    min_n(txt, 2) AS min_two_texts
FROM sql_tests_d06.t_agg_topk_extrema_misc
WHERE v IS NOT NULL
  AND txt IS NOT NULL;

-- query 4
SELECT
    grp,
    ROUND(AVG(big_v - 1.86659630566164 * (w - 3.062175673706)), 6) AS expr_avg
FROM sql_tests_d06.t_agg_topk_extrema_misc
WHERE big_v IS NOT NULL
GROUP BY grp
ORDER BY grp;

-- query 5
SELECT
    mann_whitney_u_test(v, flag, 'two-sided') AS two_sided,
    mann_whitney_u_test(v, flag, 'less') AS less_side,
    mann_whitney_u_test(v, flag, 'greater') AS greater_side
FROM sql_tests_d06.t_agg_topk_extrema_misc
WHERE v IS NOT NULL
  AND flag IS NOT NULL;
