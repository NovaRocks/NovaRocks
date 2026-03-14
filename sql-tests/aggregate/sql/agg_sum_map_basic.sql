-- @order_sensitive=true
-- @tags=aggregate,sum_map,map
-- Test Objective:
-- 1. Validate sum_map on numeric and string-keyed maps.
-- 2. Cover grouped, nullable, and empty-map semantics in a self-contained case.
-- Test Flow:
-- 1. Create/reset source tables for non-null and nullable map inputs.
-- 2. Insert deterministic maps with overlapping keys, empty maps, and NULL maps.
-- 3. Snapshot global/grouped sum_map outputs and NULL-handling behavior.
-- query 1
CREATE DATABASE IF NOT EXISTS sql_tests_d06;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_sum_map_basic;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_sum_map_nullable;
CREATE TABLE sql_tests_d06.t_agg_sum_map_basic (
    g INT,
    m_int MAP<INT, INT>,
    m_str MAP<STRING, BIGINT>,
    m_double MAP<INT, DOUBLE>
);

CREATE TABLE sql_tests_d06.t_agg_sum_map_nullable (
    id INT,
    m_nullable MAP<INT, INT>
);

INSERT INTO sql_tests_d06.t_agg_sum_map_basic VALUES
    (1, map{1:10, 2:20, 3:30}, map{'a':100, 'b':200}, map{1:1.5, 2:2.5}),
    (1, map{1:5, 2:15, 4:40}, map{'a':50, 'c':150}, map{1:0.5, 3:3.5}),
    (2, map{2:25, 3:35, 5:50}, map{'b':250, 'c':350}, map{2:1.0, 3:2.0}),
    (2, map{}, map{}, map{});

INSERT INTO sql_tests_d06.t_agg_sum_map_nullable VALUES
    (1, map{1:10, 2:20}),
    (2, NULL),
    (3, map{1:5, 3:30}),
    (4, map{}),
    (5, map{1:-5, 4:40});
SELECT
    sum_map(m_int)[1] AS int_k1,
    sum_map(m_int)[2] AS int_k2,
    sum_map(m_int)[3] AS int_k3,
    sum_map(m_int)[4] AS int_k4,
    sum_map(m_int)[5] AS int_k5,
    sum_map(m_str)['a'] AS str_a,
    sum_map(m_str)['b'] AS str_b,
    sum_map(m_str)['c'] AS str_c,
    sum_map(m_double)[1] AS dbl_k1,
    sum_map(m_double)[2] AS dbl_k2,
    sum_map(m_double)[3] AS dbl_k3
FROM sql_tests_d06.t_agg_sum_map_basic;

-- query 2
SELECT
    g,
    sum_map(m_int)[1] AS int_k1,
    sum_map(m_int)[2] AS int_k2,
    sum_map(m_int)[3] AS int_k3,
    sum_map(m_int)[4] AS int_k4,
    sum_map(m_int)[5] AS int_k5
FROM sql_tests_d06.t_agg_sum_map_basic
GROUP BY g
ORDER BY g;

-- query 3
SELECT
    sum_map(m_nullable)[1] AS nullable_k1,
    sum_map(m_nullable)[2] AS nullable_k2,
    sum_map(m_nullable)[3] AS nullable_k3,
    sum_map(m_nullable)[4] AS nullable_k4
FROM sql_tests_d06.t_agg_sum_map_nullable;

-- query 4
SELECT sum_map(m_nullable) AS nullable_all_null_map
FROM (
    SELECT CAST(NULL AS MAP<INT, INT>) AS m_nullable
    UNION ALL
    SELECT CAST(NULL AS MAP<INT, INT>) AS m_nullable
) t;
