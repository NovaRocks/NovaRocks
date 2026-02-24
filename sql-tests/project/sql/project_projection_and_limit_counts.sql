-- @order_sensitive=true
-- @tags=project,projection,limit,self_contained
-- Test Objective:
-- 1. Validate projection and LIMIT row-count semantics on customer/date-like tables.
-- 2. Prevent regressions where this case depends on SSB dataset preloading.
-- Test Flow:
-- 1. Create/reset minimal customer and dates source tables.
-- 2. Insert deterministic row counts larger/smaller than LIMIT thresholds.
-- 3. Assert projected LIMIT counts through scalar subqueries.
CREATE DATABASE IF NOT EXISTS sql_tests_d04;
DROP TABLE IF EXISTS sql_tests_d04.t_project_projection_customer;
DROP TABLE IF EXISTS sql_tests_d04.t_project_projection_dates;
CREATE TABLE sql_tests_d04.t_project_projection_customer (
    c_custkey INT,
    c_name VARCHAR(32)
);
CREATE TABLE sql_tests_d04.t_project_projection_dates (
    d_datekey INT
);

INSERT INTO sql_tests_d04.t_project_projection_customer VALUES
    (1, 'c1'),
    (2, 'c2'),
    (3, 'c3'),
    (4, 'c4'),
    (5, 'c5'),
    (6, 'c6'),
    (7, 'c7'),
    (8, 'c8'),
    (9, 'c9'),
    (10, 'c10'),
    (11, 'c11'),
    (12, 'c12');

INSERT INTO sql_tests_d04.t_project_projection_dates VALUES
    (19920101),
    (19920102),
    (19920103),
    (19920104),
    (19920105),
    (19920106),
    (19920107),
    (19920108);

SELECT
    (
        SELECT COUNT(*)
        FROM (
            SELECT c_custkey, c_name
            FROM sql_tests_d04.t_project_projection_customer
            LIMIT 100
        ) x
    ) AS projected_limit_rows,
    (
        SELECT COUNT(*)
        FROM (
            SELECT C_CUSTKEY
            FROM sql_tests_d04.t_project_projection_customer
            LIMIT 10
        ) y
    ) AS uppercase_projection_rows,
    (
        SELECT COUNT(*)
        FROM (
            SELECT *
            FROM sql_tests_d04.t_project_projection_dates
            LIMIT 10
        ) z
    ) AS dates_limit_rows;
