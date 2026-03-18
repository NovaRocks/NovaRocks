-- @tags=join,using
-- Test Objective:
-- Validate JOIN USING clause semantics across all join types (INNER, LEFT, RIGHT,
-- FULL OUTER, LEFT SEMI, LEFT ANTI, RIGHT SEMI, RIGHT ANTI, CROSS).
-- Covers: single-key USING, multi-key USING, NULL handling in USING columns,
-- column aliasing, subqueries with USING, and aggregation over USING joins.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.left_table;

-- query 2
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.right_table;

-- query 3
-- @skip_result_check=true
CREATE TABLE ${case_db}.left_table (
    name VARCHAR(20),
    id1 INT,
    age INT,
    city VARCHAR(20),
    id2 INT,
    salary DECIMAL(10,2),
    status VARCHAR(10)
) DUPLICATE KEY(name) DISTRIBUTED BY HASH(name) BUCKETS 1 PROPERTIES ("replication_num" = "1");

-- query 4
-- @skip_result_check=true
CREATE TABLE ${case_db}.right_table (
    dept VARCHAR(20),
    id1 INT,
    bonus DECIMAL(8,2),
    id2 INT
) DUPLICATE KEY(dept) DISTRIBUTED BY HASH(dept) BUCKETS 1 PROPERTIES ("replication_num" = "1");

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.left_table VALUES
    ('Alice', 1, 25, 'New York', 100, 5000.00, 'Active'),
    ('Bob', 2, 30, 'Boston', 200, 6000.00, 'Active'),
    ('Charlie', 3, 35, 'Chicago', 300, 7000.00, 'Inactive'),
    ('David', 4, 28, 'Denver', NULL, 5500.00, 'Active'),
    ('Eve', 5, 32, 'Seattle', 500, 6500.00, 'Active'),
    ('Frank', 6, 40, NULL, 600, 8000.00, 'Inactive');

-- query 6
-- @skip_result_check=true
INSERT INTO ${case_db}.right_table VALUES
    ('Engineering', 1, 1000.00, 100),
    ('Marketing', 2, 800.00, 200),
    ('Sales', 7, 1200.00, 700),
    ('HR', 8, 900.00, NULL),
    ('Finance', 9, NULL, 900);

-- query 7
-- INNER JOIN with multi-key USING
-- @order_sensitive=true
SELECT * FROM ${case_db}.left_table JOIN ${case_db}.right_table USING(id1, id2) ORDER BY id1;

-- query 8
-- Multi-key USING: select only USING columns
-- @order_sensitive=true
SELECT id1, id2 FROM ${case_db}.left_table JOIN ${case_db}.right_table USING(id1, id2) ORDER BY id1;

-- query 9
-- LEFT JOIN with multi-key USING (NULL fill)
-- @order_sensitive=true
SELECT * FROM ${case_db}.left_table LEFT JOIN ${case_db}.right_table USING(id1, id2) ORDER BY id1;

-- query 10
-- LEFT JOIN: select only USING columns
-- @order_sensitive=true
SELECT id1, id2 FROM ${case_db}.left_table LEFT JOIN ${case_db}.right_table USING(id1, id2) ORDER BY id1;

-- query 11
-- RIGHT JOIN with multi-key USING
-- @order_sensitive=true
SELECT * FROM ${case_db}.left_table RIGHT JOIN ${case_db}.right_table USING(id1, id2) ORDER BY id1;

-- query 12
-- RIGHT JOIN: select only USING columns
-- @order_sensitive=true
SELECT id1, id2 FROM ${case_db}.left_table RIGHT JOIN ${case_db}.right_table USING(id1, id2) ORDER BY id1;

-- query 13
-- FULL OUTER JOIN with multi-key USING
-- @order_sensitive=true
SELECT * FROM ${case_db}.left_table FULL OUTER JOIN ${case_db}.right_table USING(id1, id2) ORDER BY right_table.dept, right_table.bonus;

-- query 14
-- INNER JOIN with single-key USING
-- @order_sensitive=true
SELECT * FROM ${case_db}.left_table JOIN ${case_db}.right_table USING(id1) ORDER BY id1;

-- query 15
-- LEFT JOIN USING + WHERE NULL filtering
SELECT * FROM ${case_db}.left_table LEFT JOIN ${case_db}.right_table USING(id1, id2) WHERE id1 IS NULL OR id2 IS NULL;

-- query 16
-- RIGHT SEMI JOIN with USING
-- @order_sensitive=true
SELECT * FROM ${case_db}.left_table RIGHT SEMI JOIN ${case_db}.right_table USING(id1, id2) ORDER BY id1;

-- query 17
-- RIGHT ANTI JOIN with USING
-- @order_sensitive=true
SELECT * FROM ${case_db}.left_table RIGHT ANTI JOIN ${case_db}.right_table USING(id1, id2) ORDER BY id1;

-- query 18
-- CROSS JOIN (no USING, baseline)
-- @order_sensitive=true
SELECT * FROM ${case_db}.left_table CROSS JOIN ${case_db}.right_table ORDER BY left_table.id1, right_table.id1 LIMIT 5;

-- query 19
-- LEFT SEMI JOIN with multi-key USING
-- @order_sensitive=true
SELECT * FROM ${case_db}.left_table LEFT SEMI JOIN ${case_db}.right_table USING(id1, id2) ORDER BY id1;

-- query 20
-- LEFT ANTI JOIN with multi-key USING
-- @order_sensitive=true
SELECT * FROM ${case_db}.left_table LEFT ANTI JOIN ${case_db}.right_table USING(id1, id2) ORDER BY id1;

-- query 21
-- Single-key USING: LEFT JOIN
-- @order_sensitive=true
SELECT * FROM ${case_db}.left_table LEFT JOIN ${case_db}.right_table USING(id1) ORDER BY id1;

-- query 22
-- Single-key USING: RIGHT JOIN
-- @order_sensitive=true
SELECT * FROM ${case_db}.left_table RIGHT JOIN ${case_db}.right_table USING(id1) ORDER BY id1;

-- query 23
-- Single-key USING: FULL OUTER JOIN
-- @order_sensitive=true
SELECT * FROM ${case_db}.left_table FULL OUTER JOIN ${case_db}.right_table USING(id1) ORDER BY right_table.dept, right_table.bonus;

-- query 24
-- Single-key USING: LEFT SEMI JOIN
-- @order_sensitive=true
SELECT * FROM ${case_db}.left_table LEFT SEMI JOIN ${case_db}.right_table USING(id1) ORDER BY id1;

-- query 25
-- Single-key USING: LEFT ANTI JOIN
-- @order_sensitive=true
SELECT * FROM ${case_db}.left_table LEFT ANTI JOIN ${case_db}.right_table USING(id1) ORDER BY id1;

-- query 26
-- Single-key USING: RIGHT SEMI JOIN
-- @order_sensitive=true
SELECT * FROM ${case_db}.left_table RIGHT SEMI JOIN ${case_db}.right_table USING(id1) ORDER BY id1;

-- query 27
-- Single-key USING: RIGHT ANTI JOIN
-- @order_sensitive=true
SELECT * FROM ${case_db}.left_table RIGHT ANTI JOIN ${case_db}.right_table USING(id1) ORDER BY id1;

-- query 28
-- USING column aliasing
-- @order_sensitive=true
SELECT id1 AS join_key FROM ${case_db}.left_table JOIN ${case_db}.right_table USING(id1) ORDER BY join_key;

-- query 29
-- USING with table alias: left table column ref
-- @order_sensitive=true
SELECT l.id1 l_id1 FROM ${case_db}.left_table l JOIN ${case_db}.right_table r USING(id1) ORDER BY l_id1;

-- query 30
-- RIGHT JOIN: left-side column becomes nullable via USING
-- @order_sensitive=true
SELECT l.id2 l_id1 FROM ${case_db}.left_table l right JOIN ${case_db}.right_table r USING(id1) ORDER BY l_id1;

-- query 31
-- RIGHT JOIN: right-side column via USING
-- @order_sensitive=true
SELECT r.id2 l_id1 FROM ${case_db}.left_table l right JOIN ${case_db}.right_table r USING(id1) ORDER BY l_id1;

-- query 32
-- Multi-key USING with column aliases
-- @order_sensitive=true
SELECT id1 AS key1, id2 AS key2 FROM ${case_db}.left_table JOIN ${case_db}.right_table USING(id1, id2) ORDER BY key1;

-- query 33
-- RIGHT JOIN: USING column prefers non-null side
-- @order_sensitive=true
SELECT id1 AS right_preferred_key FROM ${case_db}.left_table RIGHT JOIN ${case_db}.right_table USING(id1) ORDER BY right_preferred_key;

-- query 34
-- LEFT JOIN USING with column aliases and expressions
-- @order_sensitive=true
SELECT
    id1 AS main_id,
    name AS emp_name,
    dept AS department,
    salary AS emp_salary,
    bonus AS emp_bonus
FROM ${case_db}.left_table LEFT JOIN ${case_db}.right_table USING(id1, id2)
ORDER BY main_id;

-- query 35
-- LEFT JOIN USING with WHERE filter
-- @order_sensitive=true
SELECT id1 AS pk, name, dept
FROM ${case_db}.left_table LEFT JOIN ${case_db}.right_table USING(id1)
WHERE id1 > 2
ORDER BY pk;

-- query 36
-- NOT IN subquery (related to join anti-pattern)
-- @order_sensitive=true
SELECT * FROM ${case_db}.left_table WHERE id1 NOT IN (SELECT id1 FROM ${case_db}.right_table WHERE id1 IS NOT NULL) ORDER BY id1;

-- query 37
-- LEFT JOIN USING: reorder selected columns
-- @order_sensitive=true
SELECT dept, id1, name FROM ${case_db}.left_table LEFT JOIN ${case_db}.right_table USING(id1) ORDER BY id1;

-- query 38
-- Subquery + USING
-- @order_sensitive=true
SELECT * FROM (
    SELECT id1, name, age FROM ${case_db}.left_table WHERE id1 <= 5
) L JOIN (
    SELECT id1, dept FROM ${case_db}.right_table WHERE id1 <= 5
) R USING(id1) ORDER BY id1;

-- query 39
-- Aggregation over USING join in subquery
-- @order_sensitive=true
SELECT outer_query.id1, outer_query.total_count
FROM (
    SELECT id1, COUNT(*) as total_count
    FROM ${case_db}.left_table L JOIN ${case_db}.right_table R USING(id1)
    GROUP BY id1
) outer_query
WHERE outer_query.total_count > 0
ORDER BY outer_query.id1;

-- query 40
-- Subquery with IN + USING join (empty result)
-- @order_sensitive=true
SELECT id1, name, R.dept
FROM ${case_db}.left_table L JOIN ${case_db}.right_table R USING(id1)
WHERE L.id1 IN (
    SELECT id1 FROM ${case_db}.left_table WHERE salary > 6000
)
ORDER BY L.id1;

-- query 41
-- Derived table with expressions + LEFT JOIN USING
-- @order_sensitive=true
SELECT sub.id1, sub.id2, sub.emp_info, sub.dept_info
FROM (
    SELECT
        id1,
        id2,
        CONCAT(name, '-', CAST(age AS VARCHAR)) as emp_info,
        COALESCE(dept, 'Unknown') as dept_info
    FROM ${case_db}.left_table L LEFT JOIN ${case_db}.right_table R USING(id1, id2)
    WHERE id1 IS NOT NULL
) sub
WHERE sub.emp_info LIKE '%Alice%' OR sub.dept_info = 'Engineering'
ORDER BY sub.id1;

-- query 42
-- Aggregated subqueries joined with USING
-- @order_sensitive=true
SELECT
    L_AGG.id1,
    L_AGG.avg_salary,
    R_AGG.total_bonus
FROM (
    SELECT id1, AVG(salary) as avg_salary
    FROM ${case_db}.left_table
    WHERE salary IS NOT NULL
    GROUP BY id1
) L_AGG
JOIN (
    SELECT id1, SUM(bonus) as total_bonus
    FROM ${case_db}.right_table
    WHERE bonus IS NOT NULL
    GROUP BY id1
) R_AGG USING(id1)
ORDER BY L_AGG.id1;
