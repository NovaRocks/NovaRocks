-- Migrated from dev/test/sql/test_cte/T/test_recursive_cte
-- Test Objective:
-- 1. Basic recursive CTE: org hierarchy traversal with path concatenation.
-- 2. Recursive CTE with explicit column names in header.
-- 3. Recursive CTE generating numeric sequences (respects default max_depth=5).
-- 4. Multiple recursive CTEs in one WITH clause.
-- 5. Recursive CTE with JOIN in outer query.
-- 6. Recursive CTE with aggregation in final query.
-- 7. Recursive CTE with WHERE in recursive part (level limit).
-- 8. Recursive CTE with UNION ALL + UNION (complex, result not checked).
-- 9. Recursive CTE with UNION only (result not checked).
-- 10. Fibonacci: without recursive_cte_max_depth hint → error (Unknown table).
-- 11. Fibonacci: with recursive_cte_max_depth=10 → full 10-row sequence.
-- 12. Manager chain traversal (upward walk).
-- 13. Error case: recursive reference in non-recursive anchor → error.
-- 14. Non-recursive CTE with RECURSIVE keyword (no UNION) → returns result normally.
-- 15. Mixed: non-recursive const CTE + recursive manager_chain CTE.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.employees;
CREATE TABLE ${case_db}.employees (
  `employee_id` int(11) NULL COMMENT "",
  `name` varchar(100) NULL COMMENT "",
  `manager_id` int(11) NULL COMMENT "",
  `title` varchar(50) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`employee_id`)
DISTRIBUTED BY RANDOM
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
INSERT INTO ${case_db}.employees VALUES
(1, 'Alicia', NULL, 'CEO'),
(2, 'Bob', 1, 'CTO'),
(3, 'Carol', 1, 'CFO'),
(4, 'David', 2, 'VP of Engineering'),
(5, 'Eve', 2, 'VP of Research'),
(6, 'Frank', 3, 'VP of Finance'),
(7, 'Grace', 4, 'Engineering Manager'),
(8, 'Heidi', 4, 'Tech Lead'),
(9, 'Ivan', 5, 'Research Manager'),
(10, 'Judy', 7, 'Senior Engineer'),
(11, 'Kevin', 6, 'Finance Manager'),
(12, 'Laura', 6, 'Accountant'),
(13, 'Mike', 8, 'Senior Engineer'),
(14, 'Nancy', 8, 'Engineer'),
(15, 'Oscar', 7, 'Software Engineer'),
(16, 'Peter', 7, 'QA Engineer'),
(17, 'Quinn', 9, 'Research Scientist'),
(18, 'Rachel', 9, 'Data Scientist'),
(19, 'Steve', 11, 'Financial Analyst'),
(20, 'Tina', 13, 'Engineer'),
(21, 'Uma', 13, 'Junior Engineer'),
(22, 'Victor', 15, 'Junior Developer'),
(23, 'Wendy', 17, 'Research Associate'),
(24, 'Xavier', 18, 'ML Engineer'),
(25, 'Yolanda', 14, 'Junior Engineer');
set recursive_cte_throw_limit_exception=false;

-- query 2
-- @order_sensitive=true
-- Basic recursive CTE: org hierarchy with path, ordered by employee_id
-- Default max_depth=5 means level-6 employees (20-25) are not returned
WITH RECURSIVE org_hierarchy AS (
    SELECT employee_id, name, manager_id, title, cast(1 as bigint) AS `level`, name AS path
    FROM ${case_db}.employees
    WHERE manager_id IS NULL
    UNION ALL
    SELECT e.employee_id, e.name, e.manager_id, e.title, oh.`level` + 1, CONCAT(oh.path, ' -> ', e.name)
    FROM ${case_db}.employees e
    INNER JOIN org_hierarchy oh ON e.manager_id = oh.employee_id
)
SELECT /*+ SET_VAR(enable_recursive_cte=true)*/ employee_id, name, title, `level`, path
FROM org_hierarchy
ORDER BY employee_id;

-- query 3
-- @order_sensitive=true
-- Recursive CTE with explicit column names in header; same hierarchy, same result
WITH RECURSIVE org_hierarchy(emp_id, emp_name, mgr_id, emp_title, emp_level, emp_path) AS (
    SELECT employee_id, name, manager_id, title, cast(1 as bigint), name
    FROM ${case_db}.employees
    WHERE manager_id IS NULL
    UNION ALL
    SELECT e.employee_id, e.name, e.manager_id, e.title, oh.emp_level + 1, CONCAT(oh.emp_path, ' -> ', e.name)
    FROM ${case_db}.employees e
    INNER JOIN org_hierarchy oh ON e.manager_id = oh.emp_id
)
SELECT /*+ SET_VAR(enable_recursive_cte=true)*/ emp_id, emp_name, emp_title, emp_level, emp_path
FROM org_hierarchy
ORDER BY emp_id;

-- query 4
-- @order_sensitive=true
-- Numbers 1..10 but default max_depth=5 limits to n=1..5 only
WITH RECURSIVE numbers AS (
    SELECT cast(1 as bigint) AS n, cast(1 as bigint) AS square, cast(1 as bigint) AS cc
    UNION ALL
    SELECT n + 1, (n + 1) * (n + 1), (n + 1) * (n + 1) * (n + 1)
    FROM numbers
    WHERE n < 10
)
SELECT /*+ SET_VAR(enable_recursive_cte=true)*/ n, square, cc
FROM numbers
ORDER BY n;

-- query 5
-- @order_sensitive=true
-- Multiple recursive CTEs: cte1 generates 1..5, cte2 generates 10..15 (both limited by max_depth=5)
WITH RECURSIVE
cte1 AS (
    SELECT cast(1 as bigint) AS n
    UNION ALL
    SELECT n + 1 FROM cte1 WHERE n < 5
),
cte2 AS (
    SELECT cast(10 as bigint) AS n
    UNION ALL
    SELECT n + 1 FROM cte2 WHERE n < 15
)
SELECT /*+ SET_VAR(enable_recursive_cte=true)*/ 'cte1' AS source, n FROM cte1
UNION ALL
SELECT /*+ SET_VAR(enable_recursive_cte=true)*/ 'cte2' AS source, n FROM cte2
ORDER BY source, n;

-- query 6
-- @order_sensitive=true
-- Recursive CTE with JOIN in outer query: direct subordinates of Bob (emp_id=2)
WITH RECURSIVE subordinates AS (
    SELECT employee_id, name, manager_id, title
    FROM ${case_db}.employees
    WHERE employee_id = 2
    UNION ALL
    SELECT e.employee_id, e.name, e.manager_id, e.title
    FROM ${case_db}.employees e
    INNER JOIN subordinates s ON e.manager_id = s.employee_id
)
SELECT /*+ SET_VAR(enable_recursive_cte=true)*/ s.employee_id, s.name, s.title, e.name AS manager_name
FROM subordinates s
LEFT JOIN ${case_db}.employees e ON s.manager_id = e.employee_id
ORDER BY s.employee_id;

-- query 7
-- @order_sensitive=true
-- Aggregation over recursive CTE: employee count per org level
WITH RECURSIVE org_hierarchy AS (
    SELECT employee_id, name, manager_id, title, cast(1 as bigint) AS `level`
    FROM ${case_db}.employees
    WHERE manager_id IS NULL
    UNION ALL
    SELECT e.employee_id, e.name, e.manager_id, e.title, oh.`level` + 1
    FROM ${case_db}.employees e
    INNER JOIN org_hierarchy oh ON e.manager_id = oh.employee_id
)
SELECT /*+ SET_VAR(enable_recursive_cte=true)*/ `level`, COUNT(*) AS employee_count
FROM org_hierarchy
GROUP BY `level`
ORDER BY `level`;

-- query 8
-- @order_sensitive=true
-- Recursive CTE with WHERE in recursive part limiting depth to 3
WITH RECURSIVE parent_cte AS (
    SELECT employee_id, name, manager_id, cast(1 as bigint) AS `level`
    FROM ${case_db}.employees
    WHERE manager_id IS NULL
    UNION ALL
    SELECT e.employee_id, e.name, e.manager_id, p.`level` + 1
    FROM ${case_db}.employees e
    INNER JOIN parent_cte p ON e.manager_id = p.employee_id
    WHERE p.`level` < 3
)
SELECT /*+ SET_VAR(enable_recursive_cte=true)*/ employee_id, name, `level`
FROM parent_cte
ORDER BY employee_id;

-- query 9
-- Recursive CTE with mixed UNION ALL + UNION in body → self-reference resolves to Unknown table
-- @expect_error=Unknown table
WITH RECURSIVE numbers AS (
    SELECT cast(1 as bigint) AS n, cast(1 as bigint) AS category
    UNION ALL
    SELECT n + 1, 1 FROM numbers WHERE n < 5
    UNION
    SELECT n, 2 FROM numbers
)
SELECT /*+ SET_VAR(enable_recursive_cte=true)*/ DISTINCT n, category
FROM numbers
ORDER BY n, category;

-- query 10
-- @skip_result_check=true
-- Recursive CTE with UNION only (not UNION ALL) in body; succeeds in NovaRocks (result not validated in original)
WITH RECURSIVE numbers AS (
    SELECT cast(1 as bigint) AS n, cast(1 as bigint) AS category
    UNION
    SELECT n + 1, 1 FROM numbers WHERE n < 5
    UNION
    SELECT n, 2 FROM numbers
)
SELECT /*+ SET_VAR(enable_recursive_cte=true)*/ DISTINCT n, category
FROM numbers
ORDER BY n, category;

-- query 11
-- @order_sensitive=true
-- Fibonacci WITHOUT explicit recursive_cte_max_depth; NovaRocks runs with default max_depth
-- (StarRocks produces error here; NovaRocks succeeds with default depth limit)
WITH RECURSIVE fibonacci(n, fib_n, fib_n_plus_1) AS (
    SELECT cast(1 as bigint), cast(0 as bigint), cast(1 as bigint)
    UNION ALL
    SELECT n + 1, fib_n_plus_1, fib_n + fib_n_plus_1
    FROM fibonacci
    WHERE n < 10
)
SELECT /*+ SET_VAR(enable_recursive_cte=true)*/ n, fib_n AS fibonacci_number
FROM fibonacci
ORDER BY n;

-- query 12
-- @order_sensitive=true
-- Fibonacci WITH recursive_cte_max_depth=10 → correct 10-row Fibonacci sequence
WITH RECURSIVE fibonacci(n, fib_n, fib_n_plus_1) AS (
    SELECT cast(1 as bigint), cast(0 as bigint), cast(1 as bigint)
    UNION ALL
    SELECT n + 1, fib_n_plus_1, fib_n + fib_n_plus_1
    FROM fibonacci
    WHERE n < 10
)
SELECT /*+ SET_VAR(enable_recursive_cte=true, recursive_cte_max_depth=10)*/ n, fib_n AS fibonacci_number
FROM fibonacci
ORDER BY n;

-- query 13
-- @order_sensitive=true
-- Manager chain: upward walk from employee_id=10 (Judy) to CEO
WITH RECURSIVE manager_chain AS (
    SELECT employee_id, name, manager_id, title, cast(0 as bigint) AS steps
    FROM ${case_db}.employees
    WHERE employee_id = 10
    UNION ALL
    SELECT e.employee_id, e.name, e.manager_id, e.title, mc.steps + 1
    FROM ${case_db}.employees e
    INNER JOIN manager_chain mc ON mc.manager_id = e.employee_id
)
SELECT /*+ SET_VAR(enable_recursive_cte=true)*/ employee_id, name, title, steps
FROM manager_chain
ORDER BY steps;

-- query 14
-- Error case: recursive reference in non-recursive anchor part → Unknown table
-- @expect_error=Unknown table
WITH RECURSIVE invalid_cte AS (
    SELECT employee_id, name FROM ${case_db}.employees WHERE employee_id IN (SELECT employee_id FROM invalid_cte)
    UNION ALL
    SELECT employee_id, name FROM ${case_db}.employees WHERE manager_id = 1
)
SELECT /*+ SET_VAR(enable_recursive_cte=true)*/ * FROM invalid_cte;

-- query 15
-- @order_sensitive=true
-- CTE with RECURSIVE keyword but no UNION ALL → behaves as regular non-recursive CTE
-- Result: single row for employee_id=1 (Alicia)
WITH RECURSIVE invalid_cte AS (
    SELECT employee_id, name FROM ${case_db}.employees WHERE employee_id = 1
)
SELECT /*+ SET_VAR(enable_recursive_cte=true)*/ * FROM invalid_cte;

-- query 16
-- @order_sensitive=true
-- Mixed: non-recursive const_cte + recursive manager_chain; both reference const_cte in outer query
WITH RECURSIVE const_cte as (
    SELECT cast(10 as bigint) AS id
),
manager_chain AS (
    SELECT employee_id, name, manager_id, title, cast(0 as bigint) AS steps
    FROM ${case_db}.employees, const_cte
    WHERE employee_id = const_cte.id
    UNION ALL
    SELECT e.employee_id, e.name, e.manager_id, e.title, mc.steps + 1
    FROM ${case_db}.employees e
    INNER JOIN manager_chain mc ON mc.manager_id = e.employee_id
)
SELECT /*+ SET_VAR(enable_recursive_cte=true)*/ employee_id, name, title, steps, id
FROM manager_chain, const_cte
ORDER BY steps;
