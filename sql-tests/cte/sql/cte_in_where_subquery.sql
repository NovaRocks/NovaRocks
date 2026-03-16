-- Migrated from dev/test/sql/test_analyzer/T/test_cte_in_where_subquery
-- Test Objective:
-- 1. CTE referenced in WHERE IN subquery.
-- 2. CTE with column alias referenced in WHERE IN subquery.
-- 3. CTE used in both main query and WHERE IN subquery.
-- 4. Multiple CTEs, each used in a separate WHERE subquery.
-- 5. CTE used in JOIN ON clause with IN subquery (original bug fix case).
-- 6. CTE with JOIN used in ON clause IN subquery.
-- 7. CTE with EXISTS subquery.
-- 8. CTE with correlated EXISTS in ON clause.
-- 9. CTE with scalar subquery (scalar aggregate).
-- 10. Nested subquery referencing CTE.
-- 11. CTE with nested IN subqueries.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_cte_0;
DROP TABLE IF EXISTS ${case_db}.t_cte_1;
CREATE TABLE ${case_db}.t_cte_0 (
    v1 bigint NULL,
    v2 bigint NULL,
    v3 bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(v1, v2, v3)
DISTRIBUTED BY HASH(v1) BUCKETS 3
PROPERTIES ("replication_num" = "1");
CREATE TABLE ${case_db}.t_cte_1 (
    v4 bigint NULL,
    v5 bigint NULL,
    v6 bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(v4, v5, v6)
DISTRIBUTED BY HASH(v4) BUCKETS 3
PROPERTIES ("replication_num" = "1");
INSERT INTO ${case_db}.t_cte_0 VALUES (1, 2, 3), (2, 3, 4), (3, 4, 5);
INSERT INTO ${case_db}.t_cte_1 VALUES (1, 10, 100), (2, 20, 200), (4, 40, 400);

-- query 2
-- @order_sensitive=true
-- CTE referenced in WHERE IN subquery; all 3 rows of t_cte_0 match
WITH a1 AS (SELECT v1 FROM ${case_db}.t_cte_0)
SELECT v1 FROM ${case_db}.t_cte_0 WHERE v1 IN (SELECT v1 FROM a1) ORDER BY v1;

-- query 3
-- @order_sensitive=true
-- CTE with column alias (cust_no); same result
WITH a1 AS (SELECT v1 AS cust_no FROM ${case_db}.t_cte_0)
SELECT v1 FROM ${case_db}.t_cte_0 WHERE v1 IN (SELECT cust_no FROM a1) ORDER BY v1;

-- query 4
-- @order_sensitive=true
-- CTE used in both outer SELECT and WHERE IN subquery; returns all 3 rows with v1,v2,v3
WITH a1 AS (SELECT v1, v2 FROM ${case_db}.t_cte_0)
SELECT * FROM ${case_db}.t_cte_0 WHERE v1 IN (SELECT v1 FROM a1) ORDER BY v1;

-- query 5
-- @order_sensitive=true
-- Two CTEs, each used in separate WHERE subquery predicates
WITH a1 AS (SELECT v1 FROM ${case_db}.t_cte_0),
     a2 AS (SELECT v2 FROM ${case_db}.t_cte_0)
SELECT v1 FROM ${case_db}.t_cte_0
WHERE v1 IN (SELECT v1 FROM a1) AND v2 IN (SELECT v2 FROM a2) ORDER BY v1;

-- query 6
-- @order_sensitive=true
-- CTE referenced in JOIN ON clause with IN subquery (original bug fix case)
-- Only rows where t_cte_0.v1 matches t_cte_1.v4 AND is in CTE: v1=1,2
WITH a1 AS (SELECT v1 FROM ${case_db}.t_cte_0)
SELECT t_cte_0.v1 FROM ${case_db}.t_cte_0
JOIN ${case_db}.t_cte_1 ON t_cte_0.v1 = t_cte_1.v4
AND t_cte_0.v1 IN (SELECT v1 FROM a1) ORDER BY t_cte_0.v1;

-- query 7
-- @order_sensitive=true
-- CTE with JOIN used in ON clause IN subquery
WITH a1 AS (SELECT t_cte_0.v1 AS cust_no FROM ${case_db}.t_cte_0 JOIN ${case_db}.t_cte_1 ON t_cte_0.v1 = t_cte_1.v4)
SELECT t_cte_0.v1 FROM ${case_db}.t_cte_0
JOIN ${case_db}.t_cte_1 ON t_cte_0.v1 = t_cte_1.v4
AND t_cte_0.v1 IN (SELECT cust_no FROM a1) ORDER BY t_cte_0.v1;

-- query 8
-- @order_sensitive=true
-- CTE with EXISTS subquery; all 3 rows exist in a1
WITH a1 AS (SELECT v1 FROM ${case_db}.t_cte_0)
SELECT v1 FROM ${case_db}.t_cte_0
WHERE EXISTS (SELECT 1 FROM a1 WHERE a1.v1 = t_cte_0.v1) ORDER BY v1;

-- query 9
-- @order_sensitive=true
-- CTE with correlated EXISTS in JOIN ON clause
WITH a1 AS (SELECT v1, v2 FROM ${case_db}.t_cte_0)
SELECT t_cte_0.v1 FROM ${case_db}.t_cte_0
JOIN ${case_db}.t_cte_1 ON t_cte_0.v1 = t_cte_1.v4
AND EXISTS (SELECT 1 FROM a1 WHERE a1.v1 = t_cte_0.v1) ORDER BY t_cte_0.v1;

-- query 10
-- @order_sensitive=true
-- CTE with scalar subquery (MAX aggregate); returns only the row where v1 = max(v1) = 3
WITH a1 AS (SELECT MAX(v1) AS max_v1 FROM ${case_db}.t_cte_0)
SELECT v1 FROM ${case_db}.t_cte_0 WHERE v1 = (SELECT max_v1 FROM a1) ORDER BY v1;

-- query 11
-- @order_sensitive=true
-- Nested subquery referencing CTE: WHERE v1 IN (SELECT v1 FROM (SELECT v1 FROM a1) t)
WITH a1 AS (SELECT v1 FROM ${case_db}.t_cte_0)
SELECT v1 FROM ${case_db}.t_cte_0
WHERE v1 IN (SELECT v1 FROM (SELECT v1 FROM a1) t) ORDER BY v1;

-- query 12
-- @order_sensitive=true
-- Nested IN subqueries: v1 IN (a1 WHERE v1 IN (a2 with v1>1)) → returns v1=2,3
WITH a1 AS (SELECT v1 FROM ${case_db}.t_cte_0),
     a2 AS (SELECT v1 FROM ${case_db}.t_cte_0 WHERE v1 > 1)
SELECT v1 FROM ${case_db}.t_cte_0
WHERE v1 IN (SELECT v1 FROM a1 WHERE v1 IN (SELECT v1 FROM a2)) ORDER BY v1;
