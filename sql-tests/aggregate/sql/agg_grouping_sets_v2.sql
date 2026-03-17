-- Migrated from dev/test/sql/test_grouping_sets/T/test_grouping_sets_v2
-- Test Objective:
-- 1. Validate GROUPING SETS, ROLLUP, CUBE with enable_rewrite_groupingsets_to_union_all=true.
-- 2. Validate grouping_id() and grouping() pseudo-columns.
-- 3. Cover NULL handling in grouping key columns.
-- 4. Cover CTE + JOIN + multi_distinct_count / count(distinct) with grouping sets.

-- query 1
-- Setup: enable union-all rewrite; create and populate tables
-- @skip_result_check=true
USE ${case_db};
SET enable_rewrite_groupingsets_to_union_all = true;
CREATE TABLE tbl_with_null1 (
    k1  date,
    k2  datetime,
    k3  varchar(20),
    k4  varchar(20),
    k5  boolean,
    k6  tinyint,
    k7  smallint,
    k8  int,
    K9  bigint,
    K10 largeint,
    K11 float,
    K12 double,
    K13 decimal(27,9)
) DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 2
-- @skip_result_check=true
USE ${case_db};
INSERT INTO tbl_with_null1 VALUES
 ('2020-10-22','2020-10-23 12:12:12','k1','k4',0,1,2,3,4,5,1.1,1.12,2.889)
,('2020-10-23','2020-10-23 12:12:12','k2','k4',0,0,2,3,4,5,1.1,1.12,2.889)
,('2020-10-24','2020-10-23 12:12:12','k3','k4',0,1,2,3,4,5,1.1,1.12,2.889)
,('2020-10-25','2020-10-23 12:12:12','k4','k4',0,1,2,3,4,NULL,NULL,NULL,2.889)
,('2020-10-26',NULL, NULL, NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);

-- query 3
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE tbl_with_null2 (
    k1  date,
    k2  datetime,
    k3  varchar(20),
    k4  varchar(20),
    k5  boolean,
    k6  tinyint,
    k7  smallint,
    k8  int,
    K9  bigint,
    K10 largeint,
    K11 float,
    K12 double,
    K13 decimal(27,9)
) DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 9
PROPERTIES ("replication_num" = "1");

-- query 4
-- @skip_result_check=true
USE ${case_db};
INSERT INTO tbl_with_null2 VALUES
 ('2020-10-22','2020-10-23 12:12:12','k1','k4',0,1,2,3,4,5,1.1,1.12,2.889)
,('2020-10-23','2020-10-23 12:12:12','k2','k4',0,0,2,3,4,5,1.1,1.12,2.889)
,('2020-10-24','2020-10-23 12:12:12','k3','k4',0,1,2,3,4,5,1.1,1.12,2.889)
,('2020-10-25','2020-10-23 12:12:12','k4','k4',0,1,2,3,4,NULL,NULL,NULL,2.889)
,('2020-10-26',NULL, NULL, NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);

-- query 5
-- GROUPING SETS: () and (k1)
-- @order_sensitive=true
USE ${case_db};
SELECT k1, sum(k8) AS v1
FROM tbl_with_null1 GROUP BY GROUPING SETS((), (k1)) ORDER BY k1, v1;

-- query 6
-- GROUPING SETS: () and (k1, k2)
-- @order_sensitive=true
USE ${case_db};
SELECT k1, k2, sum(k8) AS v1
FROM tbl_with_null1 GROUP BY GROUPING SETS((), (k1, k2)) ORDER BY k1, k2, v1;

-- query 7
-- ROLLUP(k1, k2)
-- @order_sensitive=true
USE ${case_db};
SELECT k1, k2, sum(k8) AS v1
FROM tbl_with_null1 GROUP BY ROLLUP(k1, k2) ORDER BY k1, k2, v1;

-- query 8
-- CUBE(k1, k2)
-- @order_sensitive=true
USE ${case_db};
SELECT k1, k2, sum(k8) AS v1
FROM tbl_with_null1 GROUP BY CUBE(k1, k2) ORDER BY k1, k2, v1;

-- query 9
-- grouping_id/grouping with GROUPING SETS (k1)
-- @order_sensitive=true
USE ${case_db};
SELECT k1, grouping_id(k1) AS f1, grouping(k1) AS f2, sum(k8) AS v1
FROM tbl_with_null1 GROUP BY GROUPING SETS((), (k1)) ORDER BY k1, f1, f2, v1;

-- query 10
-- grouping_id/grouping with GROUPING SETS (k1, k2)
-- @order_sensitive=true
USE ${case_db};
SELECT k1, k2, grouping_id(k1, k2) AS f1, grouping(k1, k2) AS f2, sum(k8) AS v1
FROM tbl_with_null1 GROUP BY GROUPING SETS((), (k1, k2)) ORDER BY f1, f2, k1, k2, v1;

-- query 11
-- grouping_id/grouping with CUBE(k1, k2)
-- @order_sensitive=true
USE ${case_db};
SELECT k1, k2, grouping_id(k1, k2) AS f1, grouping(k1, k2) AS f2, sum(k8) AS v1
FROM tbl_with_null1 GROUP BY CUBE(k1, k2) ORDER BY f1, f2, k1, k2, v1;

-- query 12
-- grouping_id/grouping with ROLLUP(k1, k2)
-- @order_sensitive=true
USE ${case_db};
SELECT k1, k2, grouping_id(k1, k2) AS f1, grouping(k1, k2) AS f2, sum(k8) AS v1
FROM tbl_with_null1 GROUP BY ROLLUP(k1, k2) ORDER BY f1, f2, k1, k2, v1;

-- query 13
-- CTE + multi_distinct_count + grouping sets with 3 grouping levels
-- @order_sensitive=true
USE ${case_db};
WITH cte1 AS (SELECT * FROM tbl_with_null1 ORDER BY k1)
SELECT multi_distinct_count(k4) AS v1, k1, grouping(k1) AS f_k1, k2, grouping(k2) AS f_k2
FROM cte1 GROUP BY GROUPING SETS ((), (k1), (k1, k2))
ORDER BY f_k1 DESC, k1 ASC, f_k2 DESC, k2 ASC, v1 LIMIT 10 OFFSET 0;

-- query 14
-- CTE + JOIN on k1 + count(distinct) + grouping sets
-- @order_sensitive=true
USE ${case_db};
WITH cte1 AS (SELECT t1.k1, t2.k2, t1.k3, t2.k4, t2.k5 FROM tbl_with_null1 t1 JOIN tbl_with_null2 t2 ON t1.k1=t2.k1 ORDER BY t1.k1)
SELECT count(DISTINCT k3) AS v1, count(DISTINCT k4) AS v2, count(DISTINCT k5) AS v3, k1, grouping(k1) AS f_k1, k2, grouping(k2) AS f_k2
FROM cte1 GROUP BY GROUPING SETS ((), (k1), (k1, k2))
ORDER BY f_k1 DESC, k1 ASC, f_k2 DESC, k2 ASC, v1, v2, v3 LIMIT 10 OFFSET 0;

-- query 15
-- CTE + JOIN on k6 + count(distinct) + grouping sets
-- @order_sensitive=true
USE ${case_db};
WITH cte1 AS (SELECT t1.k1, t2.k2, t1.k3, t2.k4, t2.k5 FROM tbl_with_null1 t1 JOIN tbl_with_null2 t2 ON t1.k6=t2.k6 ORDER BY t1.k6)
SELECT count(DISTINCT k3) AS v1, count(DISTINCT k4) AS v2, count(DISTINCT k5) AS v3, k1, grouping(k1) AS f_k1, k2, grouping(k2) AS f_k2
FROM cte1 GROUP BY GROUPING SETS ((), (k1), (k1, k2))
ORDER BY f_k1 DESC, k1 ASC, f_k2 DESC, k2 ASC, v1, v2, v3 LIMIT 10 OFFSET 0;
