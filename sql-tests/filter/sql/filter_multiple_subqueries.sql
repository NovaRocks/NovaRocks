-- @order_sensitive=true
-- @tags=filter,subquery
-- Test Objective:
-- 1. Multiple subqueries in the same SELECT (WHERE, HAVING, projection) must
--    all be rewritten. This was previously broken because
--    `infer_scalar_subquery_data_type` re-entered the analyzer and drained
--    the outer query's `collected_subqueries`, so any subquery seen BEFORE
--    a scalar one ended up un-rewritten and reached codegen as a stray
--    `SubqueryPlaceholder`. TPC-DS q14 (IN + HAVING-scalar) was the
--    canonical reproduction.
-- 2. Cover the three trigger shapes: IN-then-scalar in WHERE,
--    scalar-then-scalar in WHERE, multiple scalars in projection.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_subq;
CREATE TABLE ${case_db}.t_subq (
    id BIGINT NULL,
    v BIGINT NULL
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");
INSERT INTO ${case_db}.t_subq VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);

-- query 2
-- IN-then-scalar in WHERE — q14 shape. id IN {1,2,3} and v greater than min v (=10).
SELECT id, v FROM ${case_db}.t_subq
WHERE id IN (SELECT id FROM ${case_db}.t_subq WHERE id <= 3)
  AND v > (SELECT MIN(v) FROM ${case_db}.t_subq)
ORDER BY id;

-- query 3
-- scalar-then-IN in WHERE — opposite order (was already working pre-fix, included for parity).
SELECT id, v FROM ${case_db}.t_subq
WHERE v > (SELECT MIN(v) FROM ${case_db}.t_subq)
  AND id IN (SELECT id FROM ${case_db}.t_subq WHERE id <= 3)
ORDER BY id;

-- query 4
-- Two scalar subqueries in WHERE.
SELECT id, v FROM ${case_db}.t_subq
WHERE v > (SELECT MIN(v) FROM ${case_db}.t_subq)
  AND v < (SELECT MAX(v) FROM ${case_db}.t_subq)
ORDER BY id;

-- query 5
-- Two scalar subqueries in projection.
SELECT (SELECT MIN(v) FROM ${case_db}.t_subq) AS min_v,
       (SELECT MAX(v) FROM ${case_db}.t_subq) AS max_v;

-- query 6
-- IN in WHERE + scalar in HAVING (TPC-DS q14 step 1 shape).
SELECT id, sum(v) AS sv FROM ${case_db}.t_subq
WHERE id IN (SELECT id FROM ${case_db}.t_subq WHERE id <= 4)
GROUP BY id
HAVING sum(v) > (SELECT MIN(v) FROM ${case_db}.t_subq)
ORDER BY id;
