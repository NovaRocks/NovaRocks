-- @tags=join,reorder,performance
-- Test Objective:
-- 1. Regression: the JoinReorder rewrite rule used to run its full
--    `reorder_joins_cbo` walk at *every* node visited by the pipeline,
--    including non-Join nodes (Project, Filter, CTEAnchor, SubqueryAlias,
--    Scan, …). For plans with deeply-nested CTEs / set ops / correlated
--    subqueries (the TPC-DS q14 shape), the redundant per-node walks
--    alone exhausted the 10-second optimizer budget before any of the
--    real per-join work could finish, surfacing as
--    `optimizer timeout during JoinReorder`.
-- 2. This test shapes a small query that hits the same redundancy
--    pattern (multiple CTEs feeding both an IN-subquery in WHERE and a
--    scalar subquery in HAVING, plus a derived-table self-join). With
--    the fix, optimization completes well under the budget.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_jr_left;
DROP TABLE IF EXISTS ${case_db}.t_jr_right;
DROP TABLE IF EXISTS ${case_db}.t_jr_dim;
CREATE TABLE ${case_db}.t_jr_left (
    k BIGINT NULL,
    v BIGINT NULL,
    d BIGINT NULL
) ENGINE=OLAP
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 3
PROPERTIES ("replication_num" = "1");
CREATE TABLE ${case_db}.t_jr_right (
    k BIGINT NULL,
    v BIGINT NULL,
    d BIGINT NULL
) ENGINE=OLAP
DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 3
PROPERTIES ("replication_num" = "1");
CREATE TABLE ${case_db}.t_jr_dim (
    d BIGINT NULL,
    label BIGINT NULL
) ENGINE=OLAP
DUPLICATE KEY(d)
DISTRIBUTED BY HASH(d) BUCKETS 3
PROPERTIES ("replication_num" = "1");
INSERT INTO ${case_db}.t_jr_left  VALUES (1, 10, 100), (2, 20, 200), (3, 30, 100);
INSERT INTO ${case_db}.t_jr_right VALUES (1, 11, 100), (2, 22, 200), (3, 33, 200);
INSERT INTO ${case_db}.t_jr_dim   VALUES (100, 1), (200, 2), (300, 3);

-- query 2
-- @order_sensitive=true
-- TPC-DS q14 shape, miniaturised: two CTEs (one referenced via IN in WHERE,
-- the other via a scalar in HAVING), plus a derived-table self-join. This
-- compounded into a deeply-nested plan that the old JoinReorder rule
-- spent the entire 10-second budget walking redundantly.
WITH allowed_keys AS (
    SELECT k FROM ${case_db}.t_jr_left
    INTERSECT
    SELECT k FROM ${case_db}.t_jr_right
),
avg_val AS (
    SELECT AVG(v) AS avg_v FROM (
        SELECT v FROM ${case_db}.t_jr_left
        UNION ALL
        SELECT v FROM ${case_db}.t_jr_right
    ) x
)
SELECT t_now.k AS k, t_now.s AS s_now, t_prev.s AS s_prev
FROM
    (SELECT l.k AS k, SUM(l.v * r.v) AS s
     FROM ${case_db}.t_jr_left l, ${case_db}.t_jr_right r, ${case_db}.t_jr_dim d
     WHERE l.k = r.k
       AND l.d = d.d
       AND l.k IN (SELECT k FROM allowed_keys)
     GROUP BY l.k
     HAVING SUM(l.v * r.v) > (SELECT avg_v FROM avg_val)) t_now,
    (SELECT l.k AS k, SUM(l.v * r.v) AS s
     FROM ${case_db}.t_jr_left l, ${case_db}.t_jr_right r, ${case_db}.t_jr_dim d
     WHERE l.k = r.k
       AND r.d = d.d
       AND l.k IN (SELECT k FROM allowed_keys)
     GROUP BY l.k
     HAVING SUM(l.v * r.v) > (SELECT avg_v FROM avg_val)) t_prev
WHERE t_now.k = t_prev.k
ORDER BY t_now.k;
