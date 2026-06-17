-- @order_sensitive=true
-- @tags=runtime_filter,outer_join,cross_exchange_guard
-- Test Objective:
-- 1. Validate FULL OUTER JOIN null-preserving semantics with runtime filter enabled.
-- 2. Compare against RuntimeFilterPushDown-disabled execution to guard cross-exchange placement.
-- Test Flow:
-- 1. Create/reset left, right, and dimension tables.
-- 2. Insert deterministic rows including NULL and right-only FULL OUTER keys.
-- 3. Run the same guarded query with runtime filter enabled and disabled.
DROP TABLE IF EXISTS ${case_db}.rf_outer_l;
DROP TABLE IF EXISTS ${case_db}.rf_outer_r;
DROP TABLE IF EXISTS ${case_db}.rf_outer_dim;
CREATE TABLE ${case_db}.rf_outer_l (
    id INT,
    k INT
)
TBLPROPERTIES ("format-version" = "3");
CREATE TABLE ${case_db}.rf_outer_r (
    k INT
)
TBLPROPERTIES ("format-version" = "3");
CREATE TABLE ${case_db}.rf_outer_dim (
    k INT
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.rf_outer_l VALUES
    (1, 10),
    (2, NULL),
    (3, 30);
INSERT INTO ${case_db}.rf_outer_r VALUES (10), (40);
INSERT INTO ${case_db}.rf_outer_dim VALUES (10), (30), (40);

SET disable_optimizer_rules = '';
-- @explain_contains=HASH JOIN (PARTITIONED, FULL OUTER
-- @explain_contains=build runtime filters:
-- @explain_contains=build_expr = (d.k)
-- @explain_not_contains=probe_expr = (l.k)
-- @explain_not_contains=probe_expr = (r.k)
WITH x AS (
    SELECT l.id, l.k, r.k AS rk
    FROM ${case_db}.rf_outer_l l
    FULL OUTER JOIN ${case_db}.rf_outer_r r
      ON l.k = r.k
),
q AS (
    SELECT id, k, rk
    FROM x
    WHERE k IS NULL AND rk IS NULL
    UNION ALL
    SELECT x.id, x.k, x.rk
    FROM x
    INNER JOIN ${case_db}.rf_outer_dim d
      ON COALESCE(x.k, x.rk) = d.k
)
SELECT id, k, rk
FROM q
ORDER BY COALESCE(id, 999999), COALESCE(k, rk);

SET disable_optimizer_rules = 'RuntimeFilterPushDown';
WITH x AS (
    SELECT l.id, l.k, r.k AS rk
    FROM ${case_db}.rf_outer_l l
    FULL OUTER JOIN ${case_db}.rf_outer_r r
      ON l.k = r.k
),
q AS (
    SELECT id, k, rk
    FROM x
    WHERE k IS NULL AND rk IS NULL
    UNION ALL
    SELECT x.id, x.k, x.rk
    FROM x
    INNER JOIN ${case_db}.rf_outer_dim d
      ON COALESCE(x.k, x.rk) = d.k
)
SELECT id, k, rk
FROM q
ORDER BY COALESCE(id, 999999), COALESCE(k, rk);

SET disable_optimizer_rules = '';
