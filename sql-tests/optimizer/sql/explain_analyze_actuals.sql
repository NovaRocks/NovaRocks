-- @tags=optimizer,explain_analyze,actuals
-- Test Objective:
-- 1. EXPLAIN ANALYZE keeps the query-level timing header.
-- 2. EXPLAIN ANALYZE renders per-node actual metrics next to estimates.
-- 3. The query uses join + aggregate so actual metrics cover non-scan operators too.
DROP TABLE IF EXISTS ${case_db}.explain_analyze_actuals_l;
DROP TABLE IF EXISTS ${case_db}.explain_analyze_actuals_r;
CREATE TABLE ${case_db}.explain_analyze_actuals_l (k INT, v INT);
CREATE TABLE ${case_db}.explain_analyze_actuals_r (k INT, v INT);
INSERT INTO ${case_db}.explain_analyze_actuals_l VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO ${case_db}.explain_analyze_actuals_r VALUES (1, 100), (2, 200), (4, 400);

-- @skip_result_check=true
-- @result_contains=Planning:
-- @result_contains=Execution:
-- @result_contains=stats={rows=
-- @result_contains=act={rows=
-- @result_contains=} act={rows=
-- @result_contains=SCAN ${case_db}.explain_analyze_actuals_l (alias=l) stats={rows=3 conf=estimated} act={rows=3
-- @result_contains=HASH JOIN
-- @result_contains=HASH AGGREGATE
EXPLAIN ANALYZE
SELECT COUNT(*)
FROM ${case_db}.explain_analyze_actuals_l l
INNER JOIN ${case_db}.explain_analyze_actuals_r r ON l.k = r.k;
