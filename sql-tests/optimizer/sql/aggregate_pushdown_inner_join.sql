-- @tags=optimizer,aggregate_pushdown
-- Test Objective:
-- 1. Verify the EXPLAIN VERBOSE plan shows a partial AGGREGATE under
--    the join and a final AGGREGATE on top.
-- 2. Future PRs touching aggregate pushdown must intentionally re-record.
--
-- Data design: 20 000 rows on the left, NDV(k) = 100 (k = v % 100).
-- With ANALYZE TABLE the cost gate sees NDV=100 << 20000*0.5, so it fires.
DROP TABLE IF EXISTS ${case_db}.t_agg_pd_a;
DROP TABLE IF EXISTS ${case_db}.t_agg_pd_b;
CREATE TABLE ${case_db}.t_agg_pd_a (k INT, v INT);
CREATE TABLE ${case_db}.t_agg_pd_b (k INT);
INSERT INTO ${case_db}.t_agg_pd_a
    SELECT generate_series % 100, generate_series FROM TABLE(generate_series(1, 20000));
INSERT INTO ${case_db}.t_agg_pd_b
    SELECT DISTINCT generate_series % 100 FROM TABLE(generate_series(1, 20000));
ANALYZE TABLE ${case_db}.t_agg_pd_a;
ANALYZE TABLE ${case_db}.t_agg_pd_b;
EXPLAIN VERBOSE
SELECT a.k, SUM(a.v)
FROM ${case_db}.t_agg_pd_a a
INNER JOIN ${case_db}.t_agg_pd_b b ON a.k = b.k
GROUP BY a.k;
