-- @tags=optimizer,aggregate_pushdown,outer
-- Test Objective:
-- LEFT OUTER JOIN with aggregate on the preserved (left) side: the
-- rule must still push to the left side.
-- Data design: 20 000 rows on the left, NDV(k) = 100. ANALYZE TABLE
-- ensures the cost gate fires.
DROP TABLE IF EXISTS ${case_db}.t_agg_pd_lo_a;
DROP TABLE IF EXISTS ${case_db}.t_agg_pd_lo_b;
CREATE TABLE ${case_db}.t_agg_pd_lo_a (k INT, v INT);
CREATE TABLE ${case_db}.t_agg_pd_lo_b (k INT);
INSERT INTO ${case_db}.t_agg_pd_lo_a
    SELECT generate_series % 100, generate_series FROM TABLE(generate_series(1, 20000));
INSERT INTO ${case_db}.t_agg_pd_lo_b
    SELECT DISTINCT generate_series % 50 FROM TABLE(generate_series(1, 50));
ANALYZE TABLE ${case_db}.t_agg_pd_lo_a;
ANALYZE TABLE ${case_db}.t_agg_pd_lo_b;
EXPLAIN VERBOSE
SELECT a.k, SUM(a.v)
FROM ${case_db}.t_agg_pd_lo_a a
LEFT OUTER JOIN ${case_db}.t_agg_pd_lo_b b ON a.k = b.k
GROUP BY a.k;
