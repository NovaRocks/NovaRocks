-- @tags=optimizer,aggregate_pushdown,session_rule_disable
-- Test Objective:
-- Verify SET disable_optimizer_rules = 'AggregatePushdown' suppresses
-- the rewrite. Two EXPLAIN VERBOSE outputs around the SET must differ:
-- first has partial AGGREGATE under the join; second has a single top-level
-- AGGREGATE only.
-- Data design: 20 000 rows on the left, NDV(k) = 100. ANALYZE TABLE
-- ensures the cost gate fires in the baseline (enabled) case.
DROP TABLE IF EXISTS ${case_db}.t_agg_pd_dis_a;
DROP TABLE IF EXISTS ${case_db}.t_agg_pd_dis_b;
CREATE TABLE ${case_db}.t_agg_pd_dis_a (k INT, v INT);
CREATE TABLE ${case_db}.t_agg_pd_dis_b (k INT);
INSERT INTO ${case_db}.t_agg_pd_dis_a
    SELECT generate_series % 100, generate_series FROM TABLE(generate_series(1, 20000));
INSERT INTO ${case_db}.t_agg_pd_dis_b
    SELECT DISTINCT generate_series % 100 FROM TABLE(generate_series(1, 20000));
ANALYZE TABLE ${case_db}.t_agg_pd_dis_a;
ANALYZE TABLE ${case_db}.t_agg_pd_dis_b;

-- Baseline (AggregatePushdown enabled): expect partial AGGREGATE under join.
EXPLAIN VERBOSE
SELECT a.k, SUM(a.v)
FROM ${case_db}.t_agg_pd_dis_a a
INNER JOIN ${case_db}.t_agg_pd_dis_b b ON a.k = b.k
GROUP BY a.k;

SET disable_optimizer_rules = 'AggregatePushdown';

-- With AggregatePushdown disabled: single top-level AGGREGATE, no partial.
EXPLAIN VERBOSE
SELECT a.k, SUM(a.v)
FROM ${case_db}.t_agg_pd_dis_a a
INNER JOIN ${case_db}.t_agg_pd_dis_b b ON a.k = b.k
GROUP BY a.k;

SET disable_optimizer_rules = '';
