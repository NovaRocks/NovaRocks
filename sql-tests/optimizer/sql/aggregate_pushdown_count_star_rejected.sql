-- @tags=optimizer,aggregate_pushdown,negative
-- Test Objective:
-- COUNT(*) must NOT be pushed; the plan should contain a single
-- top-level AGGREGATE with no partial AGGREGATE underneath the join.
DROP TABLE IF EXISTS ${case_db}.t_agg_pd_neg_a;
DROP TABLE IF EXISTS ${case_db}.t_agg_pd_neg_b;
CREATE TABLE ${case_db}.t_agg_pd_neg_a (k INT);
CREATE TABLE ${case_db}.t_agg_pd_neg_b (k INT);
INSERT INTO ${case_db}.t_agg_pd_neg_a VALUES (1), (2);
INSERT INTO ${case_db}.t_agg_pd_neg_b VALUES (1);
EXPLAIN VERBOSE
SELECT COUNT(*)
FROM ${case_db}.t_agg_pd_neg_a a
INNER JOIN ${case_db}.t_agg_pd_neg_b b ON a.k = b.k;
