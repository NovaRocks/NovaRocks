-- @tags=optimizer,oq9,residual,session_rule_disable
-- Test Objective:
-- SET disable_optimizer_rules='JoinPredicateMoveAround' suppresses derived
-- predicates that move from an already-filtered join child to a parent join sibling.
DROP TABLE IF EXISTS ${case_db}.residual_mad_a;
DROP TABLE IF EXISTS ${case_db}.residual_mad_b;
DROP TABLE IF EXISTS ${case_db}.residual_mad_c;
CREATE TABLE ${case_db}.residual_mad_a (k INT, payload INT);
CREATE TABLE ${case_db}.residual_mad_b (k INT, payload INT);
CREATE TABLE ${case_db}.residual_mad_c (k INT, payload INT);

SET disable_optimizer_rules = 'JoinReorder,JoinCommutativity';

EXPLAIN VERBOSE
SELECT a.payload, b.payload, c.payload
FROM ${case_db}.residual_mad_a a
JOIN ${case_db}.residual_mad_b b ON a.k = b.k
JOIN ${case_db}.residual_mad_c c ON b.k = c.k
WHERE a.k = 7;

SET disable_optimizer_rules = 'JoinPredicateMoveAround,JoinReorder,JoinCommutativity';

EXPLAIN VERBOSE
SELECT a.payload, b.payload, c.payload
FROM ${case_db}.residual_mad_a a
JOIN ${case_db}.residual_mad_b b ON a.k = b.k
JOIN ${case_db}.residual_mad_c c ON b.k = c.k
WHERE a.k = 7;

SET disable_optimizer_rules = '';
