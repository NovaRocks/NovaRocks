-- @tags=optimizer,oq9,residual,lowest_cover
-- Test Objective:
-- Keep a cross-side OR residual at the lowest join that covers its columns.
DROP TABLE IF EXISTS ${case_db}.residual_lcj_a;
DROP TABLE IF EXISTS ${case_db}.residual_lcj_b;
DROP TABLE IF EXISTS ${case_db}.residual_lcj_c;
CREATE TABLE ${case_db}.residual_lcj_a (k INT, bucket INT, payload INT);
CREATE TABLE ${case_db}.residual_lcj_b (k INT, bucket INT, payload INT);
CREATE TABLE ${case_db}.residual_lcj_c (k INT, flag INT, payload INT);

SET disable_optimizer_rules = 'JoinReorder,JoinCommutativity';

EXPLAIN VERBOSE
SELECT a.payload, b.payload, c.payload
FROM ${case_db}.residual_lcj_a a
JOIN ${case_db}.residual_lcj_b b
  ON a.k = b.k AND (a.bucket = b.bucket OR a.payload = b.payload)
JOIN ${case_db}.residual_lcj_c c
  ON b.k = c.k
WHERE c.flag = 1;

SET disable_optimizer_rules = '';
