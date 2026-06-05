-- @tags=optimizer,oq9,residual,outer_guard
-- Test Objective:
-- LEFT OUTER JOIN keeps preserved-side ON predicates in the join residual
-- instead of pushing them below the preserved side scan.
DROP TABLE IF EXISTS ${case_db}.residual_outer_l;
DROP TABLE IF EXISTS ${case_db}.residual_outer_r;
CREATE TABLE ${case_db}.residual_outer_l (k INT, flag INT, payload INT);
CREATE TABLE ${case_db}.residual_outer_r (k INT, payload INT);

EXPLAIN VERBOSE
SELECT l.k, r.k
FROM ${case_db}.residual_outer_l l
LEFT JOIN ${case_db}.residual_outer_r r
  ON l.k = r.k AND l.flag = 1;
