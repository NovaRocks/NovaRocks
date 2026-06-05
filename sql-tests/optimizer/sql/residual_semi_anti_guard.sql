-- @tags=optimizer,oq9,residual,semi_anti_guard
-- Test Objective:
-- SEMI/ANTI join conditions stay guarded from the inner/cross-only
-- predicate move-around derivation path.
DROP TABLE IF EXISTS ${case_db}.residual_sa_l;
DROP TABLE IF EXISTS ${case_db}.residual_sa_r;
CREATE TABLE ${case_db}.residual_sa_l (k INT, flag INT, payload INT);
CREATE TABLE ${case_db}.residual_sa_r (k INT, flag INT, payload INT);

EXPLAIN VERBOSE
SELECT l.k
FROM ${case_db}.residual_sa_l l
LEFT SEMI JOIN ${case_db}.residual_sa_r r
  ON l.k = r.k AND l.flag = r.flag AND l.flag = 1;

EXPLAIN VERBOSE
SELECT l.k
FROM ${case_db}.residual_sa_l l
LEFT ANTI JOIN ${case_db}.residual_sa_r r
  ON l.k = r.k AND l.flag = r.flag AND l.flag = 1;
