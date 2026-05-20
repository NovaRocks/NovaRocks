-- @tags=optimizer,baseline
-- Test Objective:
-- 1. Lock in the current EXPLAIN VERBOSE shape of a plain inner-equi-join.
-- 2. Failure of this case in a future PR signals a plan-shape change
--    that must be intentional and acknowledged via record mode.
DROP TABLE IF EXISTS ${case_db}.t_optimizer_baseline_a;
DROP TABLE IF EXISTS ${case_db}.t_optimizer_baseline_b;
CREATE TABLE ${case_db}.t_optimizer_baseline_a (k INT, v INT);
CREATE TABLE ${case_db}.t_optimizer_baseline_b (k INT, w INT);
INSERT INTO ${case_db}.t_optimizer_baseline_a VALUES (1, 10), (2, 20);
INSERT INTO ${case_db}.t_optimizer_baseline_b VALUES (1, 100), (2, 200);
EXPLAIN VERBOSE
SELECT a.k, a.v, b.w
FROM ${case_db}.t_optimizer_baseline_a a
INNER JOIN ${case_db}.t_optimizer_baseline_b b ON a.k = b.k;
