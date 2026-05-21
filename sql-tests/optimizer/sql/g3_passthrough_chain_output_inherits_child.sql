-- @tags=optimizer,g3
-- Test Objective:
-- 1. Lock in the G3 contract: passthrough operators (Filter / Project)
--    report the child's actual distribution as their own output.
--    An Aggregate keyed on the same column above a Filter+Join chain
--    reuses the child's Hash distribution and skips an EXCHANGE.
-- 2. Regression guard for "passthrough output follows child".
DROP TABLE IF EXISTS ${case_db}.g3_pt_a;
DROP TABLE IF EXISTS ${case_db}.g3_pt_b;
CREATE TABLE ${case_db}.g3_pt_a (k INT, v INT);
CREATE TABLE ${case_db}.g3_pt_b (k INT, w INT);
INSERT INTO ${case_db}.g3_pt_a VALUES (1, 10), (2, 20);
INSERT INTO ${case_db}.g3_pt_b VALUES (1, 100), (2, 200);
EXPLAIN VERBOSE
SELECT a.k, SUM(a.v + b.w) AS s
FROM ${case_db}.g3_pt_a a
INNER JOIN ${case_db}.g3_pt_b b ON a.k = b.k
WHERE a.v > 0
GROUP BY a.k;
