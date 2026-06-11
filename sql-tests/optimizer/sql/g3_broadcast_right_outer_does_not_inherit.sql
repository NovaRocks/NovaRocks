-- @tags=optimizer,g3
-- Test Objective:
-- 1. Negative G3 contract: a RIGHT OUTER join's ShuffleJoin distribution
--    does NOT satisfy the Window's ShuffleAgg partition requirement.
--    The Window above must see an explicit ShuffleAgg HASH EXCHANGE rather
--    than reuse the join output.
-- 2. Regression guard against accidentally treating right-outer output as
--    preserving the left-side partition contract.
DROP TABLE IF EXISTS ${case_db}.g3_ro_left;
DROP TABLE IF EXISTS ${case_db}.date_dim;
CREATE TABLE ${case_db}.g3_ro_left  (k INT, v INT);
CREATE TABLE ${case_db}.date_dim (k INT, w INT);
INSERT INTO ${case_db}.g3_ro_left  VALUES (1, 10), (2, 20);
INSERT INTO ${case_db}.date_dim VALUES (1, 100), (3, 300);
ANALYZE TABLE ${case_db}.g3_ro_left;
ANALYZE TABLE ${case_db}.date_dim;
SET disable_optimizer_rules = 'JoinCommutativity';
EXPLAIN VERBOSE
SELECT a.k, b.w, ROW_NUMBER() OVER (PARTITION BY a.k ORDER BY a.v) AS rn
FROM ${case_db}.g3_ro_left a
RIGHT OUTER JOIN ${case_db}.date_dim b ON a.k = b.k;
SET disable_optimizer_rules = '';
