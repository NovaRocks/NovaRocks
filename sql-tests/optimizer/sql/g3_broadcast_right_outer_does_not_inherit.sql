-- @tags=optimizer,g3
-- Test Objective:
-- 1. Negative G3 contract: a BROADCAST RIGHT OUTER join does NOT
--    inherit the left child's distribution because unmatched-right
--    NULL rows break the partition contract. The Window above must
--    see an explicit HASH EXCHANGE rather than reuse the join output.
-- 2. Regression guard against accidentally extending preserves_left
--    to right-outer.
DROP TABLE IF EXISTS ${case_db}.g3_ro_left;
DROP TABLE IF EXISTS ${case_db}.g3_ro_right;
CREATE TABLE ${case_db}.g3_ro_left  (k INT, v INT);
CREATE TABLE ${case_db}.g3_ro_right (k INT, w INT);
INSERT INTO ${case_db}.g3_ro_left  VALUES (1, 10), (2, 20);
INSERT INTO ${case_db}.g3_ro_right VALUES (1, 100), (3, 300);
EXPLAIN VERBOSE
SELECT a.k, b.w, ROW_NUMBER() OVER (PARTITION BY a.k ORDER BY a.v) AS rn
FROM ${case_db}.g3_ro_left a
RIGHT OUTER JOIN ${case_db}.g3_ro_right b ON a.k = b.k;
