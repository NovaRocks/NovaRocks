-- @tags=optimizer,g3
-- Test Objective:
-- 1. Lock in the G3 contract: a BROADCAST Inner Join above a SHUFFLE
--    Hash Join inherits the SHUFFLE join's natural Hash([eq_keys])
--    output. Downstream Hash-required operators (here a Window keyed on
--    one of those columns) reuse the distribution and skip an EXCHANGE.
-- 2. Regression guard for "Broadcast preserves left output" + the
--    `satisfyContainAll` superset rule.
DROP TABLE IF EXISTS ${case_db}.g3_bj_left;
DROP TABLE IF EXISTS ${case_db}.g3_bj_right;
DROP TABLE IF EXISTS ${case_db}.g3_bj_small;
CREATE TABLE ${case_db}.g3_bj_left  (k INT, v INT);
CREATE TABLE ${case_db}.g3_bj_right (k INT, w INT);
CREATE TABLE ${case_db}.g3_bj_small (s INT, x INT);
INSERT INTO ${case_db}.g3_bj_left  VALUES (1, 10), (2, 20);
INSERT INTO ${case_db}.g3_bj_right VALUES (1, 100), (2, 200);
INSERT INTO ${case_db}.g3_bj_small VALUES (1, 1000);
-- Inner BROADCAST(small) join above an INNER SHUFFLE join keyed on a.k.
-- Window keyed on a.k reuses the join chain's Hash([a.k, b.k]) without
-- a redundant HASH EXCHANGE above the Broadcast.
EXPLAIN VERBOSE
SELECT a.k, a.v, b.w, ROW_NUMBER() OVER (PARTITION BY a.k ORDER BY a.v) AS rn
FROM ${case_db}.g3_bj_left  a
INNER JOIN ${case_db}.g3_bj_right b ON a.k = b.k
INNER JOIN ${case_db}.g3_bj_small s ON a.k = s.s;
