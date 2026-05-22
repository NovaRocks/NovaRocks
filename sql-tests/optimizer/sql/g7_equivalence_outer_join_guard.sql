-- @tags=optimizer,g7,equivalence_predicate,negative
-- Test Objective:
-- G7 (InnerJoinEquivalencePredicateRule) must NEVER fire for non-INNER joins.
-- For a LEFT OUTER JOIN, propagating `l.lk = 10` to the right (nullable) side
-- as `r.rk = 10` would silently drop the null-extended rows that
-- LEFT JOIN preserves.
-- This golden locks in the plan for the LEFT OUTER form so any future change
-- that accidentally relaxes the join-kind guard will produce a diff.
DROP TABLE IF EXISTS ${case_db}.g7_outer_l;
DROP TABLE IF EXISTS ${case_db}.g7_outer_r;
CREATE TABLE ${case_db}.g7_outer_l (lk BIGINT, payload BIGINT);
CREATE TABLE ${case_db}.g7_outer_r (rk BIGINT, payload BIGINT);
EXPLAIN VERBOSE
SELECT l.lk, r.rk
FROM ${case_db}.g7_outer_l l
LEFT JOIN ${case_db}.g7_outer_r r ON l.lk = r.rk AND l.lk = 10;
