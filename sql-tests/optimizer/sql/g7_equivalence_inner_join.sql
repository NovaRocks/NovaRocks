-- @tags=optimizer,g7,equivalence_predicate
-- Test Objective:
-- Lock in the current EXPLAIN VERBOSE plan shape for an INNER JOIN whose
-- ON-clause carries a literal equality on one side of the equi-join.
--
-- G7 (InnerJoinEquivalencePredicateRule) inserts a propagated
-- `r.rk = 10` filter alternative into the right child's memo group.
-- Whether that alternative is finally chosen by the cost search depends on
-- two unrelated pre-existing optimizer behaviors:
--   1. DP join reorder drops single-side literal conjuncts from JOIN
--      conditions (see `collect_join_predicates` in
--      `rbo/rules/join_reorder/reorder.rs`).
--   2. PushDownPredicate only walks INTO `Filter(Join)`, not into a Join's
--      own ON-condition conjuncts.
-- Once either of those is improved, the propagated predicate will surface in
-- this golden and the diff will signal that G7 is now end-to-end visible.
-- Until then, this case serves as a plan-shape regression guard.
DROP TABLE IF EXISTS ${case_db}.g7_l;
DROP TABLE IF EXISTS ${case_db}.g7_r;
CREATE TABLE ${case_db}.g7_l (lk BIGINT, payload BIGINT);
CREATE TABLE ${case_db}.g7_r (rk BIGINT, payload BIGINT);
EXPLAIN VERBOSE
SELECT l.lk, r.rk
FROM ${case_db}.g7_l l
JOIN ${case_db}.g7_r r ON l.lk = r.rk AND l.lk = 10;
EXPLAIN VERBOSE
SELECT l.lk, r.rk
FROM ${case_db}.g7_l l
JOIN ${case_db}.g7_r r ON l.lk = r.rk AND r.rk = 20;
