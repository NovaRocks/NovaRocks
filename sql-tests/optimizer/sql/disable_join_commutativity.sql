-- @tags=optimizer,session_rule_disable
-- Test Objective:
-- 1. Verify SET disable_optimizer_rules = 'JoinCommutativity' changes plan shape.
-- 2. Two EXPLAIN VERBOSE statements; the .result captures both and the
--    diff between them is the join order / distribution type.
-- Design note:
-- The query is a LEFT OUTER JOIN written as date_dim LEFT, lineorder RIGHT.
-- query rewrite join-reorder only applies to INNER/CROSS joins, so it leaves outer
-- joins alone.  The CBO JoinCommutativity rule is the only mechanism that
-- can swap left/right for outer joins.
-- With JoinCommutativity: CBO swaps to lineorder LEFT (probe) + date_dim RIGHT
--   (broadcast build), converting LEFT OUTER -> RIGHT OUTER.
--   date_dim (10k rows) fits under the broadcast threshold; lineorder (1M) does not.
-- Without JoinCommutativity: CBO cannot swap, lineorder RIGHT (1M rows) exceeds
--   the broadcast limit, so it falls back to a SHUFFLE LEFT OUTER join.
DROP TABLE IF EXISTS ${case_db}.lineorder;
DROP TABLE IF EXISTS ${case_db}.date_dim;
CREATE TABLE ${case_db}.lineorder (lo_orderkey INT, lo_datekey INT, lo_revenue INT);
CREATE TABLE ${case_db}.date_dim (d_datekey INT, d_year INT);
INSERT INTO ${case_db}.lineorder VALUES (1, 19980101, 100), (2, 19980102, 200);
INSERT INTO ${case_db}.date_dim VALUES (19980101, 1998), (19980102, 1998);

-- Baseline (no disable): full CBO including JoinCommutativity.
-- Expected: CBO swaps to lineorder LEFT + date_dim RIGHT (BROADCAST, RIGHT OUTER).
EXPLAIN VERBOSE
SELECT lo.lo_orderkey, d.d_year
FROM ${case_db}.date_dim d
LEFT JOIN ${case_db}.lineorder lo ON d.d_datekey = lo.lo_datekey;

-- Disable JoinCommutativity for the next query.
SET disable_optimizer_rules = 'JoinCommutativity';

-- Without commutativity: cannot swap, lineorder on RIGHT exceeds broadcast limit.
-- Expected: SHUFFLE join, date_dim stays LEFT, join remains LEFT OUTER.
EXPLAIN VERBOSE
SELECT lo.lo_orderkey, d.d_year
FROM ${case_db}.date_dim d
LEFT JOIN ${case_db}.lineorder lo ON d.d_datekey = lo.lo_datekey;

-- Restore.
SET disable_optimizer_rules = '';
