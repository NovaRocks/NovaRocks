-- Test Objective:
-- 1. Validate MV rewrite can normalize OR predicates into subsets that are still satisfiable by the MV predicate.
-- 2. Cover same-column OR pruning, mixed-column OR rewrite, and AND-of-OR predicate compensation.

-- query 1
create database db_${uuid0};

-- query 2
use db_${uuid0};

-- query 3
CREATE TABLE lineorder (
  lo_orderkey int,
  lo_linenumber int,
  lo_custkey int,
  lo_quantity int,
  lo_revenue int
)
ENGINE=OLAP
DUPLICATE KEY(lo_orderkey)
DISTRIBUTED BY HASH(lo_orderkey) BUCKETS 2
PROPERTIES ("replication_num" = "1");

-- query 4
insert into lineorder values
  (10001, 1, 1, 10, 1000),
  (10002, 2, 2, 20, 2000),
  (10003, 1, 3, 30, 3000),
  (10004, 1, null, 40, 4000),
  (10005, 1, 5, 50, 5000),
  (10006, 3, 6, 60, 6000);

-- query 5
-- Scenario 1: an MV defined with a disjunction on the same column should still satisfy a stricter range predicate.
create materialized view mv_lineorder_or_range
distributed by hash(lo_orderkey) buckets 10
refresh manual
as
select lo_orderkey, lo_linenumber, lo_quantity, lo_revenue, lo_custkey
from lineorder
where lo_orderkey > 10003 or lo_orderkey < 10002;

-- query 6
refresh materialized view mv_lineorder_or_range with sync mode;

-- query 7
-- @result_contains=mv_lineorder_or_range
SET enable_materialized_view_rewrite = true;
EXPLAIN
select lo_orderkey, lo_linenumber, lo_quantity, lo_revenue, lo_custkey
from lineorder
where lo_orderkey > 10004
order by lo_orderkey;

-- query 8
select lo_orderkey, lo_linenumber, lo_quantity, lo_revenue, lo_custkey
from lineorder
where lo_orderkey > 10004
order by lo_orderkey;

-- query 9
drop materialized view mv_lineorder_or_range;

-- query 10
-- Scenario 2: an MV defined with OR predicates across different columns should satisfy both the direct OR query and single-side pruning.
create materialized view mv_lineorder_or_mixed
distributed by hash(lo_orderkey) buckets 10
refresh manual
as
select lo_orderkey, lo_linenumber, lo_quantity, lo_revenue, lo_custkey
from lineorder
where lo_orderkey > 10003 or lo_linenumber < 2;

-- query 11
refresh materialized view mv_lineorder_or_mixed with sync mode;

-- query 12
-- @result_contains=mv_lineorder_or_mixed
SET enable_materialized_view_rewrite = true;
EXPLAIN
select lo_orderkey, lo_linenumber, lo_quantity, lo_revenue, lo_custkey
from lineorder
where lo_linenumber < 2
order by lo_orderkey;

-- query 13
select lo_orderkey, lo_linenumber, lo_quantity, lo_revenue, lo_custkey
from lineorder
where lo_linenumber < 2
order by lo_orderkey;

-- query 14
-- @result_contains=mv_lineorder_or_mixed
SET enable_materialized_view_rewrite = true;
EXPLAIN
select lo_orderkey, lo_linenumber, lo_quantity, lo_revenue, lo_custkey
from lineorder
where lo_orderkey > 10003 or lo_linenumber < 2
order by lo_orderkey;

-- query 15
select lo_orderkey, lo_linenumber, lo_quantity, lo_revenue, lo_custkey
from lineorder
where lo_orderkey > 10003 or lo_linenumber < 2
order by lo_orderkey;

-- query 16
drop materialized view mv_lineorder_or_mixed;

-- query 17
-- Scenario 3: CNF/DNF normalization should still let the optimizer pick an MV whose predicate is an AND of OR clauses.
create materialized view mv_lineorder_and_of_ors
distributed by hash(lo_orderkey) buckets 10
refresh manual
as
select lo_orderkey, lo_linenumber, lo_quantity, lo_revenue, lo_custkey
from lineorder
where (lo_orderkey > 10003 or lo_linenumber > 2)
  and (lo_orderkey < 10002 or lo_linenumber < 2);

-- query 18
refresh materialized view mv_lineorder_and_of_ors with sync mode;

-- query 19
-- @result_contains=mv_lineorder_and_of_ors
SET enable_materialized_view_rewrite = true;
EXPLAIN
select lo_orderkey, lo_linenumber, lo_quantity, lo_revenue, lo_custkey
from lineorder
where lo_orderkey > 10003 and lo_linenumber < 2
order by lo_orderkey;

-- query 20
select lo_orderkey, lo_linenumber, lo_quantity, lo_revenue, lo_custkey
from lineorder
where lo_orderkey > 10003 and lo_linenumber < 2
order by lo_orderkey;

-- query 21
drop database db_${uuid0} force;
