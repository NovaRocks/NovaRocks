-- @tags=join,null_aware_anti
-- Test Objective:
-- 1. Validate NULL-aware anti join (NOT IN subquery) semantics with nullable and non-nullable columns.
-- 2. Verify correlated NOT IN with = and != predicates.
-- 3. Test NOT EXISTS semantics equivalence.
-- 4. Validate multi-column NOT IN with large generated datasets.
-- 5. Test NOT IN against build tables with NULL values and various constant probe values.
-- Test Flow:
-- 1. Create lineitem (non-nullable keys) and lineitem_nullable (nullable keys).
-- 2. Test NOT IN / IN subqueries with correlated predicates.
-- 3. Create aware3 (4096 rows, some with NULLs) and build1/build2 (small build tables).
-- 4. Test NOT IN with constants, correlated predicates, and NULL build-side values.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.lineitem (
  `l_orderkey` int(11) NOT NULL,
  `l_partkey` int(11) NOT NULL,
  `l_suppkey` int(11)
) ENGINE=OLAP
DUPLICATE KEY(`l_orderkey`)
DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

-- query 2
-- @skip_result_check=true
insert into ${case_db}.lineitem values (1,1,1),(1,2,1),(1,3,2),(11,1,11),(11,2,1),(2,3,2),(2,3,null);

-- NOT IN with correlated = predicate on non-nullable table
-- query 3
-- @order_sensitive=true
select * from ${case_db}.lineitem l1 where l1.l_orderkey not in ( select l3.l_orderkey from ${case_db}.lineitem l3 where l3.l_suppkey = l1.l_suppkey ) order by 1,2,3;

-- NOT IN with correlated != predicate on non-nullable table
-- query 4
-- @order_sensitive=true
select * from ${case_db}.lineitem l1 where l1.l_orderkey not in ( select l3.l_orderkey from ${case_db}.lineitem l3 where l3.l_suppkey != l1.l_suppkey ) order by 1,2,3;

-- query 5
-- @skip_result_check=true
CREATE TABLE ${case_db}.lineitem_nullable (
  `l_orderkey` int(11),
  `l_partkey` int(11),
  `l_suppkey` int(11)
) ENGINE=OLAP
DUPLICATE KEY(`l_orderkey`)
DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

-- query 6
-- @skip_result_check=true
insert into ${case_db}.lineitem_nullable values (1,1,1),(1,2,1),(1,3,2),(11,1,11),(11,2,1),(2,3,2),(2,3,null),(null,null,null);

-- NOT IN uncorrelated on nullable table (should return empty)
-- query 7
-- @order_sensitive=true
select * from ${case_db}.lineitem_nullable l1 where l1.l_orderkey not in ( select l3.l_orderkey from ${case_db}.lineitem_nullable l3 ) order by 1,2,3;

-- IN uncorrelated on nullable table
-- query 8
-- @order_sensitive=true
select * from ${case_db}.lineitem_nullable l1 where l1.l_orderkey in ( select l3.l_orderkey from ${case_db}.lineitem_nullable l3 ) order by 1,2,3;

-- NOT IN with correlated = predicate on nullable table
-- query 9
-- @order_sensitive=true
select * from ${case_db}.lineitem_nullable l1 where l1.l_orderkey not in ( select l3.l_orderkey from ${case_db}.lineitem_nullable l3 where l3.l_suppkey = l1.l_suppkey ) order by 1,2,3;

-- NOT IN with correlated != predicate on nullable table
-- query 10
-- @order_sensitive=true
select * from ${case_db}.lineitem_nullable l1 where l1.l_orderkey not in ( select l3.l_orderkey from ${case_db}.lineitem_nullable l3 where l3.l_suppkey != l1.l_suppkey ) order by 1,2,3;

-- NOT EXISTS with correlated predicates on nullable table
-- query 11
-- @order_sensitive=true
select * from ${case_db}.lineitem_nullable l1 where not exists ( select 1 from ${case_db}.lineitem_nullable l3 where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey != l1.l_suppkey) order by 1,2,3;

-- query 12
-- @skip_result_check=true
set pipeline_dop = 1;

-- multi-column NOT IN with large generated dataset (int pair)
-- query 13
select count(*) from (SELECT * from (SELECT if (generate_series <= 1000, null, generate_series) x0 FROM TABLE(generate_series(1, 8192))) t where (x0, x0 + 1) not in ( select l3.l_orderkey, l3.l_orderkey + 1 from ${case_db}.lineitem l3 ) order by 1) t;

-- multi-column NOT IN with large generated dataset (int + string pair)
-- query 14
select count(*) from (SELECT * from (SELECT if (generate_series <= 1000, null, generate_series) x0 FROM TABLE(generate_series(1, 8192))) t where (x0, concat("l", x0)) not in ( select l3.l_orderkey, concat("l", l_orderkey) from ${case_db}.lineitem l3 ) order by 1) t;

-- query 15
-- @skip_result_check=true
CREATE TABLE ${case_db}.aware3 (
  `k1` int(11),
  `k2` int(11),
  `k3` int(11)
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

-- query 16
-- @skip_result_check=true
insert into ${case_db}.aware3 SELECT generate_series, if(generate_series<4095, 1, null), if(generate_series<4095, generate_series, null) FROM TABLE(generate_series(1, 4096));

-- query 17
-- @skip_result_check=true
CREATE TABLE ${case_db}.build1 (
  `k1` int(11),
  `k2` int(11),
  `k3` int(11)
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

-- query 18
-- @skip_result_check=true
insert into ${case_db}.build1 values (1,1,1),(1,1,1),(1,1,1),(1,1,1),(1,1,1),(1,1,1),(1,1,1),(2,2,2),(3,3,3),(4,4,4);

-- NOT IN with correlated coalesce predicate
-- query 19
select count(*) from ${case_db}.aware3 l where l.k2 not in (select k2 from ${case_db}.build1 r where r.k3 = coalesce(l.k3, 2));

-- constant 1 NOT IN (non-matching build)
-- query 20
select count(*) from ${case_db}.aware3 l where 1 not in (select k2 from ${case_db}.build1 r);

-- constant 5 NOT IN (non-matching build)
-- query 21
select count(*) from ${case_db}.aware3 l where 5 not in (select k2 from ${case_db}.build1 r);

-- null NOT IN (should return 0)
-- query 22
select count(*) from ${case_db}.aware3 l where null not in (select k2 from ${case_db}.build1 r);

-- constant 1 NOT IN with correlated != predicate
-- query 23
select count(*) from ${case_db}.aware3 l where 1 not in (select k2 from ${case_db}.build1 r where r.k3 != l.k3);

-- constant 1000 NOT IN with correlated != predicate
-- query 24
select count(*) from ${case_db}.aware3 l where 1000 not in (select k2 from ${case_db}.build1 r where r.k3 != l.k3);

-- query 25
-- @skip_result_check=true
CREATE TABLE ${case_db}.build2 (
  `k1` int(11),
  `k2` int(11),
  `k3` int(11)
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

-- query 26
-- @skip_result_check=true
insert into ${case_db}.build2 values (1,1,1),(NULL,NULL,NULL);

-- constant 1 NOT IN build with NULL row
-- query 27
select count(*) from ${case_db}.aware3 l where 1 not in (select k2 from ${case_db}.build2 r);

-- null NOT IN build with NULL row
-- query 28
select count(*) from ${case_db}.aware3 l where null not in (select k2 from ${case_db}.build2 r);

-- constant 5 NOT IN build with NULL row (should return 0 due to NULL)
-- query 29
select count(*) from ${case_db}.aware3 l where 5 not in (select k2 from ${case_db}.build2 r);

-- NOT IN with correlated = on build1
-- query 30
select count(*) from ${case_db}.aware3 l where l.k2 not in (select k2 from ${case_db}.build1 r where r.k3 = l.k3);

-- constant 1 NOT IN with correlated != on build1
-- query 31
select count(*) from ${case_db}.aware3 l where 1 not in (select k2 from ${case_db}.build1 r where r.k3 != l.k3);

-- NOT IN with correlated = on build2
-- query 32
select count(*) from ${case_db}.aware3 l where l.k2 not in (select k2 from ${case_db}.build2 r where r.k3 = l.k3);

-- constant 1 NOT IN with correlated != on build2
-- query 33
select count(*) from ${case_db}.aware3 l where 1 not in (select k2 from ${case_db}.build2 r where r.k3 != l.k3);
