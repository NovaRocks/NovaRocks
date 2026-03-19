-- Test Objective:
-- 1. Validate multi-column Global Runtime Filter (GRF) with colocate join.
-- 2. Cover inner/left/right/full-outer join types with colocate and bucket shuffle.
-- 3. Cover cross join, CTE reuse, table function join, window function scenarios.
-- 4. Verify bucket-shuffle join with and without GROUP BY.
-- 5. Verify tablet pruning, assert nodes, and spill case.

-- query 1
-- @skip_result_check=true
set enable_multicolumn_global_runtime_filter = true;

CREATE TABLE ${case_db}.t0 (
  c0 INT NULL,
  c1 VARCHAR(20) NULL,
  c2 VARCHAR(200) NULL,
  c3 INT NULL
) ENGINE=OLAP
DUPLICATE KEY(c0, c1)
DISTRIBUTED BY HASH(c0, c1) BUCKETS 48
PROPERTIES (
  "colocate_with" = "rf_mc_cg",
  "replication_num" = "1",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);

CREATE TABLE ${case_db}.t1 (
  c0 INT NULL,
  c1 VARCHAR(20) NULL,
  c2 VARCHAR(200) NULL,
  c3 INT NULL
) ENGINE=OLAP
DUPLICATE KEY(c0, c1)
DISTRIBUTED BY HASH(c0, c1) BUCKETS 48
PROPERTIES (
  "colocate_with" = "rf_mc_cg",
  "replication_num" = "1",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);

CREATE TABLE ${case_db}.small_table (
  c0 INT NULL,
  c1 VARCHAR(20) NULL,
  c2 VARCHAR(200) NULL,
  c3 INT NULL
) ENGINE=OLAP
DUPLICATE KEY(c0, c1)
DISTRIBUTED BY HASH(c0, c1, c2) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE ${case_db}.empty_t LIKE ${case_db}.t0;

INSERT INTO ${case_db}.t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1, 40960));
INSERT INTO ${case_db}.t0 VALUES (null, null, null, null);
INSERT INTO ${case_db}.t1 SELECT * FROM ${case_db}.t0;
INSERT INTO ${case_db}.small_table SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1, 100));

-- query 2
select count(*) from ${case_db}.t0;
-- query 3
select count(*) from ${case_db}.t1;
-- query 4
select count(*) from ${case_db}.empty_t;
-- query 5
select count(*) from ${case_db}.small_table;

-- hash join cases
-- query 6
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where l.c3 > 100;
-- query 7
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l left join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where l.c3 > 100;
-- query 8
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l right join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where r.c3 < 1024;
-- query 9
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where r.c3 < 1024;
-- query 10
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where r.c3 < 1024;
-- query 11
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 join [broadcast] ${case_db}.small_table s on l.c0 = s.c0 and l.c1 = s.c1;
-- query 12
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 join [bucket] ${case_db}.small_table s on l.c0 = s.c0 and l.c1 = s.c1;
-- query 13
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 join [broadcast] ${case_db}.empty_t s on l.c0 = s.c0 and l.c1 = s.c1;

-- probe side empty
-- query 14
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where l.c3 < 0;
-- query 15
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l left join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where l.c3 < 0;
-- query 16
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l right join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where l.c3 < 0;
-- query 17
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l full outer join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where l.c3 < 0;

-- build side empty
-- query 18
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where r.c3 < 0;
-- query 19
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l left join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where r.c3 < 0;
-- query 20
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l right join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where r.c3 < 0;
-- query 21
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l full outer join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where r.c3 < 0;

-- colocate runtime filter: IN filters
-- query 22
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where r.c3 < 10;
-- colocate runtime filter: bloom filters
-- query 23
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where r.c3 < 102400 - 1;
-- colocate runtime filter: part bloom filter and part runtime in filter
-- query 24
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where r.c3 < 10000;
-- runtime filter push down to build side
-- query 25
select /*+SET_VAR(low_cardinality_optimize_v2=false,global_runtime_filter_build_max_size=-1) */ ${case_db}.t0.*,${case_db}.t1.* from ${case_db}.t0 join [broadcast] ${case_db}.small_table t2 on ${case_db}.t0.c0=t2.c0 join [colocate] ${case_db}.t1 on ${case_db}.t1.c1=${case_db}.t0.c1 and ${case_db}.t1.c0 =${case_db}.t0.c0 and ${case_db}.t1.c2 = t2.c2 where ${case_db}.t1.c3 < 10 order by 1,2,3,4,5,6,7,8;
-- query 26
select /*+SET_VAR(low_cardinality_optimize_v2=false,global_runtime_filter_build_max_size=-1) */ ${case_db}.t0.*,${case_db}.t1.* from ${case_db}.t0 join [bucket] ${case_db}.small_table t2 on ${case_db}.t0.c0=t2.c0 join [colocate] ${case_db}.t1 on ${case_db}.t1.c1=${case_db}.t0.c1 and ${case_db}.t1.c0 =${case_db}.t0.c0 and ${case_db}.t1.c2 = t2.c2 where ${case_db}.t1.c3 < 10 order by 1,2,3,4,5,6,7,8;

-- null equality
-- query 27
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 <=> r.c0 and l.c1 <=> r.c1 where r.c3 < 10;

-- cross join: in colocate group upper
-- query 28
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 join [broadcast] ${case_db}.small_table t3 where r.c3 < 10 and t3.c1 < 3;
-- cross join: in colocate group lower
-- query 29
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l join [broadcast] ${case_db}.small_table t3 join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where r.c3 < 10 and t3.c1 < 3;
-- cross join with runtime filter
-- query 30
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l join [broadcast] ${case_db}.small_table t3 join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where r.c3 < 10 and t3.c1 = 3;
-- query 31
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 join [broadcast] ${case_db}.small_table t3 where r.c3 < 10 and t3.c1 = 3;

-- CTE
-- query 32
with agged_table as (select distinct c0, c1 from ${case_db}.t0) select /*+SET_VAR(cbo_cte_reuse_rate=0)*/ count(*), sum(l.c0), sum(r.c0), sum(l.c1), sum(r.c1) from agged_table l join [colocate] agged_table r on l.c0 = r.c0 and l.c1 = r.c1;
-- query 33
with agged_table as (select distinct c0, c1 from ${case_db}.t0) select count(*), sum(l.c0), sum(r.c0), sum(l.c1), sum(r.c1) from agged_table l join [colocate] agged_table r on l.c0 = r.c0 and l.c1 = r.c1;
-- query 34
with agged_table as (select distinct c0, c1 from ${case_db}.t0) select count(*), sum(l.c0), sum(r.c0), sum(l.c1), sum(r.c1) from agged_table l join [colocate] ${case_db}.t0 r on l.c0 = r.c0 and l.c1 = r.c1;
-- query 35
with agged_table as (select distinct c0, c1 from ${case_db}.t0) select count(*), sum(l.c0), sum(r.c0), sum(l.c1), sum(r.c1) from agged_table l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1;

-- TableFunction
-- query 36
with agged_table as (select distinct c0, c1 from ${case_db}.t0) select count(*), sum(l.c0), sum(l.c1) from agged_table l join [broadcast] TABLE(generate_series(1, 100)) r on l.c0 = r.generate_series;
-- query 37
with agged_table as (select distinct c0, c1 from ${case_db}.t0) select count(*), sum(l.c0), sum(l.c1) from agged_table l join [bucket] TABLE(generate_series(1, 100)) r on l.c0 = r.generate_series;

-- bucket shuffle with group by
-- query 38
with agged_table as (select distinct c0, c1 from ${case_db}.t0) select /*+SET_VAR(cbo_cte_reuse_rate=-1)*/ count(*), sum(l.c0), sum(r.c0), sum(l.c1), sum(r.c1) from agged_table l join [bucket] agged_table r on l.c0 = r.c0 and l.c1 = r.c1;
-- query 39
with agged_table as (select distinct c0, c1 from ${case_db}.t0) select /*+SET_VAR(cbo_cte_reuse_rate=-1)*/ count(*), sum(l.c0), sum(r.c0), sum(l.c1), sum(r.c1) from agged_table l right join [bucket] agged_table r on l.c0 = r.c0 and l.c1 = r.c1;
-- query 40
with agged_table as (select distinct c0, c1 from ${case_db}.t0) select /*+SET_VAR(cbo_cte_reuse_rate=-1)*/ count(*), sum(l.c0), sum(r.c0), sum(l.c1), sum(r.c1) from agged_table l left join [bucket] agged_table r on l.c0 = r.c0 and l.c1 = r.c1;
-- query 41
with flat_table as (select c0, c1 from ${case_db}.t0) select /*+SET_VAR(cbo_cte_reuse_rate=-1)*/ l.c0, l.c1, count(*) from flat_table l join [bucket] flat_table r on l.c0 = r.c0 and l.c1 = r.c1 group by 1,2 order by 1,2 limit 10000,2;
-- query 42
with flat_table as (select c0, c1 from ${case_db}.t0) select /*+SET_VAR(cbo_cte_reuse_rate=-1)*/ l.c0, l.c1, count(*) from flat_table l right join [bucket] flat_table r on l.c0 = r.c0 and l.c1 = r.c1 group by 1,2 order by 1,2 limit 10000,2;
-- query 43
with flat_table as (select c0, c1 from ${case_db}.t0) select /*+SET_VAR(cbo_cte_reuse_rate=-1)*/ l.c0, l.c1, count(*) from flat_table l left join [bucket] flat_table r on l.c0 = r.c0 and l.c1 = r.c1 group by 1,2 order by 1,2 limit 10000,2;

-- bucket shuffle join with runtime filter
-- query 44
with agged_table as (select distinct c0, c1, c3 from ${case_db}.t0) select /*+SET_VAR(cbo_cte_reuse_rate=-1)*/ count(*), sum(l.c0), sum(r.c0), sum(l.c1), sum(r.c1) from agged_table l join [bucket] agged_table r on l.c0 = r.c0 and l.c1 = r.c1 where r.c3 < 100;

-- normal bucket shuffle join
-- query 45
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l join [bucket] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1;
-- query 46
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l right join [bucket] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1;
-- query 47
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1), count(s.c0) from ${case_db}.t0 l join [bucket] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 join [bucket] ${case_db}.small_table s on l.c0 = s.c0 and l.c1 = s.c1;
-- query 48
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1), count(s.c0) from ${case_db}.t0 l left join [bucket] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 join [bucket] ${case_db}.small_table s on l.c0 = s.c0 and l.c1 = s.c1;
-- query 49
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1), count(s.c0) from ${case_db}.t0 l join [bucket] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 join [broadcast] ${case_db}.small_table s on l.c0 = s.c0 and l.c1 = s.c1;

-- colocate join with bucket shuffle group by
-- query 50
with agged_table as (select distinct c0, c1 from ${case_db}.t0) select /*+SET_VAR(cbo_cte_reuse_rate=-1)*/ count(l.c0), count(l.c1) from ${case_db}.t0 l join [colocate] (select l.c0, r.c1 from agged_table l join [bucket] ${case_db}.t0 r on l.c0=r.c0 and l.c1 = r.c1) r on l.c0=r.c0 and l.c1 = r.c1;
-- query 51
with agged_table as (select distinct c0, c1 from ${case_db}.t0) select /*+SET_VAR(cbo_cte_reuse_rate=-1)*/ count(l.c0), count(l.c1) from ${case_db}.t0 l join [colocate] (select l.c0, r.c1 from agged_table l right join [bucket] ${case_db}.t0 r on l.c0=r.c0 and l.c1 = r.c1) r on l.c0=r.c0 and l.c1 = r.c1;

-- multi fragment: grouping sets
-- query 52
select count(*), sum(c0), sum(c1) from (select l.c0, l.c1 from (select c0, c1 from ${case_db}.t0 group by rollup (c0, c1)) l join ${case_db}.t1 r on l.c0 = r.c0 and r.c1 = l.c1) tb;

-- WITH TOPN
-- @order_sensitive=true
-- query 53
select l.c0, l.c1, r.c1 from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 order by 1,2,3 limit 10000, 10;

-- WITH WINDOW FUNCTION (not in the same fragment)
-- query 54
select count(*) from (select l.c0, l.c1, r.c1, row_number() over () from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 order by 1,2,3 limit 10) tb;
-- @order_sensitive=true
-- query 55
select l.c0, l.c1, r.c1, row_number() over (partition by l.c0) from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 order by 1,2,3 limit 10;
-- @order_sensitive=true
-- query 56
select l.c0, l.c1, r.c1, row_number() over (partition by l.c0, l.c1) from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 order by 1,2,3 limit 10;

-- WITH PARTITION-TOP-N
-- query 57
select count(*), count(lc0), count(lc1), count(rc1) from (select l.c0 lc0, l.c1 lc1, r.c1 rc1, row_number() over () rn from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1)tb where rn < 10;
-- query 58
select count(*), sum(lc0), sum(lc1), sum(rc1) from (select l.c0 lc0, l.c1 lc1, r.c1 rc1, row_number() over (partition by l.c0) rn from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1)tb where rn < 10;
-- query 59
select count(*), sum(lc0), sum(lc1), sum(rc1) from (select l.c0 lc0, l.c1 lc1, r.c1 rc1, row_number() over (partition by l.c0, l.c1) rn from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1)tb where rn < 10;

-- WITH PARTITION-TOP-N and AGG
-- query 60
select count(*) from (select c0l, c1l from (select l.c0 c0l, l.c1 c1l, r.c1 c1r, row_number() over () rn from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1) tb where rn < 10 group by 1,2 order by 1,2 limit 2000, 1) tb;
-- @order_sensitive=true
-- query 61
select c0l, c1l from (select l.c0 c0l, l.c1 c1l, r.c1 c1r, row_number() over (partition by l.c0) rn from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1) tb where rn < 10 group by 1,2 order by 1,2 limit 2000, 1;
-- @order_sensitive=true
-- query 62
select c0l, c1l from (select l.c0 c0l, l.c1 c1l, r.c1 c1r, row_number() over (partition by l.c0, l.c1) rn from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1) tb where rn < 10 group by 1,2 order by 1,2 limit 2000, 1;

-- assert nodes
-- @order_sensitive=true
-- query 63
select c0,c1 in (select c1 from ${case_db}.t1 where c0 = 10) from (select l.c0, r.c1 from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1) tb order by 1, 2 limit 10000, 1;
-- @order_sensitive=true
-- query 64
select c0,c1 = (select c1 from ${case_db}.t1 where c0 = 10) from (select l.c0, r.c1 from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1) tb order by 1, 2 limit 10000, 1;

-- tablet prune (prune left table)
-- query 65
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where l.c0=1 and l.c1=1;
-- query 66
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l left join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where l.c0=1 and l.c1=1;
-- query 67
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l right join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where l.c0=1 and l.c1=1;
-- query 68
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l full join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where l.c0=1 and l.c1=1;

-- tablet prune (prune right table)
-- query 69
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where r.c0=1 and r.c1=1;
-- query 70
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l left join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where r.c0=1 and r.c1=1;
-- query 71
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l right join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where r.c0=1 and r.c1=1;
-- query 72
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l full join [colocate] ${case_db}.t1 r on l.c0 = r.c0 and l.c1 = r.c1 where r.c0=1 and r.c1=1;

-- CTE with tablet prune (prune left-side CTE)
-- query 73
with
  tx as (select c0, c1 from ${case_db}.t0 where c0 = 1 and c1 = 1),
  ty as (select c0, c1 from ${case_db}.t0 where c0 > 0)
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from tx l join [colocate] ty r on l.c0 = r.c0 and l.c1 = r.c1;
-- query 74
with
  tx as (select c0, c1 from ${case_db}.t0 where c0 = 1 and c1 = 1),
  ty as (select c0, c1 from ${case_db}.t0 where c0 > 0)
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from tx l left join [colocate] ty r on l.c0 = r.c0 and l.c1 = r.c1;
-- query 75
with
  tx as (select c0, c1 from ${case_db}.t0 where c0 = 1 and c1 = 1),
  ty as (select c0, c1 from ${case_db}.t0 where c0 > 0)
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from tx l right join [colocate] ty r on l.c0 = r.c0 and l.c1 = r.c1;
-- query 76
with
  tx as (select c0, c1 from ${case_db}.t0 where c0 = 1 and c1 = 1),
  ty as (select c0, c1 from ${case_db}.t0 where c0 > 0)
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from tx l full join [colocate] ty r on l.c0 = r.c0 and l.c1 = r.c1;

-- CTE with tablet prune (prune right-side CTE)
-- query 77
with
  tx as (select c0, c1 from ${case_db}.t0),
  ty as (select c0, c1 from ${case_db}.t0 where c0 = 1 and c1 = 1)
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from tx l join [colocate] ty r on l.c0 = r.c0 and l.c1 = r.c1;
-- query 78
with
  tx as (select c0, c1 from ${case_db}.t0),
  ty as (select c0, c1 from ${case_db}.t0 where c0 = 1 and c1 = 1)
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from tx l left join [colocate] ty r on l.c0 = r.c0 and l.c1 = r.c1;
-- query 79
with
  tx as (select c0, c1 from ${case_db}.t0),
  ty as (select c0, c1 from ${case_db}.t0 where c0 = 1 and c1 = 1)
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from tx l right join [colocate] ty r on l.c0 = r.c0 and l.c1 = r.c1;
-- query 80
with
  tx as (select c0, c1 from ${case_db}.t0),
  ty as (select c0, c1 from ${case_db}.t0 where c0 = 1 and c1 = 1)
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from tx l full join [colocate] ty r on l.c0 = r.c0 and l.c1 = r.c1;

-- spill case
set enable_spill = true;
-- query 81
select count(l.c0), avg(l.c0), count(l.c1), count(l.c0), count(r.c1) from ${case_db}.t0 l join [colocate] ${case_db}.t1 r on l.c0 = r.c0 join [bucket] ${case_db}.small_table s on l.c0 = s.c0 and l.c1 = s.c1;
