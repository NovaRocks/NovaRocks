-- Migrated from dev/test/sql/test_agg/R/test_agg_with_limit
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_agg_with_limit @mac
-- query 2
-- @skip_result_check=true
USE ${case_db};
create table t0 (
    c0 STRING,
    c1 STRING NOT NULL,
    c2 int,
    c3 int NOT NULL
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  100000));

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  100000));

-- query 5
-- @skip_result_check=true
USE ${case_db};
insert into t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  100000));

-- query 6
-- @skip_result_check=true
USE ${case_db};
create table t1 (
    c0 STRING NOT NULL,
    c1 STRING,
    c2 int,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');

-- query 7
-- @skip_result_check=true
USE ${case_db};
create table t2 (
    c0 int NOT NULL,
    c1 int,
    c2 string,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');

-- query 8
-- @skip_result_check=true
USE ${case_db};
create table t3 (
    c0 int,
    c1 int NOT NULL,
    c2 string,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');

-- query 9
-- @skip_result_check=true
USE ${case_db};
insert into t1 select * from t0;

-- query 10
-- @skip_result_check=true
USE ${case_db};
insert into t2 select * from t0;

-- query 11
-- @skip_result_check=true
USE ${case_db};
insert into t3 select * from t0;

-- query 12
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 limit 10), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 13
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t1 group by c0 limit 10), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 14
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t2 group by c0 limit 10), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 15
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t3 group by c0 limit 10), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 16
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 17
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t1 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 18
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t2 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 19
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t3 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 20
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c1 from t0 group by c1 limit 10), cte1 as (select avg(c3), sum(c3) sc3, c1 from t0 group by c1 ) select count(*) from (select l.sc3, l.c1 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c1 <=> r.c1) t ;

-- query 21
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c1 from t1 group by c1 limit 10), cte1 as (select avg(c3), sum(c3) sc3, c1 from t0 group by c1 ) select count(*) from (select l.sc3, l.c1 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c1 <=> r.c1) t ;

-- query 22
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c1 from t2 group by c1 limit 10), cte1 as (select avg(c3), sum(c3) sc3, c1 from t0 group by c1 ) select count(*) from (select l.sc3, l.c1 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c1 <=> r.c1) t ;

-- query 23
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c1 from t3 group by c1 limit 10), cte1 as (select avg(c3), sum(c3) sc3, c1 from t0 group by c1 ) select count(*) from (select l.sc3, l.c1 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c1 <=> r.c1) t ;

-- query 24
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 25
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t1 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 26
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t2 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 27
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t3 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 28
-- @skip_result_check=true
USE ${case_db};
set streaming_preaggregation_mode="force_streaming";

-- query 29
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 30
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t1 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 31
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t2 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 32
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t3 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 33
-- @skip_result_check=true
USE ${case_db};
create table t4 (
    c0 int,
    c1 int,
    c2 string,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');

-- query 34
-- @skip_result_check=true
USE ${case_db};
insert into t4 SELECT generate_series % 4, generate_series % 9, generate_series % 9, generate_series %9 FROM TABLE(generate_series(1,  100000));

-- query 35
-- @skip_result_check=true
USE ${case_db};
insert into t4 SELECT generate_series % 4, null, null, null FROM TABLE(generate_series(1,  100000));

-- query 36
-- @skip_result_check=true
USE ${case_db};
create table t5 (
    c0 int,
    c1 int,
    c2 string,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');

-- query 37
-- @skip_result_check=true
USE ${case_db};
insert into t5 select * from t4;

-- query 38
-- @skip_result_check=true
USE ${case_db};
set streaming_preaggregation_mode="auto";

-- query 39
USE ${case_db};
select * from (select max(c3), sum(c3) sc3, c0 from t5 group by c0 limit 10) t order by 3;

-- query 40
USE ${case_db};
select * from (select max(c3), sum(c3) sc3, c1 from t5 group by c1 limit 10) t order by 3;

-- query 41
-- @skip_result_check=true
USE ${case_db};
set streaming_preaggregation_mode="force_streaming";

-- query 42
USE ${case_db};
select * from (select max(c3), sum(c3) sc3, c0 from t5 group by c0 limit 10) t order by 3;

-- query 43
USE ${case_db};
select * from (select max(c3), sum(c3) sc3, c1 from t5 group by c1 limit 10) t order by 3;

-- query 44
-- @skip_result_check=true
USE ${case_db};
analyze full table t5;

-- query 45
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=Decode
-- @skip_result_check=true
USE ${case_db};
EXPLAIN COSTS SELECT DISTINCT c2 FROM t5;

-- query 46
USE ${case_db};
select * from (select max(c3), sum(c3) sc3, c2 from t5 group by c2 limit 10) t order by 3;

-- query 47
-- @skip_result_check=true
USE ${case_db};
set streaming_preaggregation_mode="force_streaming";

-- query 48
-- @skip_result_check=true
USE ${case_db};
create table t6 (
    c0 int,
    c1 float,
    c2 string,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 PROPERTIES('replication_num' = '1');

-- query 49
-- @skip_result_check=true
USE ${case_db};
insert into t6 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  100000));

-- query 50
USE ${case_db};
select count(*) from (select sum(c3) from t6 group by c1, c2 limit 10) t;

-- query 51
USE ${case_db};
select count(*) from (select sum(c3) from t6 group by c1, c2, c3 limit 10) t;

-- query 52
USE ${case_db};
select count(*) from (select sum(c3) from t6 group by c2, c1 limit 10) t;

-- query 53
-- @skip_result_check=true
USE ${case_db};
create table tempty (
    c0 int,
    c1 float,
    c2 string,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');

-- query 54
USE ${case_db};
select sum(c3) from tempty group by c1, c2 limit 10;

-- query 55
USE ${case_db};
select sum(c3) from tempty group by c1, c2, c3 limit 10;

-- query 56
USE ${case_db};
select sum(c3) from tempty group by c2, c1 limit 10;

-- query 57
-- @skip_result_check=true
USE ${case_db};
create table tarray (
    c0 int,
    c1 array<int>,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');

-- query 58
-- @skip_result_check=true
USE ${case_db};
insert into tarray SELECT generate_series, [generate_series], generate_series FROM TABLE(generate_series(1,  100000));

-- query 59
USE ${case_db};
select count(*) from (select sum(c3) from tarray group by c1, c3 limit 10) t;

-- query 60
USE ${case_db};
select count(*) from (select sum(c3) from tarray group by c1 limit 10) t;
