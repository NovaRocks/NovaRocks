-- Migrated from dev/test/sql/test_agg/R/test_agg_with_limit
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_test_agg_with_limit FORCE;
CREATE DATABASE sql_tests_test_agg_with_limit;
USE sql_tests_test_agg_with_limit;

-- name: test_agg_with_limit @mac
-- query 2
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
create table t0 (
    c0 STRING,
    c1 STRING NOT NULL,
    c2 int,
    c3 int NOT NULL
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');

-- query 3
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
insert into t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  100000));

-- query 4
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
insert into t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  100000));

-- query 5
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
insert into t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  100000));

-- query 6
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
create table t1 (
    c0 STRING NOT NULL,
    c1 STRING,
    c2 int,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');

-- query 7
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
create table t2 (
    c0 int NOT NULL,
    c1 int,
    c2 string,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');

-- query 8
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
create table t3 (
    c0 int,
    c1 int NOT NULL,
    c2 string,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');

-- query 9
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
insert into t1 select * from t0;

-- query 10
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
insert into t2 select * from t0;

-- query 11
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
insert into t3 select * from t0;

-- query 12
USE sql_tests_test_agg_with_limit;
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 limit 10), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 13
USE sql_tests_test_agg_with_limit;
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t1 group by c0 limit 10), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 14
USE sql_tests_test_agg_with_limit;
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t2 group by c0 limit 10), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 15
USE sql_tests_test_agg_with_limit;
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t3 group by c0 limit 10), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 16
USE sql_tests_test_agg_with_limit;
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 17
USE sql_tests_test_agg_with_limit;
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t1 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 18
USE sql_tests_test_agg_with_limit;
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t2 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 19
USE sql_tests_test_agg_with_limit;
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t3 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 20
USE sql_tests_test_agg_with_limit;
with cte0 as (select avg(c3), sum(c3) sc3, c1 from t0 group by c1 limit 10), cte1 as (select avg(c3), sum(c3) sc3, c1 from t0 group by c1 ) select count(*) from (select l.sc3, l.c1 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c1 <=> r.c1) t ;

-- query 21
USE sql_tests_test_agg_with_limit;
with cte0 as (select avg(c3), sum(c3) sc3, c1 from t1 group by c1 limit 10), cte1 as (select avg(c3), sum(c3) sc3, c1 from t0 group by c1 ) select count(*) from (select l.sc3, l.c1 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c1 <=> r.c1) t ;

-- query 22
USE sql_tests_test_agg_with_limit;
with cte0 as (select avg(c3), sum(c3) sc3, c1 from t2 group by c1 limit 10), cte1 as (select avg(c3), sum(c3) sc3, c1 from t0 group by c1 ) select count(*) from (select l.sc3, l.c1 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c1 <=> r.c1) t ;

-- query 23
USE sql_tests_test_agg_with_limit;
with cte0 as (select avg(c3), sum(c3) sc3, c1 from t3 group by c1 limit 10), cte1 as (select avg(c3), sum(c3) sc3, c1 from t0 group by c1 ) select count(*) from (select l.sc3, l.c1 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c1 <=> r.c1) t ;

-- query 24
USE sql_tests_test_agg_with_limit;
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 25
USE sql_tests_test_agg_with_limit;
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t1 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 26
USE sql_tests_test_agg_with_limit;
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t2 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 27
USE sql_tests_test_agg_with_limit;
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t3 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 28
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
set streaming_preaggregation_mode="force_streaming";

-- query 29
USE sql_tests_test_agg_with_limit;
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 30
USE sql_tests_test_agg_with_limit;
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t1 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 31
USE sql_tests_test_agg_with_limit;
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t2 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 32
USE sql_tests_test_agg_with_limit;
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t3 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 33
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
create table t4 (
    c0 int,
    c1 int,
    c2 string,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');

-- query 34
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
insert into t4 SELECT generate_series % 4, generate_series % 9, generate_series % 9, generate_series %9 FROM TABLE(generate_series(1,  100000));

-- query 35
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
insert into t4 SELECT generate_series % 4, null, null, null FROM TABLE(generate_series(1,  100000));

-- query 36
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
create table t5 (
    c0 int,
    c1 int,
    c2 string,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');

-- query 37
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
insert into t5 select * from t4;

-- query 38
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
set streaming_preaggregation_mode="auto";

-- query 39
USE sql_tests_test_agg_with_limit;
select * from (select max(c3), sum(c3) sc3, c0 from t5 group by c0 limit 10) t order by 3;

-- query 40
USE sql_tests_test_agg_with_limit;
select * from (select max(c3), sum(c3) sc3, c1 from t5 group by c1 limit 10) t order by 3;

-- query 41
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
set streaming_preaggregation_mode="force_streaming";

-- query 42
USE sql_tests_test_agg_with_limit;
select * from (select max(c3), sum(c3) sc3, c0 from t5 group by c0 limit 10) t order by 3;

-- query 43
USE sql_tests_test_agg_with_limit;
select * from (select max(c3), sum(c3) sc3, c1 from t5 group by c1 limit 10) t order by 3;

-- query 44
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
analyze full table t5;

-- query 45
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=Decode
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
EXPLAIN COSTS SELECT DISTINCT c2 FROM t5;

-- query 46
USE sql_tests_test_agg_with_limit;
select * from (select max(c3), sum(c3) sc3, c2 from t5 group by c2 limit 10) t order by 3;

-- query 47
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
set streaming_preaggregation_mode="force_streaming";

-- query 48
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
create table t6 (
    c0 int,
    c1 float,
    c2 string,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 PROPERTIES('replication_num' = '1');

-- query 49
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
insert into t6 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  100000));

-- query 50
USE sql_tests_test_agg_with_limit;
select count(*) from (select sum(c3) from t6 group by c1, c2 limit 10) t;

-- query 51
USE sql_tests_test_agg_with_limit;
select count(*) from (select sum(c3) from t6 group by c1, c2, c3 limit 10) t;

-- query 52
USE sql_tests_test_agg_with_limit;
select count(*) from (select sum(c3) from t6 group by c2, c1 limit 10) t;

-- query 53
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
create table tempty (
    c0 int,
    c1 float,
    c2 string,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');

-- query 54
USE sql_tests_test_agg_with_limit;
select sum(c3) from tempty group by c1, c2 limit 10;

-- query 55
USE sql_tests_test_agg_with_limit;
select sum(c3) from tempty group by c1, c2, c3 limit 10;

-- query 56
USE sql_tests_test_agg_with_limit;
select sum(c3) from tempty group by c2, c1 limit 10;

-- query 57
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
create table tarray (
    c0 int,
    c1 array<int>,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');

-- query 58
-- @skip_result_check=true
USE sql_tests_test_agg_with_limit;
insert into tarray SELECT generate_series, [generate_series], generate_series FROM TABLE(generate_series(1,  100000));

-- query 59
USE sql_tests_test_agg_with_limit;
select count(*) from (select sum(c3) from tarray group by c1, c3 limit 10) t;

-- query 60
USE sql_tests_test_agg_with_limit;
select count(*) from (select sum(c3) from tarray group by c1 limit 10) t;
