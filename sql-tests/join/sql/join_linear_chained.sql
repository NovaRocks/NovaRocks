-- @tags=join,linear_chained
-- Test Objective:
-- 1. Validate hash join correctness for linear-chained (direct-mapping) join patterns.
-- 2. Test inner join, left semi join, and left outer join with int, bigint, and string keys.
-- 3. Verify join with USING clause, multi-key joins, expression keys, and broadcast hints.
-- 4. Ensure correct row counts with various filter predicates (k1 % 2 = 0, k1 % 100 = 0).
-- Test Flow:
-- 1. Set session variables to disable range direct mapping and partition hash join.
-- 2. Create helper tables to generate 2.56M rows.
-- 3. Create t1 with int, bigint, and string columns.
-- 4. Execute inner join, left semi join, and left outer join variants.

-- query 1
-- @skip_result_check=true
set enable_hash_join_range_direct_mapping_opt = false;

-- query 2
-- @skip_result_check=true
set enable_partition_hash_join = false;

-- query 3
-- @skip_result_check=true
CREATE TABLE ${case_db}.__row_util_base (
  k1 bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 32
PROPERTIES (
    "replication_num" = "1"
);

-- query 4
-- @skip_result_check=true
insert into ${case_db}.__row_util_base select generate_series from TABLE(generate_series(0, 10000 - 1));

-- query 5
-- @skip_result_check=true
insert into ${case_db}.__row_util_base select * from ${case_db}.__row_util_base;

-- query 6
-- @skip_result_check=true
insert into ${case_db}.__row_util_base select * from ${case_db}.__row_util_base;

-- query 7
-- @skip_result_check=true
insert into ${case_db}.__row_util_base select * from ${case_db}.__row_util_base;

-- query 8
-- @skip_result_check=true
insert into ${case_db}.__row_util_base select * from ${case_db}.__row_util_base;

-- query 9
-- @skip_result_check=true
insert into ${case_db}.__row_util_base select * from ${case_db}.__row_util_base;

-- query 10
-- @skip_result_check=true
insert into ${case_db}.__row_util_base select * from ${case_db}.__row_util_base;

-- query 11
-- @skip_result_check=true
insert into ${case_db}.__row_util_base select * from ${case_db}.__row_util_base;

-- query 12
-- @skip_result_check=true
insert into ${case_db}.__row_util_base select * from ${case_db}.__row_util_base;

-- query 13
-- @skip_result_check=true
CREATE TABLE ${case_db}.__row_util (
  idx bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(`idx`)
DISTRIBUTED BY HASH(`idx`) BUCKETS 32
PROPERTIES (
    "replication_num" = "1"
);

-- query 14
-- @skip_result_check=true
insert into ${case_db}.__row_util select row_number() over() as idx from ${case_db}.__row_util_base;

-- query 15
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
    k1 bigint NULL,
    c_int int NULL,
    c_bigint bigint NULL,
    c_string STRING
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 96
PROPERTIES (
    "replication_num" = "1"
);

-- query 16
-- @skip_result_check=true
insert into ${case_db}.t1
select
    idx,
    idx,
    idx,
    uuid()
from ${case_db}.__row_util;

-- Inner join tests

-- inner join on c_int with expression key
-- query 17
select count(tt1.k1), count(tt2.k1) from ${case_db}.t1 tt1 join ${case_db}.t1 tt2 on tt1.c_int=tt2.c_int and tt1.c_int+1=tt2.c_int+1;

-- inner join on c_int with expression key and filter
-- query 18
select count(tt1.k1), count(tt2.k1) from ${case_db}.t1 tt1 join ${case_db}.t1 tt2 on tt1.c_int=tt2.c_int and tt1.c_int+1=tt2.c_int+1 where tt1.k1 % 2 = 0;

-- inner join on c_int + c_bigint
-- query 19
select count(tt1.k1), count(tt2.k1) from ${case_db}.t1 tt1 join ${case_db}.t1 tt2 on tt1.c_int=tt2.c_int and tt1.c_bigint=tt2.c_bigint;

-- inner join using(c_int)
-- query 20
select count(tt1.k1), count(tt2.k1) from ${case_db}.t1 tt1 join ${case_db}.t1 tt2 using(c_int);

-- inner join using(c_int) with filter % 2
-- query 21
select count(tt1.k1), count(tt2.k1) from ${case_db}.t1 tt1 join ${case_db}.t1 tt2 using(c_int) where tt1.k1 % 2 = 0;

-- inner join using(c_int) with filter % 100
-- query 22
select count(tt1.k1), count(tt2.k1) from ${case_db}.t1 tt1 join ${case_db}.t1 tt2 using(c_int) where tt1.k1 % 100 = 0;

-- inner join using(c_bigint)
-- query 23
select count(tt1.k1), count(tt2.k1) from ${case_db}.t1 tt1 join ${case_db}.t1 tt2 using(c_bigint);

-- inner join using(c_bigint) with filter % 2
-- query 24
select count(tt1.k1), count(tt2.k1) from ${case_db}.t1 tt1 join ${case_db}.t1 tt2 using(c_bigint) where tt1.k1 % 2 = 0;

-- inner join using(c_bigint) with filter % 100
-- query 25
select count(tt1.k1), count(tt2.k1) from ${case_db}.t1 tt1 join ${case_db}.t1 tt2 using(c_bigint) where tt1.k1 % 100 = 0;

-- inner join using(c_string)
-- query 26
select count(tt1.c_string) from ${case_db}.t1 tt1 join ${case_db}.t1 tt2 using(c_string);

-- inner join using(c_string) with filter % 2
-- query 27
select count(tt1.c_string) from ${case_db}.t1 tt1 join ${case_db}.t1 tt2 using(c_string) where tt1.k1 % 2 = 0;

-- inner join using(c_string) with filter % 100
-- query 28
select count(tt1.c_string) from ${case_db}.t1 tt1 join ${case_db}.t1 tt2 using(c_string) where tt1.k1 % 100 = 0;

-- inner join on all three keys
-- query 29
select count(tt1.k1), count(tt2.k1) from ${case_db}.t1 tt1 join ${case_db}.t1 tt2 on tt1.c_int=tt2.c_int and tt1.c_bigint=tt2.c_bigint and tt1.c_string=tt2.c_string;

-- inner join on negated key (no match)
-- query 30
select count(tt1.k1), count(tt2.k1) from ${case_db}.t1 tt1 join ${case_db}.t1 tt2 on -tt1.c_int=tt2.c_int;

-- inner join with broadcast hint and CTE
-- query 31
with
    w1 as (select k1, c_int from ${case_db}.t1 union all select k1, c_int + 2560000 from ${case_db}.t1 union all select k1, c_int + 2560000 * 2 from ${case_db}.t1)
select count(tt1.k1), count(tt2.k1) from ${case_db}.t1 tt1 join [broadcast] w1 tt2 using(c_int);

-- Left semi join tests

-- left semi join on c_int with expression key
-- query 32
select count(tt1.k1) from ${case_db}.t1 tt1 left semi join ${case_db}.t1 tt2 on tt1.c_int=tt2.c_int and tt1.c_int+1=tt2.c_int+1;

-- left semi join on c_int with expression key and filter
-- query 33
select count(tt1.k1) from ${case_db}.t1 tt1 left semi join ${case_db}.t1 tt2 on tt1.c_int=tt2.c_int and tt1.c_int+1=tt2.c_int+1 where tt1.k1 % 2 = 0;

-- left semi join on c_int + c_bigint
-- query 34
select count(tt1.k1) from ${case_db}.t1 tt1 left semi join ${case_db}.t1 tt2 on tt1.c_int=tt2.c_int and tt1.c_bigint=tt2.c_bigint;

-- left semi join using(c_int)
-- query 35
select count(tt1.k1) from ${case_db}.t1 tt1 left semi join ${case_db}.t1 tt2 using(c_int);

-- left semi join using(c_int) with filter % 2
-- query 36
select count(tt1.k1) from ${case_db}.t1 tt1 left semi join ${case_db}.t1 tt2 using(c_int) where tt1.k1 % 2 = 0;

-- left semi join using(c_int) with filter % 100
-- query 37
select count(tt1.k1) from ${case_db}.t1 tt1 left semi join ${case_db}.t1 tt2 using(c_int) where tt1.k1 % 100 = 0;

-- left semi join using(c_bigint)
-- query 38
select count(tt1.k1) from ${case_db}.t1 tt1 left semi join ${case_db}.t1 tt2 using(c_bigint);

-- left semi join using(c_bigint) with filter % 2
-- query 39
select count(tt1.k1) from ${case_db}.t1 tt1 left semi join ${case_db}.t1 tt2 using(c_bigint) where tt1.k1 % 2 = 0;

-- left semi join using(c_bigint) with filter % 100
-- query 40
select count(tt1.k1) from ${case_db}.t1 tt1 left semi join ${case_db}.t1 tt2 using(c_bigint) where tt1.k1 % 100 = 0;

-- left semi join using(c_string)
-- query 41
select count(tt1.c_string) from ${case_db}.t1 tt1 left semi join ${case_db}.t1 tt2 using(c_string);

-- left semi join using(c_string) with filter % 2
-- query 42
select count(tt1.c_string) from ${case_db}.t1 tt1 left semi join ${case_db}.t1 tt2 using(c_string) where tt1.k1 % 2 = 0;

-- left semi join using(c_string) with filter % 100
-- query 43
select count(tt1.c_string) from ${case_db}.t1 tt1 left semi join ${case_db}.t1 tt2 using(c_string) where tt1.k1 % 100 = 0;

-- left semi join on all three keys
-- query 44
select count(tt1.k1) from ${case_db}.t1 tt1 left semi join ${case_db}.t1 tt2 on tt1.c_int=tt2.c_int and tt1.c_bigint=tt2.c_bigint and tt1.c_string=tt2.c_string;

-- left semi join on negated key (no match)
-- query 45
select count(tt1.k1) from ${case_db}.t1 tt1 left semi join ${case_db}.t1 tt2 on -tt1.c_int=tt2.c_int;

-- left semi join with broadcast hint and CTE
-- query 46
with
    w1 as (select k1, c_int from ${case_db}.t1 union all select k1, c_int + 2560000 from ${case_db}.t1 union all select k1, c_int + 2560000 * 2 from ${case_db}.t1)
select count(tt1.k1) from ${case_db}.t1 tt1 left semi join [broadcast] w1 tt2 using(c_int);

-- Left outer join tests

-- left outer join on c_int with expression key
-- query 47
select count(tt1.k1), count(tt2.k1) from ${case_db}.t1 tt1 left outer join ${case_db}.t1 tt2 on tt1.c_int=tt2.c_int and tt1.c_int+1=tt2.c_int+1;

-- left outer join on c_int with expression key and filter
-- query 48
select count(tt1.k1), count(tt2.k1) from ${case_db}.t1 tt1 left outer join ${case_db}.t1 tt2 on tt1.c_int=tt2.c_int and tt1.c_int+1=tt2.c_int+1 where tt1.k1 % 2 = 0;

-- left outer join on c_int + c_bigint
-- query 49
select count(tt1.k1), count(tt2.k1) from ${case_db}.t1 tt1 left outer join ${case_db}.t1 tt2 on tt1.c_int=tt2.c_int and tt1.c_bigint=tt2.c_bigint;

-- left outer join using(c_int)
-- query 50
select count(tt1.k1), count(tt2.k1) from ${case_db}.t1 tt1 left outer join ${case_db}.t1 tt2 using(c_int);

-- left outer join using(c_int) with filter % 2
-- query 51
select count(tt1.k1), count(tt2.k1) from ${case_db}.t1 tt1 left outer join ${case_db}.t1 tt2 using(c_int) where tt1.k1 % 2 = 0;

-- left outer join using(c_int) with filter % 100
-- query 52
select count(tt1.k1), count(tt2.k1) from ${case_db}.t1 tt1 left outer join ${case_db}.t1 tt2 using(c_int) where tt1.k1 % 100 = 0;

-- left outer join using(c_bigint)
-- query 53
select count(tt1.k1), count(tt2.k1) from ${case_db}.t1 tt1 left outer join ${case_db}.t1 tt2 using(c_bigint);

-- left outer join using(c_bigint) with filter % 2
-- query 54
select count(tt1.k1), count(tt2.k1) from ${case_db}.t1 tt1 left outer join ${case_db}.t1 tt2 using(c_bigint) where tt1.k1 % 2 = 0;

-- left outer join using(c_bigint) with filter % 100
-- query 55
select count(tt1.k1), count(tt2.k1) from ${case_db}.t1 tt1 left outer join ${case_db}.t1 tt2 using(c_bigint) where tt1.k1 % 100 = 0;

-- left outer join using(c_string)
-- query 56
select count(tt1.c_string) from ${case_db}.t1 tt1 left outer join ${case_db}.t1 tt2 using(c_string);

-- left outer join using(c_string) with filter % 2
-- query 57
select count(tt1.c_string) from ${case_db}.t1 tt1 left outer join ${case_db}.t1 tt2 using(c_string) where tt1.k1 % 2 = 0;

-- left outer join using(c_string) with filter % 100
-- query 58
select count(tt1.c_string) from ${case_db}.t1 tt1 left outer join ${case_db}.t1 tt2 using(c_string) where tt1.k1 % 100 = 0;

-- left outer join on all three keys
-- query 59
select count(tt1.k1), count(tt2.k1) from ${case_db}.t1 tt1 left outer join ${case_db}.t1 tt2 on tt1.c_int=tt2.c_int and tt1.c_bigint=tt2.c_bigint and tt1.c_string=tt2.c_string;

-- left outer join on negated key (no right matches)
-- query 60
select count(tt1.k1), count(tt2.k1) from ${case_db}.t1 tt1 left outer join ${case_db}.t1 tt2 on -tt1.c_int=tt2.c_int;

-- left outer join with broadcast hint and CTE
-- query 61
with
    w1 as (select k1, c_int from ${case_db}.t1 union all select k1, c_int + 2560000 from ${case_db}.t1 union all select k1, c_int + 2560000 * 2 from ${case_db}.t1)
select count(tt1.k1), count(tt2.k1) from ${case_db}.t1 tt1 left outer join [broadcast] w1 tt2 using(c_int);
