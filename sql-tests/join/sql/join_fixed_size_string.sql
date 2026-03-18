-- @tags=join,fixed_size_string
-- Test Objective:
-- 1. Validate hash join correctness on fixed-size string keys (4, 8, 16, 32 bytes).
-- 2. Verify both equality (=) and null-safe equality (<=>) join semantics on string columns.
-- 3. Ensure correct handling of null-byte strings in join keys.
-- Test Flow:
-- 1. Create helper tables to generate a large row set with row_number indexing.
-- 2. Create t1 and t2 with string columns of various fixed-size lengths.
-- 3. Join on each string column length with = and <=> and verify aggregated counts/hashes.
-- 4. Insert null-byte strings and verify <=> join still works correctly.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.__row_util_base (
  k1 bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 32
PROPERTIES (
    "replication_num" = "1"
);

-- query 2
-- @skip_result_check=true
insert into ${case_db}.__row_util_base select generate_series from TABLE(generate_series(0, 10000 - 1));

-- query 3
-- @skip_result_check=true
insert into ${case_db}.__row_util_base select * from ${case_db}.__row_util_base;

-- query 4
-- @skip_result_check=true
insert into ${case_db}.__row_util_base select * from ${case_db}.__row_util_base;

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
CREATE TABLE ${case_db}.__row_util (
  idx bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(`idx`)
DISTRIBUTED BY HASH(`idx`) BUCKETS 32
PROPERTIES (
    "replication_num" = "1"
);

-- query 9
-- @skip_result_check=true
insert into ${case_db}.__row_util select row_number() over() as idx from ${case_db}.__row_util_base;

-- query 10
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
    k1 bigint NULL,
    c_int int NULL,
    c_bigint bigint NULL,
    c_str4 STRING NULL,
    c_str8 STRING NULL,
    c_str16 STRING NULL,
    c_str32 STRING NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 64
PROPERTIES (
    "replication_num" = "1"
);

-- query 11
-- @skip_result_check=true
insert into ${case_db}.t1
select
    idx,
    idx,
    idx,
    substr(lpad(idx, 4, '-'), 1, 4),
    substr(lpad(idx, 8, '-'), 1, 8),
    substr(lpad(idx, 16, '-'), 1, 16),
    substr(lpad(idx, 32, '-'), 1, 32)
from ${case_db}.__row_util where idx <= 10000;

-- query 12
-- @skip_result_check=true
insert into ${case_db}.t1 (k1) select 1001;

-- query 13
-- @skip_result_check=true
CREATE TABLE ${case_db}.t2 (
    k1 bigint NULL,
    c_int int NULL,
    c_bigint bigint NULL,
    c_str4 STRING NULL,
    c_str8 STRING NULL,
    c_str16 STRING NULL,
    c_str32 STRING NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 64
PROPERTIES (
    "replication_num" = "1"
);

-- query 14
-- @skip_result_check=true
insert into ${case_db}.t2 select idx, t1.c_int, t1.c_bigint, t1.c_str4, t1.c_str8, t1.c_str16, t1.c_str32 from ${case_db}.__row_util join ${case_db}.t1 on idx % 1000 = t1.k1;

-- query 15
-- @skip_result_check=true
insert into ${case_db}.t2 (k1) select 320000;

-- query 16
-- @skip_result_check=true
insert into ${case_db}.t2 select idx, idx, idx, uuid(), uuid(), uuid(), uuid() from ${case_db}.__row_util where idx <= 10000;

-- join on c_str4 with =
-- query 17
select count(1), sum(murmur_hash3_32(t1.c_str4)), sum(murmur_hash3_32(t2.c_str4))
from ${case_db}.t1 join ${case_db}.t2 on t1.c_str4 = t2.c_str4;

-- join on c_str4 with <=>
-- query 18
select count(1), sum(murmur_hash3_32(t1.c_str4)), sum(murmur_hash3_32(t2.c_str4))
from ${case_db}.t1 join ${case_db}.t2 on t1.c_str4 <=> t2.c_str4;

-- join on c_str4 with = and c_int =
-- query 19
select count(1), sum(murmur_hash3_32(t1.c_str4)), sum(murmur_hash3_32(t2.c_str4))
from ${case_db}.t1 join ${case_db}.t2 on t1.c_str4 = t2.c_str4 and t1.c_int = t2.c_int;

-- join on c_str8 with =
-- query 20
select count(1), sum(murmur_hash3_32(t1.c_str8)), sum(murmur_hash3_32(t2.c_str8))
from ${case_db}.t1 join ${case_db}.t2 on t1.c_str8 = t2.c_str8;

-- join on c_str8 with <=>
-- query 21
select count(1), sum(murmur_hash3_32(t1.c_str8)), sum(murmur_hash3_32(t2.c_str8))
from ${case_db}.t1 join ${case_db}.t2 on t1.c_str8 <=> t2.c_str8;

-- join on c_str8 with = and c_int =
-- query 22
select count(1), sum(murmur_hash3_32(t1.c_str8)), sum(murmur_hash3_32(t2.c_str8))
from ${case_db}.t1 join ${case_db}.t2 on t1.c_str8 = t2.c_str8 and t1.c_int = t2.c_int;

-- join on c_str16 with =
-- query 23
select count(1), sum(murmur_hash3_32(t1.c_str16)), sum(murmur_hash3_32(t2.c_str16))
from ${case_db}.t1 join ${case_db}.t2 on t1.c_str16 = t2.c_str16;

-- join on c_str16 with <=>
-- query 24
select count(1), sum(murmur_hash3_32(t1.c_str16)), sum(murmur_hash3_32(t2.c_str16))
from ${case_db}.t1 join ${case_db}.t2 on t1.c_str16 <=> t2.c_str16;

-- join on c_str16 with = and c_int =
-- query 25
select count(1), sum(murmur_hash3_32(t1.c_str16)), sum(murmur_hash3_32(t2.c_str16))
from ${case_db}.t1 join ${case_db}.t2 on t1.c_str16 = t2.c_str16 and t1.c_int = t2.c_int;

-- join on c_str32 with =
-- query 26
select count(1), sum(murmur_hash3_32(t1.c_str32)), sum(murmur_hash3_32(t2.c_str32))
from ${case_db}.t1 join ${case_db}.t2 on t1.c_str32 = t2.c_str32;

-- insert null-byte string rows into t2
-- query 27
-- @skip_result_check=true
insert into ${case_db}.t2
select
    320001,
    320001,
    320001,
    '\0\0\0\0',
    '\0\0\0\0\0\0\0\0',
    '\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
    '\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0';

-- join on c_str8 with <=> after null-byte insert into t2
-- query 28
select count(1), sum(murmur_hash3_32(t1.c_str8)), sum(murmur_hash3_32(t2.c_str8))
from ${case_db}.t1 join ${case_db}.t2 on t1.c_str8 <=> t2.c_str8;

-- insert null-byte string rows into t1
-- query 29
-- @skip_result_check=true
insert into ${case_db}.t1
select
    320001,
    320001,
    320001,
    '\0\0\0\0',
    '\0\0\0\0\0\0\0\0',
    '\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0',
    '\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0';

-- join on c_str8 with <=> after null-byte insert into both
-- query 30
select count(1), sum(murmur_hash3_32(t1.c_str8)), sum(murmur_hash3_32(t2.c_str8))
from ${case_db}.t1 join ${case_db}.t2 on t1.c_str8 <=> t2.c_str8;
