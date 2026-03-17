-- Test Objective:
-- 1. Validate CAST from integer types (tinyint/int/bigint/largeint) to DECIMAL(9,0), DECIMAL(9,1).
-- 2. Validate CAST to DECIMAL(27,0), DECIMAL(27,1), DECIMAL(38,0), DECIMAL(38,1).
-- 3. Validate integer multiplication by 0/1 produces correct results.
-- 4. Validate CAST of multiplication result to DECIMAL(38,0).
-- Migrated from dev/test/sql/test_decimal/T/test_decimal_cast

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t1;
CREATE TABLE ${case_db}.t1 (
    k1 bigint NULL,
    c_tinyint tinyint null,
    c_int int null,
    c_bigint bigint null,
    c_largeint largeint null
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 96
PROPERTIES (
    "replication_num" = "1"
);
insert into ${case_db}.t1 values
    (1, 127, 2147483647, 9223372036854775807, 170141183460469231731687303715884105727),
    (2, -128, -2147483648, -9223372036854775808, -170141183460469231731687303715884105728),
    (3, null, null, null, null),
    (4, 0, 0, 0, 0),
    (5, 1, 1, 1, 1),
    (6, -1, -1, -1, -1),
    (7, 12, 214748364, 922337203685477580, 17014118346046923173168730371588410572),
    (8, -12, -214748364, -922337203685477580, -17014118346046923173168730371588410572);

-- query 2
select
    k1,
    cast(c_tinyint as DECIMAL(9,0)),
    cast(c_int as DECIMAL(9,0)),
    cast(c_bigint as DECIMAL(9,0)),
    cast(c_largeint as DECIMAL(9,0))
from ${case_db}.t1
order by k1;

-- query 3
select
    k1,
    cast(c_tinyint as DECIMAL(9,1)),
    cast(c_int as DECIMAL(9,1)),
    cast(c_bigint as DECIMAL(9,1)),
    cast(c_largeint as DECIMAL(9,1))
from ${case_db}.t1
order by k1;

-- query 4
select
    k1,
    cast(c_tinyint as DECIMAL(27,0)),
    cast(c_int as DECIMAL(27,0)),
    cast(c_bigint as DECIMAL(27,0)),
    cast(c_largeint as DECIMAL(27,0))
from ${case_db}.t1
order by k1;

-- query 5
select
    k1,
    cast(c_tinyint as DECIMAL(27,1)),
    cast(c_int as DECIMAL(27,1)),
    cast(c_bigint as DECIMAL(27,1)),
    cast(c_largeint as DECIMAL(27,1))
from ${case_db}.t1
order by k1;

-- query 6
select
    k1,
    cast(c_tinyint as DECIMAL(38,0)),
    cast(c_int as DECIMAL(38,0)),
    cast(c_bigint as DECIMAL(38,0)),
    cast(c_largeint as DECIMAL(38,0))
from ${case_db}.t1
order by k1;

-- query 7
select
    k1,
    cast(c_tinyint as DECIMAL(38,1)),
    cast(c_int as DECIMAL(38,1)),
    cast(c_bigint as DECIMAL(38,1)),
    cast(c_largeint as DECIMAL(38,1))
from ${case_db}.t1
order by k1;

-- query 8
select k1, c_tinyint * 0, c_int * 0, c_bigint * 0, c_largeint * 0 from ${case_db}.t1 order by k1;

-- query 9
select k1, c_tinyint * 1, c_int * 1, c_bigint * 1, c_largeint * 1 from ${case_db}.t1 order by k1;

-- query 10
select k1, cast(c_tinyint * 0 as decimal(38, 0)), cast(c_int * 0 as decimal(38, 0)), cast(c_bigint * 0 as decimal(38, 0)), cast(c_largeint * 0 as decimal(38, 0)) from ${case_db}.t1 order by k1;

-- query 11
select k1, cast(c_tinyint * 1 as decimal(38, 0)), cast(c_int * 1 as decimal(38, 0)), cast(c_bigint * 1 as decimal(38, 0)), cast(c_largeint * 1 as decimal(38, 0)) from ${case_db}.t1 order by k1;
