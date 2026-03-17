-- Test Objective:
-- 1. Validate decimal multiplication result is NULL on overflow (default mode).
-- 2. Validate ERROR_IF_OVERFLOW sql_mode raises error on overflow.
-- 3. Validate decimal subtraction and complex aggregate expressions.
-- Migrated from dev/test/sql/test_decimal/T/test_decimal_overflow

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_decimal_overflow;
DROP TABLE IF EXISTS ${case_db}.avg_test;
CREATE TABLE ${case_db}.`t_decimal_overflow` (
  `c_id` int(11) NOT NULL,
  `c_d32` decimal32(9,3) NOT NULL,
  `c_d64` decimal64(18,5) NOT NULL,
  `c_d128` decimal128(38,7) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c_id`)
DISTRIBUTED BY HASH(`c_id`) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);
CREATE TABLE ${case_db}.`avg_test` (
  `c0` bigint NULL COMMENT "",
  `c1` array<int> NULL COMMENT "",
  `c2` bigint NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 5
PROPERTIES (
"replication_num" = "1"
);
INSERT INTO ${case_db}.`t_decimal_overflow` (c_id, c_d32, c_d64, c_d128) values
   (1, 999999.99, 9999999999999.99999, 9999999999999999999999999999999.9999999),
   (2, -999999.99, -9999999999999.99999, -9999999999999999999999999999999.9999999);
insert into ${case_db}.avg_test values (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 123456789, 1);
insert into ${case_db}.avg_test values (1, [11, 12, 13, 14, 15, 16, 17, 18, 19, 20], 123456789, 1);

-- query 2
select 274.97790000000000000000 * (round(1103.00000000000000000000 * 1.0000,16) /round(1103.00000000000000000000,16));

-- query 3
-- @expect_error=The 'mul' operation involving decimal values overflows
select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ 274.97790000000000000000 * (round(1103.00000000000000000000 * 1.0000,16) /round(1103.00000000000000000000,16));

-- query 4
select cast(c_d32 * c_d32 as decimal32) from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 5
select cast(c_d32 * c_d32 as decimal32) from ${case_db}.t_decimal_overflow where c_id = 2;

-- query 6
select cast(c_d64 * c_d64 as decimal64) from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 7
select cast(c_d64 * c_d64 as decimal64) from ${case_db}.t_decimal_overflow where c_id = 2;

-- query 8
select cast(c_d128 * c_d128 as decimal128) from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 9
select cast(c_d128 * c_d128 as decimal128) from ${case_db}.t_decimal_overflow where c_id = 2;

-- query 10
select cast(c_d32 * 1.000 as decimal32) from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 11
select cast(c_d32 * 1.000 as decimal32) from ${case_db}.t_decimal_overflow where c_id = 2;

-- query 12
select cast(c_d64 * 1.000000 as decimal64) from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 13
select cast(c_d64 * 1.000000 as decimal64) from ${case_db}.t_decimal_overflow where c_id = 2;

-- query 14
select cast(c_d128 * 1.000000000 as decimal128) from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 15
select cast(c_d128 * 1.000000000 as decimal128) from ${case_db}.t_decimal_overflow where c_id = 2;

-- query 16
-- @expect_error=The type cast from decimal to decimal overflows
select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ cast(c_d32 * c_d32 as decimal32) from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 17
-- @expect_error=The type cast from decimal to decimal overflows
select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ cast(c_d32 * c_d32 as decimal32) from ${case_db}.t_decimal_overflow where c_id = 2;

-- query 18
-- @expect_error=The type cast from decimal to decimal overflows
select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ cast(c_d64 * c_d64 as decimal64) from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 19
-- @expect_error=The type cast from decimal to decimal overflows
select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ cast(c_d64 * c_d64 as decimal64) from ${case_db}.t_decimal_overflow where c_id = 2;

-- query 20
-- @expect_error=The 'mul' operation involving decimal values overflows
select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ cast(c_d128 * c_d128 as decimal128) from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 21
-- @expect_error=The 'mul' operation involving decimal values overflows
select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ cast(c_d128 * c_d128 as decimal128) from ${case_db}.t_decimal_overflow where c_id = 2;

-- query 22
-- @expect_error=The type cast from decimal to decimal overflows
select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ cast(c_d32 * 1.000 as decimal32) from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 23
-- @expect_error=The type cast from decimal to decimal overflows
select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ cast(c_d32 * 1.000 as decimal32) from ${case_db}.t_decimal_overflow where c_id = 2;

-- query 24
-- @expect_error=The type cast from decimal to decimal overflows
select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ cast(c_d64 * 1.000000 as decimal64) from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 25
-- @expect_error=The type cast from decimal to decimal overflows
select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ cast(c_d64 * 1.000000 as decimal64) from ${case_db}.t_decimal_overflow where c_id = 2;

-- query 26
-- @expect_error=The 'mul' operation involving decimal values overflows
select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ cast(c_d128 * 1.000000000 as decimal128) from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 27
-- @expect_error=The 'mul' operation involving decimal values overflows
select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ cast(c_d128 * 1.000000000 as decimal128) from ${case_db}.t_decimal_overflow where c_id = 2;

-- query 28
select c_id - 1.12345678901234567890 from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 29
select max(c0- 2.8665963056616452*(lt - 3.062472673706541)) as adjust_lt from (select c0, array_sum(c1) lt, c2 from ${case_db}.avg_test) t group by c2;

-- query 30
select avg(c0- 2.8665963056616452*(lt - 3.062472673706541)) as adjust_lt from (select c0, array_sum(c1) lt, c2 from ${case_db}.avg_test) t group by c2;

-- query 31
-- @expect_error=The 'mul' operation involving decimal values overflows
select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ max(lt- 2.8665963056616452*(c2 - 3.062472673706541)) as adjust_lt from (select c0, array_sum(c1) lt, c2 from ${case_db}.avg_test) t group by c0;

-- query 32
-- @expect_error=The 'mul' operation involving decimal values overflows
select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ avg(lt- 2.8665963056616452*(c2 - 3.062472673706541)) as adjust_lt from (select c0, array_sum(c1) lt, c2 from ${case_db}.avg_test) t group by c0;
