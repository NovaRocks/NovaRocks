-- Test Objective:
-- 1. Validate decimal multiplication precision with decimal_overflow_to_double = false (default):
--    overflow stays in decimal128 with truncation.
-- 2. Validate decimal multiplication precision with decimal_overflow_to_double = true:
--    overflowed results are promoted to double instead of decimal128.
-- 3. Validate literal multiplication overflow behavior under both settings.
-- 4. Validate mixed expressions and extreme overflow edge cases.
-- Migrated from dev/test/sql/test_decimal/T/test_decimal_to_double.sql

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_decimal_precision_overflow;
CREATE TABLE ${case_db}.`t_decimal_precision_overflow` (
  `c_id` int(11) NOT NULL,
  `c_d64_max` decimal64(18,9) NOT NULL,
  `c_d128_large` decimal128(30,10) NOT NULL,
  `c_d128_max` decimal128(38,15) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c_id`)
DISTRIBUTED BY HASH(`c_id`) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);

-- Insert test data that will definitely cause precision overflow when multiplied
-- decimal64(18,9) * decimal64(18,9) = precision 36, scale 18 (fits in decimal128)
-- decimal128(30,10) * decimal64(18,9) = precision 48, scale 19 (overflow!)
-- decimal128(38,15) * decimal128(38,15) = precision 76, scale 30 (overflow!)
INSERT INTO ${case_db}.`t_decimal_precision_overflow` (c_id, c_d64_max, c_d128_large, c_d128_max) values
   (1, 123456789.123456789, 12345678901234567890.1234567890, 12345678901234567890123.123456789012345),
   (2, -123456789.123456789, -12345678901234567890.1234567890, -12345678901234567890123.123456789012345),
   (3, 987654321.987654321, 98765432109876543210.9876543210, 98765432109876543210987.987654321098765),
   (4, 111222333.444555666, 11122233344455566677.8899001122, 11122233344455566677889.900112233445566),
   (5, 555666777.888999111, 55566677788899911122.2333444555, 55566677788899911122233.344455566677888);

-- Test 1: decimal_overflow_to_double = false (default behavior)

-- query 2
-- These should NOT cause overflow (precision 36, scale 18 <= 38)
set decimal_overflow_to_double = false;
select c_d64_max * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 1;

-- query 3
set decimal_overflow_to_double = false;
select c_d64_max * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 2;

-- query 4
set decimal_overflow_to_double = false;
select c_d64_max * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 3;

-- query 5
set decimal_overflow_to_double = false;
select c_d64_max * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 4;

-- query 6
set decimal_overflow_to_double = false;
select c_d64_max * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 5;

-- query 7
-- These SHOULD cause precision overflow (precision 48, scale 19) but use decimal128 truncation
set decimal_overflow_to_double = false;
select c_d128_large * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 1;

-- query 8
set decimal_overflow_to_double = false;
select c_d128_large * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 2;

-- query 9
set decimal_overflow_to_double = false;
select c_d128_large * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 3;

-- query 10
set decimal_overflow_to_double = false;
select c_d128_large * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 4;

-- query 11
set decimal_overflow_to_double = false;
select c_d128_large * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 5;

-- query 12
-- These SHOULD cause major precision overflow (precision 76, scale 30) but use decimal128 truncation
set decimal_overflow_to_double = false;
select c_d128_max * c_d128_max from ${case_db}.t_decimal_precision_overflow where c_id = 1;

-- query 13
set decimal_overflow_to_double = false;
select c_d128_max * c_d128_max from ${case_db}.t_decimal_precision_overflow where c_id = 2;

-- query 14
set decimal_overflow_to_double = false;
select c_d128_max * c_d128_max from ${case_db}.t_decimal_precision_overflow where c_id = 3;

-- query 15
-- Test literal multiplication that definitely causes precision overflow
-- precision 60, scale 18 -> overflow!
set decimal_overflow_to_double = false;
select 123456789012345678901234567890.123456789 * 987654321098765432109876543210.987654321;

-- query 16
-- precision 52, scale 20 -> overflow!
set decimal_overflow_to_double = false;
select 12345678901234567890123456789012.12345678901 * 98765432109876543210.12345678901;

-- query 17
-- precision 50, scale 16 -> overflow!
set decimal_overflow_to_double = false;
select 1234567890123456789012345678.12345678 * 9876543210987654321098765432.87654321;

-- query 18
-- precision 44, scale 12 -> overflow!
set decimal_overflow_to_double = false;
select 12345678901234567890123456.123456 * 98765432109876543210987654.876543;

-- Test 2: decimal_overflow_to_double = true (new behavior)

-- query 19
-- These should still return decimal128 (no overflow, precision 36, scale 18 <= 38)
set decimal_overflow_to_double = true;
select c_d64_max * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 1;

-- query 20
set decimal_overflow_to_double = true;
select c_d64_max * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 2;

-- query 21
set decimal_overflow_to_double = true;
select c_d64_max * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 3;

-- query 22
set decimal_overflow_to_double = true;
select c_d64_max * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 4;

-- query 23
set decimal_overflow_to_double = true;
select c_d64_max * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 5;

-- query 24
-- These SHOULD now return double instead of decimal128 (precision 48 > 38, scale 19 <= 38)
set decimal_overflow_to_double = true;
select c_d128_large * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 1;

-- query 25
set decimal_overflow_to_double = true;
select c_d128_large * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 2;

-- query 26
set decimal_overflow_to_double = true;
select c_d128_large * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 3;

-- query 27
set decimal_overflow_to_double = true;
select c_d128_large * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 4;

-- query 28
set decimal_overflow_to_double = true;
select c_d128_large * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 5;

-- query 29
-- These SHOULD now return double instead of decimal128 (precision 76 > 38, scale 30 <= 38)
set decimal_overflow_to_double = true;
select c_d128_max * c_d128_max from ${case_db}.t_decimal_precision_overflow where c_id = 1;

-- query 30
set decimal_overflow_to_double = true;
select c_d128_max * c_d128_max from ${case_db}.t_decimal_precision_overflow where c_id = 2;

-- query 31
set decimal_overflow_to_double = true;
select c_d128_max * c_d128_max from ${case_db}.t_decimal_precision_overflow where c_id = 3;

-- query 32
-- Test literal multiplication that causes precision overflow - should return double
-- precision 60 > 38, scale 18 <= 38 -> should convert to double
set decimal_overflow_to_double = true;
select 123456789012345678901234567890.123456789 * 987654321098765432109876543210.987654321;

-- query 33
-- precision 52 > 38, scale 20 <= 38 -> should convert to double
set decimal_overflow_to_double = true;
select 12345678901234567890123456789012.12345678901 * 98765432109876543210.12345678901;

-- query 34
-- precision 50 > 38, scale 16 <= 38 -> should convert to double
set decimal_overflow_to_double = true;
select 1234567890123456789012345678.12345678 * 9876543210987654321098765432.87654321;

-- query 35
-- precision 44 > 38, scale 12 <= 38 -> should convert to double
set decimal_overflow_to_double = true;
select 12345678901234567890123456.123456 * 98765432109876543210987654.876543;

-- query 36
set decimal_overflow_to_double = false;
select 1234567890123456789.1234567890123456789 * 1.0000000000000000000;

-- query 37
set decimal_overflow_to_double = true;
select 1234567890123456789.1234567890123456789 * 1.0000000000000000000;

-- query 38
set decimal_overflow_to_double = false;
select 12345678901234567890123456789012345678.0 * 1.0;

-- query 39
set decimal_overflow_to_double = true;
select 12345678901234567890123456789012345678.0 * 1.1;

-- Test 4: Mixed operations

-- query 40
-- Test multiplication in expressions - these should convert to double due to overflow
set decimal_overflow_to_double = true;
select c_id, c_d128_large * c_d64_max + 1.0 from ${case_db}.t_decimal_precision_overflow where c_id <= 2 order by 1;

-- query 41
set decimal_overflow_to_double = true;
select c_id, (c_d128_max * c_d128_max) / 2.0 from ${case_db}.t_decimal_precision_overflow where c_id <= 2 order by 1;

-- Test 6: Additional extreme overflow cases

-- query 42
-- Extreme case: very large precision overflow (should use decimal128 truncation)
set decimal_overflow_to_double = false;
select 12345678901234567890123456789012345678.0 * 98765432109876543210987654321098765432.0;

-- query 43
-- Same extreme case: should convert to double
set decimal_overflow_to_double = true;
select 12345678901234567890123456789012345678.0 * 98765432109876543210987654321098765432.0;

-- Test 7: Verify behavior with different scale combinations

-- query 44
-- High precision, low scale (should use decimal128 truncation)
set decimal_overflow_to_double = false;
select 123456789012345678901234567890123456.78 * 987654321098765432109876543210987654.32;

-- query 45
-- Same case: should convert to double (precision 72 > 38, scale 4 <= 38)
set decimal_overflow_to_double = true;
select 123456789012345678901234567890123456.78 * 987654321098765432109876543210987654.32;

-- Test 8: More diverse precision overflow cases

-- query 46
-- Medium precision overflow
set decimal_overflow_to_double = false;
select 1111222233334444555566667777.8888 * 8888777766665555444433332222.1111;

-- query 47
-- Same case with session variable enabled
set decimal_overflow_to_double = true;
select 1111222233334444555566667777.8888 * 8888777766665555444433332222.1111;

-- query 48
-- Different scale patterns
set decimal_overflow_to_double = false;
select 123456789012345678901234567890.123456 * 987654321098765432109876543210.654321;

-- query 49
set decimal_overflow_to_double = true;
select 123456789012345678901234567890.123456 * 987654321098765432109876543210.654321;

-- query 50
set decimal_overflow_to_double = true;
SELECT(0.58000000 * 0.970825897017235893 * 3021621.785498);

-- query 51
set decimal_overflow_to_double = false;
SELECT(0.58000000 * 0.970825897017235893 * 3021621.785498);
