-- Migrated from dev/test/sql/test_function/T/test_days_add
-- Test Objective:
-- 1. Validate days_add() adds days to a datetime value.
-- 2. Validate adddate() with INTERVAL for YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND units.
-- 3. Validate INTERVAL arithmetic expressions (multiplication, division).
-- 4. Validate large interval values across all units.

-- query 1
select days_add('2023-10-31 23:59:59', 1);

-- query 2
select days_add('2023-10-31 23:59:59', 1000);

-- query 3
select adddate('2023-10-31 23:59:59', INTERVAL 1 YEAR);

-- query 4
select adddate('2023-10-31 23:59:59', INTERVAL 1 YEAR * 2);

-- query 5
select adddate('2023-10-31 23:59:59', 2 * INTERVAL 1 YEAR);

-- query 6
select adddate('2023-10-31 23:59:59', INTERVAL 2 YEAR / 2);

-- query 7
-- @expect_error=Do not support Expr divide IntervalLiteral syntax
select adddate('2023-10-31 23:59:59', 2 / INTERVAL 2 YEAR);

-- query 8
select adddate('2023-10-31 23:59:59', INTERVAL 100 YEAR);

-- query 9
select adddate('2023-10-31 23:59:59', INTERVAL 1 MONTH);

-- query 10
select adddate('2023-10-31 23:59:59', INTERVAL 11 MONTH);

-- query 11
select adddate('2023-10-31 23:59:59', INTERVAL 25 MONTH);

-- query 12
select adddate('2023-10-31 23:59:59', INTERVAL 1 DAY);

-- query 13
select adddate('2023-10-31 23:59:59', INTERVAL 15 DAY);

-- query 14
select adddate('2023-10-31 23:59:59', INTERVAL 100 DAY);

-- query 15
select adddate('2023-10-31 23:59:59', INTERVAL 1000 DAY);

-- query 16
select adddate('2023-10-31 23:59:59', INTERVAL 1 HOUR);

-- query 17
select adddate('2023-10-31 23:59:59', INTERVAL 12 HOUR);

-- query 18
select adddate('2023-10-31 23:59:59', INTERVAL 25 HOUR);

-- query 19
select adddate('2023-10-31 23:59:59', INTERVAL 900 HOUR);

-- query 20
select adddate('2023-10-31 23:59:59', INTERVAL 10000 HOUR);

-- query 21
select adddate('2023-10-31 23:59:59', INTERVAL 1 MINUTE);

-- query 22
select adddate('2023-10-31 23:59:59', INTERVAL 30 MINUTE);

-- query 23
select adddate('2023-10-31 23:59:59', INTERVAL 80 MINUTE);

-- query 24
select adddate('2023-10-31 23:59:59', INTERVAL 1500 MINUTE);

-- query 25
select adddate('2023-10-31 23:59:59', INTERVAL 50000 MINUTE);

-- query 26
select adddate('2023-10-31 23:59:59', INTERVAL 600000 MINUTE);

-- query 27
select adddate('2023-10-31 23:59:59', INTERVAL 1 SECOND);

-- query 28
select adddate('2023-10-31 23:59:59', INTERVAL 30 SECOND);

-- query 29
select adddate('2023-10-31 23:59:59', INTERVAL 70 SECOND);

-- query 30
select adddate('2023-10-31 23:59:59', INTERVAL 3800 SECOND);

-- query 31
select adddate('2023-10-31 23:59:59', INTERVAL 89900 SECOND);

-- query 32
select adddate('2023-10-31 23:59:59', INTERVAL 1 MILLISECOND);

-- query 33
select adddate('2023-10-31 23:59:59', INTERVAL 500 MILLISECOND);

-- query 34
select adddate('2023-10-31 23:59:59', INTERVAL 3000 MILLISECOND);

-- query 35
select adddate('2023-10-31 23:59:59', INTERVAL 80000 MILLISECOND);

-- query 36
select adddate('2023-10-31 23:59:59', INTERVAL 4000000 MILLISECOND);

-- query 37
select adddate('2023-10-31 23:59:59', INTERVAL 1 MICROSECOND);

-- query 38
select adddate('2023-10-31 23:59:59', INTERVAL 500 MICROSECOND);

-- query 39
select adddate('2023-10-31 23:59:59', INTERVAL 3000 MICROSECOND);

-- query 40
select adddate('2023-10-31 23:59:59', INTERVAL 5500000 MICROSECOND);

-- query 41
select adddate('2023-10-31 23:59:59', INTERVAL 80000000 MICROSECOND);
