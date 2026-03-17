-- Migrated from dev/test/sql/test_function/T/test_date_trunc
-- Test Objective:
-- 1. Validate date_trunc() truncation for all supported granularities: year, quarter, month, week, day, hour, minute, second, millisecond, microsecond.
-- 2. Validate behavior with both DATETIME and DATE input strings.
-- 3. Verify sub-second precision (millisecond, microsecond) is correctly handled.

-- query 1
select date_trunc('year', '2023-10-31 23:59:59.001002');

-- query 2
select date_trunc('year', '2023-10-31');

-- query 3
select date_trunc('quarter', '2023-10-31 23:59:59.001002');

-- query 4
select date_trunc('quarter', '2023-10-31');

-- query 5
select date_trunc('quarter', '2023-09-15 23:59:59.001002');

-- query 6
select date_trunc('quarter', '2023-09-15');

-- query 7
select date_trunc('month', '2023-10-31 23:59:59.001002');

-- query 8
select date_trunc('month', '2023-10-31');

-- query 9
select date_trunc('week', '2023-10-31 23:59:59.001002');

-- query 10
select date_trunc('week', '2023-10-31');

-- query 11
select date_trunc('day', '2023-10-31 23:59:59.001002');

-- query 12
select date_trunc('day', '2023-10-31');

-- query 13
select date_trunc('hour', '2023-10-31 23:59:59.001002');

-- query 14
select date_trunc('hour', '2023-10-31');

-- query 15
select date_trunc('minute', '2023-10-31 23:59:59.001002');

-- query 16
select date_trunc('minute', '2023-10-31');

-- query 17
select date_trunc('second', '2023-10-31 23:59:59.001002');

-- query 18
select date_trunc('second', '2023-10-31');

-- query 19
select date_trunc('millisecond', '2023-10-31 23:59:59.001002');

-- query 20
select date_trunc('millisecond', '2023-10-31');

-- query 21
select date_trunc('microsecond', '2023-10-31 23:59:59.001002');

-- query 22
select date_trunc('microsecond', '2023-10-31');
