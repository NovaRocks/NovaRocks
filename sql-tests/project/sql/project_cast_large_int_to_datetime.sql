-- Migrated from dev/test/sql/test_cast/T/test_cast_to_datetime
-- Test Objective:
-- 1. Validate that CAST of out-of-range integers as DATETIME returns NULL.
-- 2. Covers overflow, zero, one, max uint64, and yearweek() with overflow.
-- 3. These values cannot represent valid DATETIME and must return NULL.

-- query 1
select cast(-18446744073709551494 AS DATETIME);

-- query 2
select cast(0 AS DATETIME);

-- query 3
select cast(1 AS DATETIME);

-- query 4
select cast(18446744073709551615 AS DATETIME);

-- query 5
select yearweek(-18446744073709551494);
