-- Migrated from dev/test/sql/test_cast/T/test_cast_string_to_int
-- Test Objective:
-- 1. Validate that CAST('+' or '-' as integer types) returns NULL.
-- 2. Covers tinyint, smallint, int, bigint, largeint.
-- 3. These strings cannot be parsed as integers and must return NULL, not an error.

-- query 1
select cast('-' as tinyint);

-- query 2
select cast('-' as smallint);

-- query 3
select cast('-' as int);

-- query 4
select cast('-' as bigint);

-- query 5
select cast('-' as largeint);

-- query 6
select cast('+' as tinyint);

-- query 7
select cast('+' as smallint);

-- query 8
select cast('+' as int);

-- query 9
select cast('+' as bigint);

-- query 10
select cast('+' as largeint);
