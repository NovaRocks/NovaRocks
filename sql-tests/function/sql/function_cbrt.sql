-- Migrated from dev/test/sql/test_function/T/test_cbrt
-- Test Objective:
-- 1. Validate cbrt() returns the cube root for positive, negative, and zero values.
-- 2. Validate cbrt() with non-integer floating-point inputs.
-- 3. Validate cbrt(null) returns NULL.

-- query 1
select cbrt(27);

-- query 2
select cbrt(0.0);

-- query 3
select cbrt(-27);

-- query 4
select cbrt(3.1415);

-- query 5
select cbrt(-3.1415);

-- query 6
select cbrt(null);
