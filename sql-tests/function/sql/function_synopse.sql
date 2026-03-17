-- Migrated from dev/test/sql/test_function/T/test_synopse
-- Test Objective:
-- 1. Validate bar() function renders a horizontal bar chart string for a value within a range.
-- 2. Validate equiwidth_bucket() assigns values into equal-width histogram buckets.
-- 3. Both functions tested over a generate_series range from 0 to 10.

-- query 1
select r, bar(r, 0, 10, 20) as x from table(generate_series(0, 10)) as s(r);

-- query 2
select r, equiwidth_bucket(r, 0, 10, 20) as x from table(generate_series(0, 10)) as s(r);
