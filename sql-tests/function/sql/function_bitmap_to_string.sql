-- Migrated from dev/test/sql/test_bitmap_functions/T/test_bitmap_to_string
-- Test Objective:
-- 1. Validate bitmap_to_string with large uint64 values near UINT64_MAX.
-- 2. Validate that generate_series(LARGEINT) is not supported (single-arg with overflow).
-- 3. Validate multi-value bitmaps covering the uint64 upper range.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_bitmap_str;
CREATE TABLE ${case_db}.t_bitmap_str (
  `c1` int(11) NULL COMMENT "",
  `c2` bitmap BITMAP_UNION NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`c1`)
DISTRIBUTED BY HASH(`c1`) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- query 2
-- @expect_error=Unknown table function 'generate_series(LARGEINT)'
-- generate_series with a single LARGEINT argument is not supported; expect error
insert into ${case_db}.t_bitmap_str select 1, bitmap_agg(generate_series) from TABLE(generate_series(18446744073709551611));

-- query 3
-- Table is still empty after the failed insert above
select bitmap_to_string(c2) from ${case_db}.t_bitmap_str;

-- query 4
-- @skip_result_check=true
insert into ${case_db}.t_bitmap_str select 1, bitmap_agg(generate_series) from TABLE(generate_series(18446744073709551611, 18446744073709551615));

-- query 5
-- 5 large values: 18446744073709551611 through 18446744073709551615
select bitmap_to_string(c2) from ${case_db}.t_bitmap_str;

-- query 6
-- @skip_result_check=true
insert into ${case_db}.t_bitmap_str select 1, bitmap_agg(generate_series) from TABLE(generate_series(18446744073709551551, 18446744073709551615));

-- query 7
-- 65 values: 18446744073709551551 through 18446744073709551615
select bitmap_to_string(c2) from ${case_db}.t_bitmap_str;
