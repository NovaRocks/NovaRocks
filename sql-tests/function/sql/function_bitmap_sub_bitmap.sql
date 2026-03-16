-- Migrated from dev/test/sql/test_bitmap_functions (sub_bitmap section)
-- Test Objective:
-- 1. Validate sub_bitmap(bitmap, start, length): positive/negative start, boundary cases.
-- 2. Validate bitmap_subset_limit(bitmap, start, count): positive/negative count.
-- 3. Validate bitmap_subset_in_range(bitmap, start, end): normal/reversed range.
-- 4. Cover inline (no-table) queries and table-based queries with NULL/single/multi-element bitmaps.
-- 5. Cover 64-bit element values in bitmaps.

-- query 1
select bitmap_to_string(sub_bitmap(bitmap_from_string(''), 0, 3));

-- query 2
select bitmap_to_string(sub_bitmap(bitmap_from_string(''), -1, 3));

-- query 3
select bitmap_to_string(sub_bitmap(bitmap_from_string('1'), 0, 3));

-- query 4
select bitmap_to_string(sub_bitmap(bitmap_from_string('1'), -1, 3));

-- query 5
select bitmap_to_string(sub_bitmap(bitmap_from_string('1'), 1, 3));

-- query 6
select bitmap_to_string(sub_bitmap(bitmap_from_string('1,0,1,2,3,1,5'), 0, 3));

-- query 7
select bitmap_to_string(sub_bitmap(bitmap_from_string('1,0,1,2,3,1,5'), 3, 6));

-- query 8
SELECT bitmap_to_string(sub_bitmap(bitmap_from_string(group_concat(cast(x as string), ',')), 0, 3)) FROM TABLE(generate_series(1, 64, 1)) t(x);

-- query 9
SELECT bitmap_to_string(sub_bitmap(bitmap_from_string(group_concat(cast(x as string), ',')), 10, 3)) FROM TABLE(generate_series(1, 64, 1)) t(x);

-- query 10
select bitmap_to_string(bitmap_subset_limit(bitmap_from_string(''), 0, 3));

-- query 11
select bitmap_to_string(bitmap_subset_limit(bitmap_from_string(''), -1, 3));

-- query 12
select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1'), 0, 3));

-- query 13
select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1'), -1, 3));

-- query 14
select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1'), 1, 3));

-- query 15
select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1,0,1,2,3,1,5'), 0, 3));

-- query 16
select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1,0,1,2,3,1,5'), 3, 6));

-- query 17
SELECT bitmap_to_string(bitmap_subset_limit(bitmap_from_string(group_concat(cast(x as string), ',')), 0, 3)) FROM TABLE(generate_series(1, 64, 1)) t(x);

-- query 18
SELECT bitmap_to_string(bitmap_subset_limit(bitmap_from_string(group_concat(cast(x as string), ',')), 10, 3)) FROM TABLE(generate_series(1, 64, 1)) t(x);

-- query 19
-- Negative count: scan leftward from start
select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1,0,1,2,3,1,5'), 0, -3));

-- query 20
select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1,0,1,2,3,1,5'), 0, -1));

-- query 21
select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1,0,1,2,3,1,5'), 3, -6));

-- query 22
SELECT bitmap_to_string(bitmap_subset_limit(bitmap_from_string(group_concat(cast(x as string), ',')), 0, -3)) FROM TABLE(generate_series(1, 64, 1)) t(x);

-- query 23
SELECT bitmap_to_string(bitmap_subset_limit(bitmap_from_string(group_concat(cast(x as string), ',')), 10, -3)) FROM TABLE(generate_series(1, 64, 1)) t(x);

-- query 24
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string(''), 0, 3));

-- query 25
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string(''), -1, 3));

-- query 26
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1'), 0, 3));

-- query 27
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1'), -1, 3));

-- query 28
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1'), 1, 3));

-- query 29
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,0,1,2,3,1,5'), 0, 3));

-- query 30
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,0,1,2,3,1,5'), 3, 6));

-- query 31
SELECT bitmap_to_string(bitmap_subset_in_range(bitmap_from_string(group_concat(cast(x as string), ',')), 0, 3)) FROM TABLE(generate_series(1, 64, 1)) t(x);

-- query 32
-- Reversed range: end < start should return NULL
SELECT bitmap_to_string(bitmap_subset_in_range(bitmap_from_string(group_concat(cast(x as string), ',')), 10, 3)) FROM TABLE(generate_series(1, 64, 1)) t(x);

-- query 33
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string(''), 3, 0));

-- query 34
select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,0,1,2,3,1,5'), 3, 0));

-- query 35
SELECT bitmap_to_string(bitmap_subset_in_range(bitmap_from_string(group_concat(cast(x as string), ',')), 3, 0)) FROM TABLE(generate_series(1, 64, 1)) t(x);

-- query 36
-- Table-based queries with NULL, single-element, multi-element bitmaps
-- @skip_result_check=true
CREATE TABLE ${case_db}.test_bitmap_table1(
    k1 INT,
    v1 BITMAP BITMAP_UNION
) AGGREGATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 3
PROPERTIES('replication_num'='1');

-- query 37
-- @skip_result_check=true
INSERT INTO ${case_db}.test_bitmap_table1 SELECT 0, NULL;

-- query 38
-- @skip_result_check=true
INSERT INTO ${case_db}.test_bitmap_table1 SELECT 1, to_bitmap('1');

-- query 39
-- @skip_result_check=true
INSERT INTO ${case_db}.test_bitmap_table1 SELECT 2, to_bitmap(cast(x as string)) FROM TABLE(generate_series(1, 10, 1)) t(x);

-- query 40
-- @skip_result_check=true
INSERT INTO ${case_db}.test_bitmap_table1 SELECT 3, to_bitmap(cast(x as string)) FROM TABLE(generate_series(1, 100, 1)) t(x);

-- query 41
-- sub_bitmap from tail: k0=NULL,k1=single,k2=10-elem,k3=100-elem
SELECT bitmap_to_string(sub_bitmap(v1, -2, 3)) FROM ${case_db}.test_bitmap_table1 ORDER BY k1;

-- query 42
SELECT bitmap_to_string(sub_bitmap(v1, -1, 3)) FROM ${case_db}.test_bitmap_table1 ORDER BY k1;

-- query 43
SELECT bitmap_to_string(sub_bitmap(v1, 0, 3)) FROM ${case_db}.test_bitmap_table1 ORDER BY k1;

-- query 44
SELECT bitmap_to_string(sub_bitmap(v1, 1, 3)) FROM ${case_db}.test_bitmap_table1 ORDER BY k1;

-- query 45
SELECT bitmap_to_string(sub_bitmap(v1, 2, 3)) FROM ${case_db}.test_bitmap_table1 ORDER BY k1;

-- query 46
SELECT bitmap_to_string(bitmap_subset_limit(v1, 0, 3)) FROM ${case_db}.test_bitmap_table1 ORDER BY k1;

-- query 47
SELECT bitmap_to_string(bitmap_subset_limit(v1, 1, 3)) FROM ${case_db}.test_bitmap_table1 ORDER BY k1;

-- query 48
SELECT bitmap_to_string(bitmap_subset_limit(v1, 1, -3)) FROM ${case_db}.test_bitmap_table1 ORDER BY k1;

-- query 49
SELECT bitmap_to_string(bitmap_subset_limit(v1, 2, 3)) FROM ${case_db}.test_bitmap_table1 ORDER BY k1;

-- query 50
SELECT bitmap_to_string(bitmap_subset_limit(v1, 2, -3)) FROM ${case_db}.test_bitmap_table1 ORDER BY k1;

-- query 51
SELECT bitmap_to_string(bitmap_subset_limit(v1, 3, -3)) FROM ${case_db}.test_bitmap_table1 ORDER BY k1;

-- query 52
SELECT bitmap_to_string(bitmap_subset_in_range(v1, 0, 3)) FROM ${case_db}.test_bitmap_table1 ORDER BY k1;

-- query 53
SELECT bitmap_to_string(bitmap_subset_in_range(v1, -1, 3)) FROM ${case_db}.test_bitmap_table1 ORDER BY k1;

-- query 54
SELECT bitmap_to_string(bitmap_subset_in_range(v1, 1, 3)) FROM ${case_db}.test_bitmap_table1 ORDER BY k1;

-- query 55
-- Reversed range
SELECT bitmap_to_string(bitmap_subset_in_range(v1, 3, 2)) FROM ${case_db}.test_bitmap_table1 ORDER BY k1;

-- query 56
SELECT bitmap_to_string(bitmap_subset_in_range(v1, 2, 3)) FROM ${case_db}.test_bitmap_table1 ORDER BY k1;

-- query 57
SELECT bitmap_to_string(bitmap_subset_in_range(v1, -2, 3)) FROM ${case_db}.test_bitmap_table1 ORDER BY k1;
