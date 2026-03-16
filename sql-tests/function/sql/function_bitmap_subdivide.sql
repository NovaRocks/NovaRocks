-- Migrated from dev/test/sql/test_bitmap_functions (test_subdivide_bitmap section)
-- Test Objective:
-- 1. Validate subdivide_bitmap() table function with nullable and non-nullable bitmap columns.
-- 2. Cover const split size (0, 1, 3, 13, 90) and non-const split size from column.
-- 3. Verify NULL bitmap, empty bitmap, single-element, small (10), large (60+19) bitmaps.
-- 4. Verify 64-bit element values (8589934592+).

-- query 1
-- Nullable bitmap column, const split size
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
  c1 int(11) NULL COMMENT "",
  c2 bitmap BITMAP_UNION NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(c1)
DISTRIBUTED BY HASH(c1) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- query 2
-- Nullable bitmap + non-const split size (split size from column c2)
-- @skip_result_check=true
CREATE TABLE ${case_db}.t2 (
  c1 int(11) NULL COMMENT "",
  c2 int(11) NULL COMMENT "",
  c3 bitmap BITMAP_UNION NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(c1, c2)
DISTRIBUTED BY HASH(c1, c2) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- query 3
-- Non-nullable bitmap column, const split size
-- @skip_result_check=true
CREATE TABLE ${case_db}.t3 (
  c1 int(11) NOT NULL COMMENT "",
  c2 bitmap BITMAP_UNION NOT NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(c1)
DISTRIBUTED BY HASH(c1) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- query 4
-- Non-nullable bitmap column, non-const split size
-- @skip_result_check=true
CREATE TABLE ${case_db}.t4 (
  c1 int(11) NOT NULL COMMENT "",
  c2 int(11) NOT NULL COMMENT "",
  c3 bitmap BITMAP_UNION NOT NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(c1, c2)
DISTRIBUTED BY HASH(c1, c2) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- query 5
-- Insert data into t1: null bitmap, empty, single-elem, 10-elem, 60-elem + 19 large values
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 SELECT 1, null;
INSERT INTO ${case_db}.t1 SELECT 2, bitmap_empty();
INSERT INTO ${case_db}.t1 SELECT 3, to_bitmap(1);
INSERT INTO ${case_db}.t1 SELECT 4, bitmap_agg(generate_series) FROM TABLE(generate_series(1, 10));
INSERT INTO ${case_db}.t1 SELECT 5, bitmap_agg(generate_series) FROM TABLE(generate_series(1, 60));
INSERT INTO ${case_db}.t1 SELECT 5, bitmap_agg(generate_series) FROM TABLE(generate_series(8589934592, 8589934610));

-- query 6
-- split_size=0: no rows returned (empty subdivide result)
SELECT c1, bitmap_count(subdivide_bitmap) FROM ${case_db}.t1, subdivide_bitmap(${case_db}.t1.c2, 0) ORDER BY c1;

-- query 7
-- split_size=1: each element becomes one subdivision
SELECT c1, bitmap_min(subdivide_bitmap) AS min_value, bitmap_to_string(subdivide_bitmap) FROM ${case_db}.t1, subdivide_bitmap(${case_db}.t1.c2, 1) ORDER BY c1, min_value;

-- query 8
-- split_size=3
SELECT c1, bitmap_min(subdivide_bitmap) AS min_value, bitmap_to_string(subdivide_bitmap) FROM ${case_db}.t1, subdivide_bitmap(${case_db}.t1.c2, 3) ORDER BY c1, min_value;

-- query 9
-- split_size=13
SELECT c1, bitmap_min(subdivide_bitmap) AS min_value, bitmap_to_string(subdivide_bitmap) FROM ${case_db}.t1, subdivide_bitmap(${case_db}.t1.c2, 13) ORDER BY c1, min_value;

-- query 10
-- split_size=90 (larger than all bitmaps): each bitmap returned as one chunk
SELECT c1, bitmap_min(subdivide_bitmap) AS min_value, bitmap_to_string(subdivide_bitmap) FROM ${case_db}.t1, subdivide_bitmap(${case_db}.t1.c2, 90) ORDER BY c1, min_value;

-- query 11
-- Insert data into t2 (non-const split size from c2 column)
-- @skip_result_check=true
INSERT INTO ${case_db}.t2 SELECT 1, 0, bitmap_empty();
INSERT INTO ${case_db}.t2 SELECT 2, 1, bitmap_empty();
INSERT INTO ${case_db}.t2 SELECT 3, 1, to_bitmap(1);
INSERT INTO ${case_db}.t2 SELECT 4, 2, to_bitmap(1);
INSERT INTO ${case_db}.t2 SELECT 5, 3, bitmap_agg(generate_series) FROM TABLE(generate_series(1, 10));
INSERT INTO ${case_db}.t2 SELECT 6, 10, bitmap_agg(generate_series) FROM TABLE(generate_series(1, 60));
INSERT INTO ${case_db}.t2 SELECT 6, 10, bitmap_agg(generate_series) FROM TABLE(generate_series(8589934592, 8589934610));
INSERT INTO ${case_db}.t2 SELECT 7, null, bitmap_agg(generate_series) FROM TABLE(generate_series(8589934592, 8589934610));
INSERT INTO ${case_db}.t2 SELECT 8, 10, null;

-- query 12
-- Non-const split size: split size comes from c2; null split size row should be excluded
SELECT c1, bitmap_min(subdivide_bitmap) AS min_value, bitmap_to_string(subdivide_bitmap) FROM ${case_db}.t2, subdivide_bitmap(${case_db}.t2.c3, c2) ORDER BY c1, min_value;

-- query 13
-- Insert data into t3 (non-nullable bitmap column)
-- @skip_result_check=true
INSERT INTO ${case_db}.t3 SELECT 1, bitmap_empty();
INSERT INTO ${case_db}.t3 SELECT 2, to_bitmap(1);
INSERT INTO ${case_db}.t3 SELECT 3, bitmap_agg(generate_series) FROM TABLE(generate_series(1, 10));
INSERT INTO ${case_db}.t3 SELECT 4, bitmap_agg(generate_series) FROM TABLE(generate_series(1, 60));
INSERT INTO ${case_db}.t3 SELECT 4, bitmap_agg(generate_series) FROM TABLE(generate_series(8589934592, 8589934610));

-- query 14
-- split_size=0 on non-nullable column
SELECT c1, bitmap_count(subdivide_bitmap) FROM ${case_db}.t3, subdivide_bitmap(${case_db}.t3.c2, 0) ORDER BY c1;

-- query 15
SELECT c1, bitmap_min(subdivide_bitmap) AS min_value, bitmap_to_string(subdivide_bitmap) FROM ${case_db}.t3, subdivide_bitmap(${case_db}.t3.c2, 1) ORDER BY c1, min_value;

-- query 16
SELECT c1, bitmap_min(subdivide_bitmap) AS min_value, bitmap_to_string(subdivide_bitmap) FROM ${case_db}.t3, subdivide_bitmap(${case_db}.t3.c2, 3) ORDER BY c1, min_value;

-- query 17
SELECT c1, bitmap_min(subdivide_bitmap) AS min_value, bitmap_to_string(subdivide_bitmap) FROM ${case_db}.t3, subdivide_bitmap(${case_db}.t3.c2, 13) ORDER BY c1, min_value;

-- query 18
SELECT c1, bitmap_min(subdivide_bitmap) AS min_value, bitmap_to_string(subdivide_bitmap) FROM ${case_db}.t3, subdivide_bitmap(${case_db}.t3.c2, 90) ORDER BY c1, min_value;

-- query 19
-- Insert data into t4 (non-nullable bitmap + non-const split size)
-- @skip_result_check=true
INSERT INTO ${case_db}.t4 SELECT 1, 0, bitmap_empty();
INSERT INTO ${case_db}.t4 SELECT 2, 1, bitmap_empty();
INSERT INTO ${case_db}.t4 SELECT 3, 1, to_bitmap(1);
INSERT INTO ${case_db}.t4 SELECT 4, 2, to_bitmap(1);
INSERT INTO ${case_db}.t4 SELECT 5, 3, bitmap_agg(generate_series) FROM TABLE(generate_series(1, 10));
INSERT INTO ${case_db}.t4 SELECT 6, 10, bitmap_agg(generate_series) FROM TABLE(generate_series(1, 60));
INSERT INTO ${case_db}.t4 SELECT 6, 10, bitmap_agg(generate_series) FROM TABLE(generate_series(8589934592, 8589934610));

-- query 20
SELECT c1, bitmap_min(subdivide_bitmap) AS min_value, bitmap_to_string(subdivide_bitmap) FROM ${case_db}.t4, subdivide_bitmap(${case_db}.t4.c3, c2) ORDER BY c1, min_value;
