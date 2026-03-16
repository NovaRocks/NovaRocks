-- Migrated from: dev/test/sql/test_window_function/T/test_bitmap_union_window_function
-- Test Objective:
-- 1. Validate bitmap_union as a window function: global window (over()), per-partition, partial partitions.
-- 2. Cover null and empty bitmap values in window aggregation.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1(
    c1 int,
    c2 bitmap
)
PRIMARY KEY(c1)
DISTRIBUTED BY HASH(c1) BUCKETS 1
PROPERTIES("replication_num"="1");

INSERT INTO ${case_db}.t1 VALUES (1, to_bitmap(11)), (2, to_bitmap(22)), (3, null), (4, bitmap_empty()), (5, to_bitmap(55));

-- query 2
-- @order_sensitive=true
SELECT c1, bitmap_to_string(bitmap_union(c2) over()) FROM ${case_db}.t1 ORDER BY c1;

-- query 3
-- @order_sensitive=true
SELECT c1, bitmap_to_string(bitmap_union(c2) over(partition by c1)) FROM ${case_db}.t1 ORDER BY c1;

-- query 4
-- @order_sensitive=true
SELECT c1, bitmap_to_string(bitmap_union(c2) over(partition by c1%2)) FROM ${case_db}.t1 ORDER BY c1;

-- query 5
-- @order_sensitive=true
SELECT c1, bitmap_to_string(bitmap_union(c2) over(partition by c1%2 order by c1)) FROM ${case_db}.t1 ORDER BY c1;
