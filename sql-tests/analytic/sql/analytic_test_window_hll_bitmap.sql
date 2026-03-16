-- Migrated from: dev/test/sql/test_window_function/T/test_window_functions_with_hll_bitmap
-- Test Objective:
-- 1. Validate lag/lead window functions on HLL columns (NovaRocks restricts HLL to lag/lead only).
-- 2. Validate lag/lead/first_value/last_value window functions on BITMAP columns.
-- 3. Test HLL_CARDINALITY and BITMAP_COUNT wrappers around window results.
-- Note: first_value/last_value on HLL/BITMAP not tested — NovaRocks restricts these types to lag/lead and their union aggregates.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.test_ignore_nulls_page_uv (
    page_id INT NOT NULL,
    visit_date datetime NOT NULL,
    visit_users BITMAP BITMAP_UNION NOT NULL,
    click_times hll hll_union
) AGGREGATE KEY(page_id, visit_date)
DISTRIBUTED BY HASH(page_id) BUCKETS 3;

INSERT INTO ${case_db}.test_ignore_nulls_page_uv VALUES (1, '2020-06-23 01:30:30', to_bitmap(1001), hll_hash(5));
INSERT INTO ${case_db}.test_ignore_nulls_page_uv VALUES (1, '2020-06-23 01:30:30', to_bitmap(1001), hll_hash(5));
INSERT INTO ${case_db}.test_ignore_nulls_page_uv VALUES (1, '2020-06-23 01:30:30', to_bitmap(1002), hll_hash(10));
INSERT INTO ${case_db}.test_ignore_nulls_page_uv VALUES (1, '2020-06-23 02:30:30', to_bitmap(1002), hll_hash(5));

-- query 2
-- @order_sensitive=true
SELECT HLL_CARDINALITY(lag(click_times IGNORE NULLS) OVER(ORDER BY visit_date)) AS val FROM ${case_db}.test_ignore_nulls_page_uv ORDER BY val;

-- query 3
-- @order_sensitive=true
SELECT HLL_CARDINALITY(lead(click_times IGNORE NULLS) OVER(ORDER BY visit_date)) AS val FROM ${case_db}.test_ignore_nulls_page_uv ORDER BY val;

-- query 4
-- @order_sensitive=true
SELECT BITMAP_COUNT(lag(visit_users) OVER(ORDER BY visit_date)) AS val FROM ${case_db}.test_ignore_nulls_page_uv ORDER BY val;

-- query 5
-- @order_sensitive=true
SELECT BITMAP_COUNT(lead(visit_users) OVER(ORDER BY visit_date)) AS val FROM ${case_db}.test_ignore_nulls_page_uv ORDER BY val;
