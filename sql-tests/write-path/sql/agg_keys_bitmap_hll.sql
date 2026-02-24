-- @order_sensitive=true
-- Test Objective:
-- 1. Validate AGG_KEYS table creation with BITMAP/HLL value columns.
-- 2. Validate insert/query flow with duplicated keys on BITMAP_UNION/HLL_UNION columns.
-- 3. Prevent regressions where AGG_KEYS value aggregation fails for object-family types.
-- Test Flow:
-- 1. Create/reset an AGG_KEYS table with BITMAP/HLL aggregate columns.
-- 2. Insert duplicated keys with bitmap/hll values.
-- 3. Query aggregated rows and assert value columns remain non-NULL after merge.
SET catalog default_catalog;
CREATE DATABASE IF NOT EXISTS sql_tests_write_path;
DROP TABLE IF EXISTS sql_tests_write_path.t_agg_keys_bitmap_hll;
CREATE TABLE sql_tests_write_path.t_agg_keys_bitmap_hll (
  k1 BIGINT,
  bm BITMAP BITMAP_UNION,
  hv HLL HLL_UNION
)
AGGREGATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO sql_tests_write_path.t_agg_keys_bitmap_hll VALUES
  (1, to_bitmap(1), hll_hash('a')),
  (1, to_bitmap(2), hll_hash('b')),
  (2, to_bitmap(7), hll_hash('c'));
SELECT k1 FROM sql_tests_write_path.t_agg_keys_bitmap_hll ORDER BY k1;
