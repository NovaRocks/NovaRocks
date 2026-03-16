-- Migrated from dev/test/sql/test_bitmap_functions/T/test_bitmap_binary
-- Test Objective:
-- 1. Validate bitmap_to_binary produces the expected hex representation for various bitmap types.
-- 2. Validate bitmap_from_binary round-trips: bitmap → binary → bitmap.
-- 3. Cover empty bitmap, single 32-bit value, single 64-bit value, small set, RoaringBitmap32, RoaringBitmap64.
-- 4. Validate invalid binary format returns NULL (not an error).
-- 5. Validate NULL input handling.
-- 6. Validate storing binary in a string column and reading it back.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_bm_bin;
DROP TABLE IF EXISTS ${case_db}.t_str;
CREATE TABLE ${case_db}.t_bm_bin (
  `c1` int(11) NULL COMMENT "",
  `c2` bitmap BITMAP_UNION NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`c1`)
DISTRIBUTED BY HASH(`c1`) BUCKETS 1
PROPERTIES ("replication_num" = "1");
CREATE TABLE ${case_db}.t_str (`c1` int, `c2` string);

-- query 2
-- @skip_result_check=true
insert into ${case_db}.t_bm_bin values (1, bitmap_empty());

-- query 3
-- @order_sensitive=true
-- empty bitmap binary hex: "00"
select c1, hex(bitmap_to_binary(c2)) from ${case_db}.t_bm_bin;

-- query 4
-- @order_sensitive=true
-- round-trip: empty bitmap has count 0
select c1, bitmap_count(bitmap_from_binary(bitmap_to_binary(c2))) from ${case_db}.t_bm_bin;

-- query 5
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t_bm_bin;
insert into ${case_db}.t_bm_bin values (1, to_bitmap(1));

-- query 6
-- @order_sensitive=true
-- single 32-bit bitmap binary
select c1, hex(bitmap_to_binary(c2)) from ${case_db}.t_bm_bin;

-- query 7
-- @order_sensitive=true
select c1, bitmap_to_string(bitmap_from_binary(bitmap_to_binary(c2))) from ${case_db}.t_bm_bin;

-- query 8
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t_bm_bin;
insert into ${case_db}.t_bm_bin values (1, to_bitmap(17179869184));

-- query 9
-- @order_sensitive=true
-- single 64-bit value (4GB) binary
select c1, hex(bitmap_to_binary(c2)) from ${case_db}.t_bm_bin;

-- query 10
-- @order_sensitive=true
select c1, bitmap_to_string(bitmap_from_binary(bitmap_to_binary(c2))) from ${case_db}.t_bm_bin;

-- query 11
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t_bm_bin;
insert into ${case_db}.t_bm_bin select 1, bitmap_agg(generate_series) from table(generate_series(1, 5));

-- query 12
-- @order_sensitive=true
-- set bitmap (5 elements) binary
select c1, hex(bitmap_to_binary(c2)) from ${case_db}.t_bm_bin;

-- query 13
-- @order_sensitive=true
select c1, bitmap_to_string(bitmap_from_binary(bitmap_to_binary(c2))) from ${case_db}.t_bm_bin;

-- query 14
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t_bm_bin;
insert into ${case_db}.t_bm_bin select 1, bitmap_agg(generate_series) from table(generate_series(1, 40));

-- query 15
-- @order_sensitive=true
-- RoaringBitmap32 binary
select c1, hex(bitmap_to_binary(c2)) from ${case_db}.t_bm_bin;

-- query 16
-- @order_sensitive=true
select c1, bitmap_to_string(bitmap_from_binary(bitmap_to_binary(c2))) from ${case_db}.t_bm_bin;

-- query 17
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t_bm_bin;
insert into ${case_db}.t_bm_bin select 1, bitmap_agg(generate_series) from table(generate_series(1, 20));
insert into ${case_db}.t_bm_bin select 1, bitmap_agg(generate_series) from table(generate_series(17179869184, 17179869284));

-- query 18
-- @order_sensitive=true
-- RoaringBitmap64 binary
select c1, hex(bitmap_to_binary(c2)) from ${case_db}.t_bm_bin;

-- query 19
-- @order_sensitive=true
select c1, bitmap_to_string(bitmap_from_binary(bitmap_to_binary(c2))) from ${case_db}.t_bm_bin;

-- query 20
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t_bm_bin;
insert into ${case_db}.t_bm_bin select 1, bitmap_agg(generate_series) from table(generate_series(1, 80));
insert into ${case_db}.t_bm_bin select 2, bitmap_agg(generate_series) from table(generate_series(1, 200));
insert into ${case_db}.t_bm_bin select 2, bitmap_agg(generate_series) from table(generate_series(900, 910));

-- query 21
-- @order_sensitive=true
-- Buf resize test: two bitmaps of different sizes
select c1, hex(bitmap_to_binary(c2)) from ${case_db}.t_bm_bin order by c1;

-- query 22
-- @order_sensitive=true
select c1, bitmap_to_string(bitmap_from_binary(bitmap_to_binary(c2))) from ${case_db}.t_bm_bin order by c1;

-- query 23
-- @order_sensitive=true
-- Invalid format: to_binary("1234") is not a valid bitmap binary → NULL
select bitmap_from_binary(to_binary("1234"));

-- query 24
-- @order_sensitive=true
-- Invalid format: to_binary("") is not a valid bitmap binary → NULL
select bitmap_from_binary(to_binary(""));

-- query 25
-- @order_sensitive=true
-- NULL input to bitmap_from_binary
select bitmap_from_binary(null);

-- query 26
-- @order_sensitive=true
-- NULL input to bitmap_to_binary
select bitmap_to_binary(null);

-- query 27
-- @order_sensitive=true
-- Invalid string in from_string: bitmap_to_binary on invalid bitmap → NULL
select bitmap_to_binary(bitmap_from_string("abc"));

-- query 28
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t_bm_bin;
TRUNCATE TABLE ${case_db}.t_str;
insert into ${case_db}.t_bm_bin select 1, bitmap_agg(generate_series) from table(generate_series(1, 80));
insert into ${case_db}.t_str select c1, bitmap_to_binary(c2) from ${case_db}.t_bm_bin;

-- query 29
-- @order_sensitive=true
-- Read binary from string column and reconstruct bitmap
select c1, bitmap_to_string(bitmap_from_binary(c2)) from ${case_db}.t_str;
