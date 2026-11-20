-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

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
CREATE TABLE ${case_db}.t_bm_bin (
  "c1" int(11),
  "c2" bitmap
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_str ("c1" int, "c2" string) WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
insert into ${case_db}.t_bm_bin values ROW(1, bitmap_empty());

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
insert into ${case_db}.t_bm_bin values ROW(1, to_bitmap(1));

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
insert into ${case_db}.t_bm_bin values ROW(1, to_bitmap(17179869184));

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
insert into ${case_db}.t_bm_bin values ROW(1, bitmap_from_string('1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,17179869184,17179869185,17179869186,17179869187,17179869188,17179869189,17179869190,17179869191,17179869192,17179869193,17179869194,17179869195,17179869196,17179869197,17179869198,17179869199,17179869200,17179869201,17179869202,17179869203,17179869204,17179869205,17179869206,17179869207,17179869208,17179869209,17179869210,17179869211,17179869212,17179869213,17179869214,17179869215,17179869216,17179869217,17179869218,17179869219,17179869220,17179869221,17179869222,17179869223,17179869224,17179869225,17179869226,17179869227,17179869228,17179869229,17179869230,17179869231,17179869232,17179869233,17179869234,17179869235,17179869236,17179869237,17179869238,17179869239,17179869240,17179869241,17179869242,17179869243,17179869244,17179869245,17179869246,17179869247,17179869248,17179869249,17179869250,17179869251,17179869252,17179869253,17179869254,17179869255,17179869256,17179869257,17179869258,17179869259,17179869260,17179869261,17179869262,17179869263,17179869264,17179869265,17179869266,17179869267,17179869268,17179869269,17179869270,17179869271,17179869272,17179869273,17179869274,17179869275,17179869276,17179869277,17179869278,17179869279,17179869280,17179869281,17179869282,17179869283,17179869284'));

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
insert into ${case_db}.t_bm_bin values ROW(1, bitmap_from_string('1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80'));
insert into ${case_db}.t_bm_bin values ROW(2, bitmap_from_string('1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,900,901,902,903,904,905,906,907,908,909,910'));

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
select bitmap_from_binary(to_binary('1234'));

-- query 24
-- @order_sensitive=true
-- Invalid format: to_binary("") is not a valid bitmap binary → NULL
select bitmap_from_binary(to_binary(''));

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
select bitmap_to_binary(bitmap_from_string('abc'));

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
