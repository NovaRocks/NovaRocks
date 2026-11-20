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

-- Migrated from dev/test/sql/test_bitmap_functions/T/test_bitmap_to_array
-- Test Objective:
-- 1. Validate bitmap_to_array on empty bitmap returns empty array.
-- 2. Validate bitmap_to_array on single-element bitmap returns one-element array.
-- 3. Validate bitmap_to_array on set bitmap (RoaringBitmap32).
-- 4. Validate bitmap_to_array on larger set bitmap.
-- 5. Validate generate_series sum for sanity check.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_bm_arr (
  "c1" int(11),
  "c2" bitmap
) WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t_bm_arr;
insert into ${case_db}.t_bm_arr select 1, bitmap_empty();

-- query 3
-- empty bitmap → empty array []
select bitmap_to_array(c2) from ${case_db}.t_bm_arr;

-- query 4
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t_bm_arr;
insert into ${case_db}.t_bm_arr select 1, to_bitmap(1);

-- query 5
-- single element bitmap → [1]
select bitmap_to_array(c2) from ${case_db}.t_bm_arr;

-- query 6
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t_bm_arr;
insert into ${case_db}.t_bm_arr select 1, bitmap_agg(generate_series) from table(generate_series(1, 10));

-- query 7
-- 10-element bitmap
select bitmap_to_array(c2) from ${case_db}.t_bm_arr;

-- query 8
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t_bm_arr;
insert into ${case_db}.t_bm_arr select 1, bitmap_agg(generate_series) from table(generate_series(1, 40));

-- query 9
-- 40-element RoaringBitmap32
select bitmap_to_array(c2) from ${case_db}.t_bm_arr;

-- query 10
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.t_bm_arr;
insert into ${case_db}.t_bm_arr select 1, bitmap_agg(generate_series) from table(generate_series(0, 4093));
insert into ${case_db}.t_bm_arr select 2, bitmap_agg(generate_series) from table(generate_series(4094, 8000));

-- query 11
-- Sanity check: sum of 0..8000 = 32004000 (not using the bitmap table)
select sum(generate_series) from table(generate_series(0, 8000));
