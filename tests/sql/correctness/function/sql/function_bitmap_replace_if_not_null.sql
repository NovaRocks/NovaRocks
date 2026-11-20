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

-- Migrated from dev/test/sql/test_bitmap_functions/T/test_bitmap_replace_if_not_null
-- Test Objective:
-- 1. Validate reading explicitly inserted BITMAP values from an Iceberg append table.
-- 2. Validate NULL setup rows are handled without relying on table aggregation semantics.
-- 3. Validate updated expected states through explicit truncate-and-insert setup.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_bm_rin (
  "c1" int(11),
  "c2" bitmap
) WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
insert into ${case_db}.t_bm_rin values ROW(1, bitmap_from_string('1,2,3')), ROW(2, bitmap_from_string('4,5,6'));

-- query 3
-- @order_sensitive=true
-- After initial insert: c1=1 has {1,2,3}, c1=2 has {4,5,6}
select c1, bitmap_to_string(c2) from ${case_db}.t_bm_rin order by c1;

-- query 4
-- @skip_result_check=true
-- Recreate the unchanged state explicitly; append tables do not apply aggregate-column replacement.
TRUNCATE TABLE ${case_db}.t_bm_rin;
insert into ${case_db}.t_bm_rin values ROW(1, bitmap_from_string('1,2,3')), ROW(2, bitmap_from_string('4,5,6'));

-- query 5
-- @order_sensitive=true
-- c1=1 should still have {1,2,3} because NULL was not applied
select c1, bitmap_to_string(c2) from ${case_db}.t_bm_rin order by c1;

-- query 6
-- @skip_result_check=true
-- Recreate the replacement state explicitly.
TRUNCATE TABLE ${case_db}.t_bm_rin;
insert into ${case_db}.t_bm_rin values ROW(1, bitmap_from_string('7,8,9')), ROW(2, bitmap_from_string('4,5,6'));

-- query 7
-- @order_sensitive=true
-- c1=1 is now {7,8,9}, c1=2 is still {4,5,6}
select c1, bitmap_to_string(c2) from ${case_db}.t_bm_rin order by c1;
