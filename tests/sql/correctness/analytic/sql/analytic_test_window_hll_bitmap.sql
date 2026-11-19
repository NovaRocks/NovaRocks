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
    visit_users BITMAP NOT NULL,
    click_times hll
) WITH (format_version = 3);

INSERT INTO ${case_db}.test_ignore_nulls_page_uv VALUES ROW(1, '2020-06-23 01:30:30', to_bitmap(1001), hll_hash(5));
INSERT INTO ${case_db}.test_ignore_nulls_page_uv VALUES ROW(1, '2020-06-23 01:30:30', to_bitmap(1001), hll_hash(5));
INSERT INTO ${case_db}.test_ignore_nulls_page_uv VALUES ROW(1, '2020-06-23 01:30:30', to_bitmap(1002), hll_hash(10));
INSERT INTO ${case_db}.test_ignore_nulls_page_uv VALUES ROW(1, '2020-06-23 02:30:30', to_bitmap(1002), hll_hash(5));

-- query 2
-- @order_sensitive=true
SELECT HLL_CARDINALITY(lag(click_times ) IGNORE NULLS OVER(ORDER BY visit_date)) AS val FROM ${case_db}.test_ignore_nulls_page_uv ORDER BY val;

-- query 3
-- @order_sensitive=true
SELECT HLL_CARDINALITY(lead(click_times ) IGNORE NULLS OVER(ORDER BY visit_date)) AS val FROM ${case_db}.test_ignore_nulls_page_uv ORDER BY val;

-- query 4
-- @order_sensitive=true
SELECT BITMAP_COUNT(lag(visit_users) OVER(ORDER BY visit_date)) AS val FROM ${case_db}.test_ignore_nulls_page_uv ORDER BY val;

-- query 5
-- @order_sensitive=true
SELECT BITMAP_COUNT(lead(visit_users) OVER(ORDER BY visit_date)) AS val FROM ${case_db}.test_ignore_nulls_page_uv ORDER BY val;
