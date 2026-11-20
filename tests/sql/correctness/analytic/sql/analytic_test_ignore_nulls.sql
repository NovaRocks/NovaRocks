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

-- Migrated from: dev/test/sql/test_window_function/T/test_ignore_nulls
-- Test Objective:
-- 1. Validate IGNORE NULLS for first_value/last_value over ORDER BY and PARTITION BY windows.
-- 2. Validate IGNORE NULLS for lead/lag with various offsets and partitions.
-- 3. Validate lead/lag IGNORE NULLS when all values in a partition are NULL.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}."t0" (
  "v1" int(11) ,
  "v2" int(11) ,
  "v3" int(11)
) WITH (format_version = 3);

INSERT INTO ${case_db}."t0" (v1, v2, v3) values
    ROW(1, 1, 1),
    ROW(1, 2, NULL),
    ROW(1, 3, 3),
    ROW(1, 4, NULL),
    ROW(1, 5, 5),
    ROW(1, 6, NULL),
    ROW(2, 1, NULL),
    ROW(2, 2, 2),
    ROW(2, 3, NULL),
    ROW(2, 4, 4),
    ROW(2, 5, NULL),
    ROW(2, 6, 6),
    ROW(3, 1, 1),
    ROW(3, 2, 2),
    ROW(3, 3, NULL),
    ROW(3, 4, NULL),
    ROW(3, 5, 5),
    ROW(3, 6, 6),
    ROW(4, 1, NULL),
    ROW(4, 2, NULL),
    ROW(4, 3, 3),
    ROW(4, 4, 4),
    ROW(4, 5, NULL),
    ROW(4, 6, NULL);

-- query 2
-- @order_sensitive=true
SELECT v1, v2, v3, first_value(v3 ) IGNORE NULLS OVER(ORDER BY v1, v2) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 3
-- @order_sensitive=true
SELECT v1, v2, v3, first_value(v3 ) IGNORE NULLS OVER(ORDER BY v1, v2 rows between 1 preceding and 1 following) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 4
-- @order_sensitive=true
SELECT v1, v2, v3, first_value(v3 ) IGNORE NULLS OVER(partition BY v1 ORDER BY v2) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 5
-- @order_sensitive=true
SELECT v1, v2, v3, first_value(v3 ) IGNORE NULLS OVER(partition BY v1 ORDER BY v2 rows between 1 preceding and 1 following) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 6
-- @order_sensitive=true
SELECT v1, v2, v3, last_value(v3 ) IGNORE NULLS OVER(ORDER BY v1, v2) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 7
-- @order_sensitive=true
SELECT v1, v2, v3, last_value(v3 ) IGNORE NULLS OVER(ORDER BY v1, v2 rows between 1 preceding and 1 following) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 8
-- @order_sensitive=true
SELECT v1, v2, v3, last_value(v3 ) IGNORE NULLS OVER(partition BY v1 ORDER BY v2) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 9
-- @order_sensitive=true
SELECT v1, v2, v3, last_value(v3 ) IGNORE NULLS OVER(partition BY v1 ORDER BY v2 rows between 1 preceding and 1 following) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 10
-- @order_sensitive=true
SELECT v1, v2, v3, lead(v3 , 1) IGNORE NULLS OVER(ORDER BY v1, v2) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 11
-- @order_sensitive=true
SELECT v1, v2, v3, lead(v3 , 1) IGNORE NULLS OVER(partition BY v1 ORDER BY v2) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 12
-- @order_sensitive=true
SELECT v1, v2, v3, lead(v3 , 2) IGNORE NULLS OVER(ORDER BY v1, v2) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 13
-- @order_sensitive=true
SELECT v1, v2, v3, lead(v3 , 2) IGNORE NULLS OVER(partition BY v1 ORDER BY v2) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 14
-- @order_sensitive=true
SELECT v1, v2, v3, lag(v3 , 1) IGNORE NULLS OVER(ORDER BY v1, v2) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 15
-- @order_sensitive=true
SELECT v1, v2, v3, lag(v3 , 1) IGNORE NULLS OVER(partition BY v1 ORDER BY v2) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 16
-- @order_sensitive=true
SELECT v1, v2, v3, lag(v3 , 2) IGNORE NULLS OVER(ORDER BY v1, v2) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 17
-- @order_sensitive=true
SELECT v1, v2, v3, lag(v3 , 2) IGNORE NULLS OVER(partition BY v1 ORDER BY v2) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 18
-- @skip_result_check=true
CREATE TABLE ${case_db}."t_all_null" (
  "v1" int(11) ,
  "v2" int(11) ,
  "v3" int(11)
) WITH (format_version = 3);

INSERT INTO ${case_db}."t_all_null" (v1, v2, v3) values
    ROW(1, 1, NULL),
    ROW(1, 2, NULL),
    ROW(1, 3, NULL),
    ROW(1, 4, NULL),
    ROW(1, 5, NULL),
    ROW(1, 6, NULL),
    ROW(2, 1, NULL),
    ROW(2, 2, NULL),
    ROW(2, 3, NULL),
    ROW(2, 4, NULL),
    ROW(2, 5, NULL),
    ROW(2, 6, NULL),
    ROW(3, 1, NULL),
    ROW(3, 2, NULL),
    ROW(3, 3, NULL),
    ROW(3, 4, NULL),
    ROW(3, 5, NULL),
    ROW(3, 6, NULL),
    ROW(4, 1, NULL),
    ROW(4, 2, NULL),
    ROW(4, 3, NULL),
    ROW(4, 4, NULL),
    ROW(4, 5, NULL),
    ROW(4, 6, NULL);

-- query 19
-- @order_sensitive=true
SELECT v1, v2, v3, lead(v3 , 1) IGNORE NULLS OVER(partition BY v1 ORDER BY v2) FROM ${case_db}.t_all_null ORDER BY v1, v2;

-- query 20
-- @order_sensitive=true
SELECT v1, v2, v3, lag(v3 , 1) IGNORE NULLS OVER(partition BY v1 ORDER BY v2) FROM ${case_db}.t_all_null ORDER BY v1, v2;
