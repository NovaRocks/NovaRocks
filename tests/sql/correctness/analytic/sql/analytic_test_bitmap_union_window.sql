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

-- Migrated from: dev/test/sql/test_window_function/T/test_bitmap_union_window_function
-- Test Objective:
-- 1. Validate bitmap_union as a window function: global window (over()), per-partition, partial partitions.
-- 2. Cover null and empty bitmap values in window aggregation.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1(
    c1 int,
    c2 bitmap
) WITH (format_version = 3);

INSERT INTO ${case_db}.t1 VALUES ROW(1, to_bitmap(11)), ROW(2, to_bitmap(22)), ROW(3, null), ROW(4, bitmap_empty()), ROW(5, to_bitmap(55));

-- query 2
-- @order_sensitive=true
SELECT c1, bitmap_to_string(bitmap_union(c2) over()) FROM ${case_db}.t1 ORDER BY c1;

-- query 3
-- Row c1=3 returns NULL because the partition's only bitmap input is NULL
-- (SQL/StarRocks semantics: aggregate over an all-NULL group yields NULL,
-- not the per-element identity).
-- @order_sensitive=true
SELECT c1, bitmap_to_string(bitmap_union(c2) over(partition by c1)) FROM ${case_db}.t1 ORDER BY c1;

-- query 4
-- @order_sensitive=true
SELECT c1, bitmap_to_string(bitmap_union(c2) over(partition by c1%2)) FROM ${case_db}.t1 ORDER BY c1;

-- query 5
-- Default frame per SQL standard / StarRocks: when ORDER BY is given with no
-- explicit window, the implicit frame is RANGE BETWEEN UNBOUNDED PRECEDING
-- AND CURRENT ROW (running cumulative union over the ordered peers).
-- @order_sensitive=true
SELECT c1, bitmap_to_string(bitmap_union(c2) over(partition by c1%2 order by c1)) FROM ${case_db}.t1 ORDER BY c1;
