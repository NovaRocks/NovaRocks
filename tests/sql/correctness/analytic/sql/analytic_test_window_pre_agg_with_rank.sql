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

-- Migrated from: dev/test/sql/test_window_function/T/test_window_pre_agg_with_rank
--                dev/test/sql/test_window_function/T/test_window_pre_agg_with_bitmap_union_count
-- Test Objective:
-- 1. Validate row_number() OVER (PARTITION BY ... ORDER BY ...) with pre-aggregation patterns.
-- 2. Cover JOIN between t1 and t2 with row_number <= predicate filter.
-- 3. Validate sum() / avg() OVER (PARTITION BY ...) combined with row_number pre-agg pattern.
-- 4. Validate COUNT(*) + ROW_NUMBER with non-deterministic ORDER BY RAND() — result not snapshotted.
-- 5. Validate bitmap_union_count(to_bitmap(uid)) OVER (PARTITION BY k) combined with rank() (crash guard).

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
    c1 INT NOT NULL,
    c2 INT NOT NULL,
    c3 INT NOT NULL
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t2 (
    c4 INT NOT NULL,
    c5 INT NOT NULL,
    c6 INT NOT NULL
) WITH (format_version = 3);
INSERT INTO ${case_db}.t1 (c1, c2, c3) VALUES
ROW(1, 1, 1), ROW(2, 1, 11), ROW(3, 1, 111),
ROW(4, 2, 2), ROW(5, 2, 22), ROW(6, 2, 222);
INSERT INTO ${case_db}.t2 (c4, c5, c6) VALUES
ROW(1, 1, 1), ROW(2, 1, 11), ROW(3, 1, 111),
ROW(4, 2, 2), ROW(5, 2, 22), ROW(6, 2, 222);

-- query 2
-- case1: row_number <= 2 without agg function
-- @order_sensitive=true
SELECT *
FROM (
    SELECT c1, c2, c3, c4, c5, c6,
           row_number() OVER (PARTITION BY c2 ORDER BY c1, c6) AS _rowid
    FROM (SELECT * FROM ${case_db}.t1 JOIN ${case_db}.t2 ON c2 = c5) t3
) t4
WHERE _rowid <= 2 ORDER BY c4;

-- query 3
-- case2: pre-agg with sum
-- @order_sensitive=true
SELECT *
FROM (
    SELECT c1, c2, c3, c4, c5, c6,
           row_number() OVER (PARTITION BY t3.c2 ORDER BY c1, c6) AS _rowid,
           sum(t3.c2) OVER (PARTITION BY t3.c2) AS _sum
    FROM (SELECT * FROM ${case_db}.t1 JOIN ${case_db}.t2 ON c2 = c5) t3
) t4
WHERE _rowid <= 2 ORDER BY c4, c6;

-- query 4
-- case3: pre-agg with sum and avg
-- @order_sensitive=true
SELECT *
FROM (
    SELECT c1, c2, c3, c4, c5, c6,
           row_number() OVER (PARTITION BY c2 ORDER BY c1, c6) AS _rowid,
           sum(c2) OVER (PARTITION BY t3.c2) AS _sum,
           avg(c2) OVER (PARTITION BY t3.c2) AS _avg
    FROM (SELECT * FROM ${case_db}.t1 JOIN ${case_db}.t2 ON c2 = c5) t3
) t4
WHERE _rowid <= 2 ORDER BY c4, c6;

-- query 5
-- case4: COUNT(*) + ROW_NUMBER with same partition, ORDER BY RAND() — non-deterministic, skip snapshot
-- @skip_result_check=true
SELECT 1
FROM (
    SELECT
        COUNT(*) OVER (PARTITION BY c1) AS _count,
        ROW_NUMBER() OVER (PARTITION BY c1 ORDER BY RAND()) AS rn
    FROM ${case_db}.t1
) AS x
WHERE rn <= 2;

-- query 6
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_rank_bitmap_crash (
    k INT,
    ts INT,
    uid BIGINT
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_rank_bitmap_crash VALUES
ROW(1, 100, 1), ROW(1, 90, 2), ROW(1, 80, NULL), ROW(1, 70, 3),
ROW(2, 100, 4), ROW(2, 90, NULL), ROW(2, 80, 5);

-- query 7
-- bitmap_union_count combined with rank() — crash guard test
-- @order_sensitive=true
SELECT * FROM (
    SELECT
        k, ts,
        rank() OVER (PARTITION BY k ORDER BY ts DESC) AS rk,
        bitmap_union_count(to_bitmap(uid)) OVER (PARTITION BY k) AS uv
    FROM ${case_db}.t_rank_bitmap_crash
) s
ORDER BY rk, k, ts DESC
LIMIT 3;
