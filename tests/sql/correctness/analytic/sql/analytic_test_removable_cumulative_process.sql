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

-- Migrated from: dev/test/sql/test_window_function/T/test_removable_cumulative_process
-- Test Objective:
-- 1. Validate COUNT/SUM/AVG window functions with ROWS BETWEEN sliding frames (preceding/following).
-- 2. Cover nullable and non-nullable integer columns for each aggregate.
-- 3. Cover all five frame variants: [5 PREC, 2 PREC], [2 PREC, 2 FOL], [2 FOL, 5 FOL],
--    [2 PREC, CURRENT ROW], [CURRENT ROW, 2 FOL].
-- 4. Validate MIN() sliding frame on string (VARCHAR) column.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
    v1 int(11) ,
    v2 int(11) ,
    v3 int(11) NOT NULL,
    v4 int(11)
) WITH (format_version = 3);
INSERT INTO ${case_db}.t1 VALUES
ROW(1, 1, 1, NULL), ROW(1, 1, 2, NULL), ROW(1, NULL, 3, NULL), ROW(1, NULL, 4, NULL),
ROW(1, 2, 5, NULL), ROW(1, 2, 6, NULL), ROW(1, NULL, 7, NULL), ROW(1, NULL, 8, NULL),
ROW(2, 3, 9, NULL), ROW(2, 3, 10, NULL), ROW(2, NULL, 11, NULL), ROW(2, NULL, 12, NULL),
ROW(2, 4, 13, NULL), ROW(2, 4, 14, NULL), ROW(2, NULL, 15, NULL), ROW(2, NULL, 16, NULL),
ROW(NULL, 3, 17, NULL), ROW(NULL, 3, 18, NULL), ROW(NULL, NULL, 19, NULL), ROW(NULL, NULL, 20, NULL),
ROW(NULL, 4, 21, NULL), ROW(NULL, 4, 22, NULL), ROW(NULL, NULL, 23, NULL), ROW(NULL, NULL, 24, NULL);

-- ========================================================= COUNT =========================================================

-- query 2
-- COUNT(Nullable Column)
-- @order_sensitive=true
SELECT v3, v1, COUNT(v1) OVER(ORDER BY v3 ROWS BETWEEN 5 PRECEDING AND 2 PRECEDING) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 3
-- @order_sensitive=true
SELECT v3, v1, COUNT(v1) OVER(ORDER BY v3 ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 4
-- @order_sensitive=true
SELECT v3, v1, COUNT(v1) OVER(ORDER BY v3 ROWS BETWEEN 2 FOLLOWING AND 5 FOLLOWING) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 5
-- @order_sensitive=true
SELECT v3, v1, COUNT(v1) OVER(ORDER BY v3 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 6
-- @order_sensitive=true
SELECT v3, v1, COUNT(v1) OVER(ORDER BY v3 ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 7
-- COUNT(Not Nullable Column)
-- @order_sensitive=true
SELECT v3, COUNT(v3) OVER(ORDER BY v3 ROWS BETWEEN 5 PRECEDING AND 2 PRECEDING) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 8
-- @order_sensitive=true
SELECT v3, COUNT(v3) OVER(ORDER BY v3 ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 9
-- @order_sensitive=true
SELECT v3, COUNT(v3) OVER(ORDER BY v3 ROWS BETWEEN 2 FOLLOWING AND 5 FOLLOWING) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 10
-- @order_sensitive=true
SELECT v3, COUNT(v3) OVER(ORDER BY v3 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 11
-- @order_sensitive=true
SELECT v3, COUNT(v3) OVER(ORDER BY v3 ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- ========================================================= SUM =========================================================

-- query 12
-- SUM(Nullable Column)
-- @order_sensitive=true
SELECT v3, v1, SUM(v1) OVER(ORDER BY v3 ROWS BETWEEN 5 PRECEDING AND 2 PRECEDING) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 13
-- @order_sensitive=true
SELECT v3, v1, SUM(v1) OVER(ORDER BY v3 ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 14
-- @order_sensitive=true
SELECT v3, v1, SUM(v1) OVER(ORDER BY v3 ROWS BETWEEN 2 FOLLOWING AND 5 FOLLOWING) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 15
-- @order_sensitive=true
SELECT v3, v1, SUM(v1) OVER(ORDER BY v3 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 16
-- @order_sensitive=true
SELECT v3, v1, SUM(v1) OVER(ORDER BY v3 ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 17
-- SUM(Not Nullable Column)
-- @order_sensitive=true
SELECT v3, SUM(v3) OVER(ORDER BY v3 ROWS BETWEEN 5 PRECEDING AND 2 PRECEDING) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 18
-- @order_sensitive=true
SELECT v3, SUM(v3) OVER(ORDER BY v3 ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 19
-- @order_sensitive=true
SELECT v3, SUM(v3) OVER(ORDER BY v3 ROWS BETWEEN 2 FOLLOWING AND 5 FOLLOWING) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 20
-- @order_sensitive=true
SELECT v3, SUM(v3) OVER(ORDER BY v3 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 21
-- @order_sensitive=true
SELECT v3, SUM(v3) OVER(ORDER BY v3 ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- ========================================================= AVG =========================================================

-- query 22
-- AVG(Nullable Column)
-- @order_sensitive=true
SELECT v3, v1, AVG(v1) OVER(ORDER BY v3 ROWS BETWEEN 5 PRECEDING AND 2 PRECEDING) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 23
-- @order_sensitive=true
SELECT v3, v1, AVG(v1) OVER(ORDER BY v3 ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 24
-- @order_sensitive=true
SELECT v3, v1, AVG(v1) OVER(ORDER BY v3 ROWS BETWEEN 2 FOLLOWING AND 5 FOLLOWING) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 25
-- @order_sensitive=true
SELECT v3, v1, AVG(v1) OVER(ORDER BY v3 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 26
-- @order_sensitive=true
SELECT v3, v1, AVG(v1) OVER(ORDER BY v3 ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 27
-- AVG(Not Nullable Column)
-- @order_sensitive=true
SELECT v3, AVG(v3) OVER(ORDER BY v3 ROWS BETWEEN 5 PRECEDING AND 2 PRECEDING) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 28
-- @order_sensitive=true
SELECT v3, AVG(v3) OVER(ORDER BY v3 ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 29
-- @order_sensitive=true
SELECT v3, AVG(v3) OVER(ORDER BY v3 ROWS BETWEEN 2 FOLLOWING AND 5 FOLLOWING) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 30
-- @order_sensitive=true
SELECT v3, AVG(v3) OVER(ORDER BY v3 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- query 31
-- @order_sensitive=true
SELECT v3, AVG(v3) OVER(ORDER BY v3 ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS CNT FROM ${case_db}.t1 ORDER BY v3;

-- ========================================================= STRING MIN =========================================================

-- query 32
-- @skip_result_check=true
CREATE TABLE ${case_db}.t2 (
    v1 int(11) ,
    v2 STRING
) WITH (format_version = 3);
INSERT INTO ${case_db}.t2 VALUES
ROW(1, '1'), ROW(2, NULL), ROW(3, '2'), ROW(4, '2'), ROW(5, '2'),
ROW(6, '3'), ROW(7, '3'), ROW(8, '200'), ROW(9, '40'), ROW(10, '50'),
ROW(11, '60'), ROW(12, '10'), ROW(13, '20');

-- query 33
-- @order_sensitive=true
SELECT v1, v2, MIN(v2) OVER (ORDER BY v1 DESC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS _min FROM ${case_db}.t2 ORDER BY v1 DESC;
