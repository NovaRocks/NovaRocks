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

-- Migrated from: dev/test/sql/test_window_function/T/test_cume_window_function
-- Test Objective:
-- 1. Validate CUME_DIST() window function with various ORDER BY, PARTITION BY, and mixed window specs.
-- 2. Validate PERCENT_RANK() window function similarly.
-- 3. Validate combining CUME_DIST/PERCENT_RANK with other window functions (ROW_NUMBER, RANK, LAG, etc.).
-- 4. Validate combining CUME_DIST/PERCENT_RANK with frame-based aggregation (SUM, AVG, MIN over rows).
-- Note: ORDER BY k1, k2 added to unordered SELECT * queries for deterministic result snapshots.

-- query 1
-- @skip_result_check=true
CREATE TABLE IF NOT EXISTS ${case_db}.cume_test (
  k1 INT,
  k2 INT,
  v1 INT,
  v2 DOUBLE,
  v3 DATE,
  v4 VARCHAR(20)
) WITH (format_version = 3);
INSERT INTO ${case_db}.cume_test (k1, k2, v1, v2, v3, v4)
VALUES ROW(1, 10, 15, 2.5, '2023-01-01', 'apple'),
       ROW(1, 2, 15, 2.5, '2023-01-02', 'banana'),
       ROW(1, 8, 15, 3.5, '2023-01-03', 'cherry'),
       ROW(1, 7, 20, 3.5, '2023-01-04', 'date'),
       ROW(2, 6, 20, 3.5, '2023-01-05', 'fig'),
       ROW(2, 5, 20, 4.5, '2023-01-06', 'grape'),
       ROW(2, 4, 20, 5.5, '2023-01-07', 'honeydew'),
       ROW(3, 3, 20, 5.5, '2023-01-08', 'kiwi'),
       ROW(3, 2, 20, 5.5, '2023-01-09', 'lemon'),
       ROW(4, 2, 25, 5.5, '2023-01-10', 'mango'),
       ROW(4, 10, 30, 5.5, '2023-01-11', 'nectarine'),
       ROW(4, 9, 30, 5.5, '2023-01-12', 'orange'),
       ROW(4, 8, 35, 6.5, '2023-01-13', 'papaya'),
       ROW(4, 7, 35, 6.5, '2023-01-14', 'pineapple'),
       ROW(4, 6, 35, 7.5, '2023-01-15', 'quince'),
       ROW(4, 5, 35, 7.5, '2023-01-16', 'raspberry'),
       ROW(5, 4, 35, 8.5, '2023-01-17', 'strawberry'),
       ROW(5, 3, 40, 8.5, '2023-01-18', 'tangerine'),
       ROW(5, 2, 40, 8.5, '2023-01-19', 'ugli fruit'),
       ROW(5, 1, 40, 8.5, '2023-01-20', 'vanilla'),
       ROW(5, 10, 45, 9.5, '2023-01-21', 'watermelon'),
       ROW(6, 9, 45, 10.5, '2023-01-22', 'xigua'),
       ROW(6, 8, 50, 11.5, '2023-01-23', 'yumberry'),
       ROW(6, 7, 50, 12.5, '2023-01-24', 'zucchini'),
       ROW(6, 6, 55, 13.5, '2023-01-25', 'acerola'),
       ROW(6, 5, 60, 13.5, '2023-01-26', 'bilberry'),
       ROW(7, 4, 60, 13.5, '2023-01-27', 'cloudberry'),
       ROW(7, 3, 60, 14.5, '2023-01-28', 'dewberry'),
       ROW(8, 2, null, 2.5, '2023-01-29', null),
       ROW(8, 8, 15, 3.5, '2023-01-30', null),
       ROW(8, 7, null, 3.5, '2023-01-31', 'date'),
       ROW(8, 6, null, null, '2023-02-01', 'fig'),
       ROW(9, 5, 20, 4.5, '2023-02-02', 'grape'),
       ROW(9, 4, 20, 5.5, '2023-02-07', null),
       ROW(10, 3, 20, null, null, 'kiwi'),
       ROW(10, 2, 20, 5.5, '2023-02-09', 'lemon'),
       ROW(10, 2, 25, 5.5, '2023-02-10', null),
       ROW(11, 10, null, 5.5, null, null),
       ROW(11, 9, 30, 5.5, '2023-02-12', 'orange'),
       ROW(12, 8, 35, 6.5, '2023-02-13', null),
       ROW(13, 7, 35, 6.5, '2023-02-14', 'pineapple'),
       ROW(13, 6, null, 7.5, '2023-02-15', 'quince'),
       ROW(13, 5, 35, 7.5, '2023-02-16', null),
       ROW(13, 4, 35, 8.5, null, null),
       ROW(13, 3, 40, 8.5, '2023-02-18', 'tangerine'),
       ROW(14, 2, 40, null, '2023-02-19', 'ugli fruit'),
       ROW(14, 1, 40, null, '2023-02-20', 'vanilla'),
       ROW(14, 10, 45, 9.5, '2023-02-21', 'watermelon'),
       ROW(15, 9, 45, 10.5, null, 'xigua'),
       ROW(16, 8, 50, 11.5, null, null),
       ROW(16, 7, 50, 12.5, '2023-02-24', null),
       ROW(16, 6, 55, 13.5, '2023-02-25', 'acerola'),
       ROW(16, 5, null, 13.5, '2023-02-26', 'bilberry'),
       ROW(17, 4, 60, 13.5, null, 'cloudberry'),
       ROW(17, 3, 60, 14.5, '2023-02-28', 'dewberry');

-- query 2
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (ORDER BY v1) AS cume_dist_v1 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 3
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (ORDER BY v2 DESC) AS cume_dist_v2 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 4
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (ORDER BY v3) AS cume_dist_v3 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 5
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (ORDER BY v4) AS cume_dist_v4 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 6
-- @order_sensitive=true
SELECT *, PERCENT_RANK() OVER (ORDER BY v1) AS percent_rank_v1 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 7
-- @order_sensitive=true
SELECT *, PERCENT_RANK() OVER (ORDER BY v2) AS percent_rank_v2 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 8
-- @order_sensitive=true
SELECT *, PERCENT_RANK() OVER (ORDER BY v3 DESC) AS percent_rank_v3 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 9
-- @order_sensitive=true
SELECT *, PERCENT_RANK() OVER (ORDER BY v4) AS percent_rank_v4 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 10
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (ORDER BY v1 DESC) AS cume_dist_v1 FROM ${case_db}.cume_test WHERE v1 IS NOT NULL ORDER BY k1, k2;

-- query 11
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (ORDER BY v2) AS cume_dist_v2 FROM ${case_db}.cume_test WHERE v2 IS NOT NULL ORDER BY k1, k2;

-- query 12
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (ORDER BY v3) AS cume_dist_v3 FROM ${case_db}.cume_test WHERE v3 IS NOT NULL ORDER BY k1, k2;

-- query 13
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (ORDER BY v4) AS cume_dist_v4 FROM ${case_db}.cume_test WHERE v4 IS NOT NULL ORDER BY k1, k2;

-- query 14
-- @order_sensitive=true
SELECT *, PERCENT_RANK() OVER (ORDER BY v1) AS percent_rank_v1 FROM ${case_db}.cume_test WHERE v1 IS NOT NULL ORDER BY k1, k2;

-- query 15
-- @order_sensitive=true
SELECT *, PERCENT_RANK() OVER (ORDER BY v2) AS percent_rank_v2 FROM ${case_db}.cume_test WHERE v2 IS NOT NULL ORDER BY k1, k2;

-- query 16
-- @order_sensitive=true
SELECT *, PERCENT_RANK() OVER (ORDER BY v3) AS percent_rank_v3 FROM ${case_db}.cume_test WHERE v3 IS NOT NULL ORDER BY k1, k2;

-- query 17
-- @order_sensitive=true
SELECT *, PERCENT_RANK() OVER (ORDER BY v4 DESC) AS percent_rank_v4 FROM ${case_db}.cume_test WHERE v4 IS NOT NULL ORDER BY k1, k2;

-- query 18
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (ORDER BY k1, k2) AS cume_dist_k1_k2 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 19
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (ORDER BY k1 ASC, k2 DESC) AS cume_dist_k1_asc_k2_desc FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 20
-- @order_sensitive=true
SELECT *, PERCENT_RANK() OVER (ORDER BY k1, k2) AS percent_rank_k1_k2 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 21
-- @order_sensitive=true
SELECT *, PERCENT_RANK() OVER (ORDER BY k1 DESC, k2 ASC) AS percent_rank_k1_desc_k2_asc FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 22
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (PARTITION BY v1 ORDER BY v2) AS cume_dist_v1_partition_v2 FROM ${case_db}.cume_test ORDER BY k1, k2, v1;

-- query 23
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (PARTITION BY v3 ORDER BY v4) AS cume_dist_v3_partition_v4 FROM ${case_db}.cume_test ORDER BY k1, k2, v1, v2, v3, v4;

-- query 24
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (PARTITION BY k1 ORDER BY k2) AS cume_dist_k1_partition_k2 FROM ${case_db}.cume_test ORDER BY v1, k1, k2;

-- query 25
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (PARTITION BY v4 ORDER BY v3) AS cume_dist_v4_partition_v3 FROM ${case_db}.cume_test ORDER BY v2 DESC, k1, k2;

-- query 26
-- @order_sensitive=true
SELECT *, PERCENT_RANK() OVER (PARTITION BY v1 ORDER BY v3) AS percent_rank_v1_partition_v3 FROM ${case_db}.cume_test ORDER BY k1, k2, v1, v2, v3, v4;

-- query 27
-- @order_sensitive=true
SELECT *, PERCENT_RANK() OVER (PARTITION BY v2 ORDER BY v4) AS percent_rank_v2_partition_v4 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 28
-- @order_sensitive=true
SELECT *, PERCENT_RANK() OVER (PARTITION BY k2 ORDER BY k1) AS percent_rank_k2_partition_k1 FROM ${case_db}.cume_test ORDER BY v1, k1, k2;

-- query 29
-- @order_sensitive=true
SELECT *, PERCENT_RANK() OVER (PARTITION BY v4 ORDER BY v2) AS percent_rank_v4_partition_v2 FROM ${case_db}.cume_test ORDER BY v2 DESC, k1, k2;

-- query 30
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (ORDER BY v1) AS cume_dist_v1, ROW_NUMBER() OVER (ORDER BY v1, v3) AS row_number_v1_v3 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 31
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (ORDER BY v2) AS cume_dist_v2, RANK() OVER (ORDER BY v1) AS rank_v1 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 32
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (ORDER BY v3) AS cume_dist_v3, DENSE_RANK() OVER (ORDER BY v1) AS dense_rank_v1 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 33
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (ORDER BY v4) AS cume_dist_v4, NTILE(5) OVER (ORDER BY v1, v3) AS ntile_v1 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 34
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (ORDER BY v1) AS cume_dist_v1, LAG(v2) OVER (ORDER BY v1, v3) AS lag_v2 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 35
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (ORDER BY v2) AS cume_dist_v2, LEAD(v2) OVER (ORDER BY v1, v3) AS lead_v2 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 36
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (ORDER BY v3) AS cume_dist_v3, FIRST_VALUE(v2) OVER (ORDER BY v1, v3) AS first_value_v2 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 37
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (ORDER BY v4) AS cume_dist_v4, LAST_VALUE(v2) OVER (ORDER BY v1, v3 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_value_v2 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 38
-- @order_sensitive=true
SELECT *, PERCENT_RANK() OVER (ORDER BY v1) AS percent_rank_v1, ROW_NUMBER() OVER (ORDER BY v1, v3) AS row_number_v1_v3 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 39
-- @order_sensitive=true
SELECT *, PERCENT_RANK() OVER (ORDER BY v2) AS percent_rank_v2, RANK() OVER (ORDER BY v1) AS rank_v1 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 40
-- @order_sensitive=true
SELECT *, PERCENT_RANK() OVER (ORDER BY v3) AS percent_rank_v3, DENSE_RANK() OVER (ORDER BY v1) AS dense_rank_v1 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 41
-- @order_sensitive=true
SELECT *, PERCENT_RANK() OVER (ORDER BY v4) AS percent_rank_v4, NTILE(5) OVER (ORDER BY v1, v3) AS ntile_v1 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 42
-- @order_sensitive=true
SELECT *, PERCENT_RANK() OVER (ORDER BY v1) AS percent_rank_v1, LAG(v2) OVER (ORDER BY v1, v3) AS lag_v2 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 43
-- @order_sensitive=true
SELECT *, PERCENT_RANK() OVER (ORDER BY v2) AS percent_rank_v2, LEAD(v2) OVER (ORDER BY v1, v3) AS lead_v2 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 44
-- @order_sensitive=true
SELECT *, PERCENT_RANK() OVER (ORDER BY v3) AS percent_rank_v3, FIRST_VALUE(v2) OVER (ORDER BY v1, v3) AS first_value_v2 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 45
-- @order_sensitive=true
SELECT *, PERCENT_RANK() OVER (ORDER BY v4) AS percent_rank_v4, LAST_VALUE(v2) OVER (ORDER BY v1, v3 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_value_v2 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 46
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (ORDER BY v1) AS cume_dist_v1, PERCENT_RANK() OVER (ORDER BY v3) AS percent_rank_v3 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 47
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (ORDER BY v2) AS cume_dist_v2, PERCENT_RANK() OVER (ORDER BY v2) AS percent_rank_v2 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 48
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (ORDER BY v3) AS cume_dist_v3, PERCENT_RANK() OVER (ORDER BY v3) AS percent_rank_v3 FROM ${case_db}.cume_test WHERE v3 IS NOT NULL ORDER BY k1, k2;

-- query 49
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (ORDER BY v4 DESC) AS cume_dist_v4, PERCENT_RANK() OVER (ORDER BY v1 DESC) AS percent_rank_v1 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 50
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (PARTITION BY k1 ORDER BY k2) AS percent_rank_k1_partition_k2, PERCENT_RANK() OVER (ORDER BY v1) AS percent_rank_v1 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 51
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (PARTITION BY v3 ORDER BY v4) AS percent_rank_v3_partition_v4, PERCENT_RANK() OVER (PARTITION BY v4 ORDER BY v3) AS percent_rank_v4_partition_v3 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 52
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (ORDER BY v1) AS cume_dist_v1, AVG(v2) OVER (ORDER BY v1, v3 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS avg_v2 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 53
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (ORDER BY v2) AS cume_dist_v2, SUM(v2) OVER (ORDER BY v1, v3 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_v2 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 54
-- @order_sensitive=true
SELECT *, CUME_DIST() OVER (ORDER BY v1) AS cume_dist_v1, MIN(v2) OVER (ORDER BY v1, v3 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_v2 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 55
-- @order_sensitive=true
SELECT *, PERCENT_RANK() OVER (ORDER BY v1) AS percent_rank_v1, AVG(v2) OVER (ORDER BY v1, v3 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS avg_v2 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 56
-- @order_sensitive=true
SELECT *, PERCENT_RANK() OVER (ORDER BY v2) AS percent_rank_v2, SUM(v2) OVER (ORDER BY v1, v3 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_v2 FROM ${case_db}.cume_test ORDER BY k1, k2;

-- query 57
-- @order_sensitive=true
SELECT *, PERCENT_RANK() OVER (ORDER BY v1) AS percent_rank_v1, MIN(v2) OVER (ORDER BY v1, v3 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_v2 FROM ${case_db}.cume_test ORDER BY k1, k2;
