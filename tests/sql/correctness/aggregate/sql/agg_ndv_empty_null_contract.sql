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

-- @order_sensitive=true
-- @tags=aggregate,ndv,null
-- Test Objective:
-- Count non-null distinct values on empty, all-null, and mixed input across groups.
-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.ndv_null_contract (k INT, v INT, s VARCHAR(8))
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.ndv_null_contract VALUES
    (1, NULL, 'a'), (1, NULL, 'a'), (2, 7, 'a'), (2, NULL, 'a'),
    (2, 7, 'a'), (3, 7, 'a'), (3, 8, 'a');

-- query 3
SELECT ndv(v) AS ndv_empty, approx_count_distinct(v) AS approx_empty,
       count(DISTINCT v) AS exact_empty
FROM ${case_db}.ndv_null_contract WHERE k = 999;

-- query 4
SELECT ndv(v) AS ndv_null, approx_count_distinct(v) AS approx_null,
       count(DISTINCT v) AS exact_null
FROM ${case_db}.ndv_null_contract WHERE k = 1;

-- query 5
SELECT k, ndv(v) AS ndv_count, approx_count_distinct(v) AS approx_count,
       count(DISTINCT v) AS exact_count
FROM ${case_db}.ndv_null_contract GROUP BY k ORDER BY k;

-- query 6
SELECT ndv(CAST(s AS BIGINT)) AS ndv_cast_null,
       approx_count_distinct(CAST(s AS BIGINT)) AS approx_cast_null
FROM ${case_db}.ndv_null_contract;

-- query 7
SELECT k FROM ${case_db}.ndv_null_contract GROUP BY k
HAVING ndv(v) = 0 AND approx_count_distinct(v) = 0 ORDER BY k;
