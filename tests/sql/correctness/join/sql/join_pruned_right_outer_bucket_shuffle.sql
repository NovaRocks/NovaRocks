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

-- @tags=join,right_outer,bucket_shuffle
-- Test Objective:
-- 1. Validate RIGHT OUTER JOIN with [bucket] and [colocate] hints produces correct row counts.
-- 2. Prevent regressions where pruned right-outer local bucket-shuffle joins lose unmatched rows.
-- Test Flow:
-- 1. Create a table with hash-distributed BIGINT keys and insert 10000 rows.
-- 2. Run a CTE chain: w1 filters to a single key, w2 does right-outer bucket join,
--    w3 does right-outer colocate join of w1 and w2.
-- 3. Assert count is 10000 (all right-side rows preserved).
-- 4. Run the same query multiple times to ensure stability.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
  k1 bigint NULL,
  c1 bigint
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 SELECT generate_series, generate_series FROM TABLE(generate_series(0, 10000 - 1));

-- query 3
WITH
  w1 AS (SELECT k1 FROM ${case_db}.t1 WHERE k1 = 10),
  w2 AS (SELECT k1 FROM ${case_db}.t1 tt1 RIGHT OUTER JOIN  ${case_db}.t1 tt2 USING(k1)),
  w3 AS (SELECT k1 FROM w1 RIGHT OUTER JOIN  w2 USING(k1))
SELECT count(1) FROM w3;

-- query 4
WITH
  w1 AS (SELECT k1 FROM ${case_db}.t1 WHERE k1 = 10),
  w2 AS (SELECT k1 FROM ${case_db}.t1 tt1 RIGHT OUTER JOIN  ${case_db}.t1 tt2 USING(k1)),
  w3 AS (SELECT k1 FROM w1 RIGHT OUTER JOIN  w2 USING(k1))
SELECT count(1) FROM w3;
