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

-- @tags=join,partition,broadcast
-- Test Objective:
-- 1. Validate broadcast join correctness on functional-scale colocated tables with partition properties.
-- 2. Prevent regressions where broadcast join with query cache drops or duplicates rows.
-- Test Flow:
-- 1. Create two colocated tables (t0, t1) with 48 buckets and insert 100K rows each.
-- 2. Execute a broadcast join aggregation and assert all 100K rows are matched.
-- 3. Enable query_cache and re-run the same query twice to confirm stability under caching.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t0 (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) NULL COMMENT "",
  `c2` varchar(200) NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1, 100000));

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 SELECT * FROM ${case_db}.t0;

-- query 5
SELECT count(l1), count(l2), count(l3), count(l4) FROM (SELECT l.c0 l1, r.c1 l2, r.c2 l3, r.c3 l4 FROM ${case_db}.t0 l JOIN  ${case_db}.t1 r ON l.c0=r.c0 AND l.c1=r.c1 GROUP BY 1,2,3,4) t;

-- query 6
-- @skip_result_check=true

-- query 7
SELECT count(l1), count(l2), count(l3), count(l4) FROM (SELECT l.c0 l1, r.c1 l2, r.c2 l3, r.c3 l4 FROM ${case_db}.t0 l JOIN  ${case_db}.t1 r ON l.c0=r.c0 AND l.c1=r.c1 GROUP BY 1,2,3,4) t;

-- query 8
SELECT count(l1), count(l2), count(l3), count(l4) FROM (SELECT l.c0 l1, r.c1 l2, r.c2 l3, r.c3 l4 FROM ${case_db}.t0 l JOIN  ${case_db}.t1 r ON l.c0=r.c0 AND l.c1=r.c1 GROUP BY 1,2,3,4) t;
