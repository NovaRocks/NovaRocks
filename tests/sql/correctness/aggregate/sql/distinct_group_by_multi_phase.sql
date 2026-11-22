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
-- Test Objective:
-- 1. Exercise the 3-phase DISTINCT aggregation (LOCAL -> DISTINCT_GLOBAL -> GLOBAL).
-- 2. TPC-DS has no GROUP BY + count(distinct) query; this is the coverage gap filler.

-- query 1
-- @skip_result_check=true

-- query 2
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_dg (
    g INT,
    x INT,
    a BIGINT
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.t_dg
VALUES
    ROW(1, 100, 10), ROW(1, 100, 20), ROW(1, 200, 30),
    ROW(2, 100, 40), ROW(2, 300, 50), ROW(2, 300, 60),
    ROW(3, 400, 70);

-- query 4
-- @skip_result_check=true
-- Force this case through SplitDistinctAgg's distributed multi-phase path.
SET SESSION disable_optimizer_rules = 'AggToHashAgg';

-- query 5
-- @explain_contains=HASH AGGREGATE (LOCAL,
-- @explain_contains=HASH AGGREGATE (DISTINCT_GLOBAL,
-- @explain_contains=HASH AGGREGATE (GLOBAL,
-- @explain_contains=HASH EXCHANGE
SELECT g, count(distinct x) AS dc, sum(a) AS sa
FROM ${case_db}.t_dg
GROUP BY g
ORDER BY g;

-- query 6
-- @skip_result_check=true
SET SESSION disable_optimizer_rules = '';

-- query 7
-- @explain_contains=HASH AGGREGATE (SINGLE,
-- @explain_not_contains=HASH AGGREGATE (DISTINCT_GLOBAL,
SELECT g, count(DISTINCT x) FILTER (WHERE a BETWEEN 20 AND 60) AS dc, sum(a) AS sa
FROM ${case_db}.t_dg GROUP BY g ORDER BY g;
