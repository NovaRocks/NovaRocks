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

-- @tags=optimizer,aggregate_pushdown
-- Test Objective:
-- 1. Verify the EXPLAIN VERBOSE plan shows a partial AGGREGATE under
--    the join and a final AGGREGATE on top.
-- 2. Future PRs touching aggregate pushdown must intentionally re-record.
--
-- Data design: 20 000 rows on the left, NDV(k) = 100 (k = v % 100).
-- With ANALYZE TABLE the cost gate sees NDV=100 << 20000*0.5, so it fires.
DROP TABLE IF EXISTS ${case_db}.t_agg_pd_a;
DROP TABLE IF EXISTS ${case_db}.t_agg_pd_b;
CREATE TABLE ${case_db}.t_agg_pd_a (k INT, v INT);
CREATE TABLE ${case_db}.t_agg_pd_b (k INT);
INSERT INTO ${case_db}.t_agg_pd_a
    SELECT generate_series % 100, generate_series FROM TABLE(generate_series(1, 20000));
INSERT INTO ${case_db}.t_agg_pd_b
    SELECT DISTINCT generate_series % 100 FROM TABLE(generate_series(1, 20000));
ANALYZE TABLE ${case_db}.t_agg_pd_a;
ANALYZE TABLE ${case_db}.t_agg_pd_b;
-- @skip_result_check=true
-- @result_contains=HASH AGGREGATE (SINGLE, group by: [a.k])
-- @result_contains=aggregations: sum(sum(a.v))
-- @result_contains=HASH JOIN (
-- @result_contains=INNER, eq:
-- @result_contains=HASH AGGREGATE (GLOBAL, group by: [a.k])
-- @result_contains=HASH AGGREGATE (LOCAL, group by: [a.k])
EXPLAIN VERBOSE
SELECT a.k, SUM(a.v)
FROM ${case_db}.t_agg_pd_a a
INNER JOIN ${case_db}.t_agg_pd_b b ON a.k = b.k
GROUP BY a.k;
