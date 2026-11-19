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

-- @tags=optimizer,stats,oq12
-- Test Objective:
-- Capture set-operation row-count estimates for all/distinct/intersect/except.
-- Golden derivation: each branch projects its own k and shuffle imports keep
-- that name, replacing v0/v1 in INTERSECT and EXCEPT sinks. The two branch
-- identities remain distinct even though their display names match.
-- Preserve set operators/exchanges and estimates 1600/160/300/500.
DROP TABLE IF EXISTS ${case_db}.oq12_stats_set_l;
DROP TABLE IF EXISTS ${case_db}.oq12_stats_set_r;
CREATE TABLE ${case_db}.oq12_stats_set_l (k INT);
CREATE TABLE ${case_db}.oq12_stats_set_r (k INT);
INSERT INTO ${case_db}.oq12_stats_set_l
    SELECT generate_series % 100
    FROM TABLE(generate_series(1, 1000));
INSERT INTO ${case_db}.oq12_stats_set_r
    SELECT generate_series % 60
    FROM TABLE(generate_series(1, 600));
ANALYZE ${case_db}.oq12_stats_set_l;
ANALYZE ${case_db}.oq12_stats_set_r;

-- @explain_contains=UNION ALL
-- @explain_contains=oq12_stats_set_l
-- @explain_contains=oq12_stats_set_r
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'  SELECT k FROM ${case_db}.oq12_stats_set_l
UNION ALL
SELECT k FROM ${case_db}.oq12_stats_set_r;

-- @explain_contains=UNION
-- @explain_contains=oq12_stats_set_l
-- @explain_contains=oq12_stats_set_r
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'  SELECT k FROM ${case_db}.oq12_stats_set_l
UNION
SELECT k FROM ${case_db}.oq12_stats_set_r;

-- @explain_contains=INTERSECT
-- @explain_contains=oq12_stats_set_l
-- @explain_contains=oq12_stats_set_r
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'  SELECT k FROM ${case_db}.oq12_stats_set_l
INTERSECT
SELECT k FROM ${case_db}.oq12_stats_set_r;

-- @explain_contains=EXCEPT
-- @explain_contains=oq12_stats_set_l
-- @explain_contains=oq12_stats_set_r
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'  SELECT k FROM ${case_db}.oq12_stats_set_l
EXCEPT
SELECT k FROM ${case_db}.oq12_stats_set_r;
