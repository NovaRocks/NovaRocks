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
-- Capture outer, semi, and anti join cardinality without one-row collapse.
DROP TABLE IF EXISTS ${case_db}.oq12_stats_join_l;
DROP TABLE IF EXISTS ${case_db}.oq12_stats_join_r;
CREATE TABLE ${case_db}.oq12_stats_join_l (k INT, payload INT);
CREATE TABLE ${case_db}.oq12_stats_join_r (k INT, flag INT);
INSERT INTO ${case_db}.oq12_stats_join_l
    SELECT generate_series % 100, generate_series
    FROM TABLE(generate_series(1, 1000));
INSERT INTO ${case_db}.oq12_stats_join_r
    SELECT generate_series % 80, generate_series % 2
    FROM TABLE(generate_series(1, 800));
ANALYZE ${case_db}.oq12_stats_join_l;
ANALYZE ${case_db}.oq12_stats_join_r;

-- @explain_contains=HASH JOIN
-- @explain_contains=LEFT OUTER
-- @explain_contains=oq12_stats_join_l
-- @explain_contains=oq12_stats_join_r
-- @explain_not_contains=stats={rows=1}
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'  SELECT l.k, r.flag
FROM ${case_db}.oq12_stats_join_l l
LEFT OUTER JOIN ${case_db}.oq12_stats_join_r r ON l.k = r.k;

-- @explain_contains=HASH JOIN
-- @explain_contains=LEFT SEMI
-- @explain_contains=oq12_stats_join_l
-- @explain_contains=oq12_stats_join_r
-- @explain_not_contains=stats={rows=1}
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'  SELECT l.k
FROM ${case_db}.oq12_stats_join_l l
WHERE EXISTS (SELECT 1 FROM ${case_db}.oq12_stats_join_r r WHERE l.k = r.k);

-- @explain_contains=HASH JOIN
-- @explain_contains=LEFT ANTI
-- @explain_contains=oq12_stats_join_l
-- @explain_contains=oq12_stats_join_r
-- @explain_not_contains=stats={rows=1}
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'  SELECT l.k
FROM ${case_db}.oq12_stats_join_l l
WHERE NOT EXISTS (SELECT 1 FROM ${case_db}.oq12_stats_join_r r WHERE l.k = r.k);
