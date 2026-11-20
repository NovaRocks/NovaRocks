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
-- Q72 proxy: cross join estimates should stay finite and readable.
DROP TABLE IF EXISTS ${case_db}.oq12_stats_big_a;
DROP TABLE IF EXISTS ${case_db}.oq12_stats_big_b;
CREATE TABLE ${case_db}.oq12_stats_big_a (k INT);
CREATE TABLE ${case_db}.oq12_stats_big_b (k INT);
INSERT INTO ${case_db}.oq12_stats_big_a
    SELECT generate_series FROM TABLE(generate_series(1, 100));
INSERT INTO ${case_db}.oq12_stats_big_b
    SELECT generate_series FROM TABLE(generate_series(1, 100));
ANALYZE ${case_db}.oq12_stats_big_a;
ANALYZE ${case_db}.oq12_stats_big_b;

-- @explain_contains=CROSS
-- @explain_contains=oq12_stats_big_a
-- @explain_contains=oq12_stats_big_b
-- @explain_not_contains=rows=>=
-- @explain_not_contains=9223372036854775807
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'  SELECT COUNT(*)
FROM ${case_db}.oq12_stats_big_a a
CROSS JOIN ${case_db}.oq12_stats_big_b b;
