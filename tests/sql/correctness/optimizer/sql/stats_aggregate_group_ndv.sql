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
-- Capture GROUP BY cardinality from two grouping keys.
DROP TABLE IF EXISTS ${case_db}.oq12_stats_group;
CREATE TABLE ${case_db}.oq12_stats_group (a INT, b INT, v INT);
INSERT INTO ${case_db}.oq12_stats_group
    SELECT generate_series % 10, generate_series % 20, generate_series
    FROM TABLE(sequence(1, 1000)) AS generate_series(generate_series);
ANALYZE ${case_db}.oq12_stats_group;

-- @explain_contains=HASH AGGREGATE
-- @explain_contains=group by: [a, b]
-- @explain_contains=oq12_stats_group
-- @explain_not_contains=stats={rows=1}
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'  SELECT a, b, SUM(v) AS total_v
FROM ${case_db}.oq12_stats_group
GROUP BY a, b;
