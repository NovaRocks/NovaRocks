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
-- Capture OR selectivity so inclusion-exclusion does not collapse to one row.
DROP TABLE IF EXISTS ${case_db}.oq12_stats_or;
CREATE TABLE ${case_db}.oq12_stats_or (a INT, b INT);
INSERT INTO ${case_db}.oq12_stats_or
    SELECT generate_series % 100, generate_series
    FROM TABLE(generate_series(1, 1000));
ANALYZE ${case_db}.oq12_stats_or;

-- @explain_contains=oq12_stats_or
-- @explain_contains=OR
-- @explain_not_contains=stats={rows=1}
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'  SELECT b
FROM ${case_db}.oq12_stats_or
WHERE a = 1 OR a = 2;
