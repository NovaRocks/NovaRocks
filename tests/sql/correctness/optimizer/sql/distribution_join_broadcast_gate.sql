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

-- @tags=optimizer,oq8,distribution
-- With real iceberg stats a small build side joins via BROADCAST. Plan-shape
-- golden over iceberg base tables; broadcast-gate behavior at scale is covered
-- by the ssb/tpc-* benchmark suites.
DROP TABLE IF EXISTS ${case_db}.oq8_probe_big;
DROP TABLE IF EXISTS ${case_db}.oq8_build_big;
CREATE TABLE ${case_db}.oq8_probe_big (k INT, v INT);
CREATE TABLE ${case_db}.oq8_build_big (k INT, v INT);
INSERT INTO ${case_db}.oq8_probe_big
    SELECT generate_series, generate_series FROM TABLE(sequence(1, 1000)) AS generate_series(generate_series);
INSERT INTO ${case_db}.oq8_build_big
    SELECT generate_series, generate_series FROM TABLE(sequence(1, 1000)) AS generate_series(generate_series);
ANALYZE ${case_db}.oq8_probe_big;
ANALYZE ${case_db}.oq8_build_big;
SET SESSION cbo_broadcast_backend_count = 7;
-- @explain_contains=bcast_verdict=feasible
-- @explain_contains=HASH JOIN (BROADCAST
SELECT COUNT(*) AS cnt
FROM ${case_db}.oq8_probe_big p
INNER JOIN ${case_db}.oq8_build_big b ON p.k = b.k;

-- The build/probe choice is a cost-model tie-breaker. Keep the semantic
-- broadcast assertions above, but do not freeze alias ordering in a full
-- Verbose EXPLAIN golden.
-- @skip_result_check=true
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT COUNT(*) AS cnt
FROM ${case_db}.oq8_probe_big p
INNER JOIN ${case_db}.oq8_build_big b ON p.k = b.k;

SET SESSION cbo_broadcast_backend_count = 3;
-- @explain_contains=bcast_verdict=feasible
-- @explain_contains=HASH JOIN (BROADCAST
SELECT COUNT(*) AS cnt
FROM ${case_db}.oq8_probe_big p
INNER JOIN ${case_db}.oq8_build_big b ON p.k = b.k;

-- @skip_result_check=true
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT COUNT(*) AS cnt
FROM ${case_db}.oq8_probe_big p
INNER JOIN ${case_db}.oq8_build_big b ON p.k = b.k;
