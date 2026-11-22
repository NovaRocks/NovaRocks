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

-- @tags=optimizer,bc1,distribution
-- Golden derivation: shuffle imports retain p.k, b.k, p.pad1 and p.pad2.
-- Replace only old unqualified import names in query 15; the query 12
-- broadcast plan, query 15 partitioned plan, and both 500000 results stay.
CREATE SCHEMA IF NOT EXISTS ${case_db};
USE ${case_db};
CREATE TABLE probe_flip (k INT, pad1 VARCHAR(100), pad2 VARCHAR(100));
CREATE TABLE build_flip (k INT);
INSERT INTO probe_flip
    SELECT generate_series, rpad('', 100, 'p'), rpad('', 100, 'q')
    FROM TABLE(sequence(1, 5000000)) AS generate_series(generate_series);
INSERT INTO build_flip
    SELECT generate_series FROM TABLE(sequence(1, 500000)) AS generate_series(generate_series);
ANALYZE probe_flip;
ANALYZE build_flip;
-- Recorded sizing: 5M wide probe + 500k narrow build. be=2 keeps broadcast;
-- be=16 flips to partitioned from backend-scaled memory/network cost.
SET SESSION cbo_broadcast_node_mem_budget_bytes = 536870912;
SET SESSION cbo_broadcast_backend_count = 2;
-- @explain_contains=HASH JOIN (BROADCAST
-- @explain_contains=bcast_verdict=feasible
SELECT COUNT(p.pad1) AS c1, COUNT(p.pad2) AS c2
FROM probe_flip p JOIN build_flip b ON p.k = b.k;

EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT COUNT(p.pad1) AS c1, COUNT(p.pad2) AS c2
FROM probe_flip p JOIN build_flip b ON p.k = b.k;

SET SESSION cbo_broadcast_backend_count = 16;
-- @explain_contains=HASH JOIN (PARTITIONED
-- @explain_not_contains=BROADCAST, INNER
SELECT COUNT(p.pad1) AS c1, COUNT(p.pad2) AS c2
FROM probe_flip p JOIN build_flip b ON p.k = b.k;

EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT COUNT(p.pad1) AS c1, COUNT(p.pad2) AS c2
FROM probe_flip p JOIN build_flip b ON p.k = b.k;
