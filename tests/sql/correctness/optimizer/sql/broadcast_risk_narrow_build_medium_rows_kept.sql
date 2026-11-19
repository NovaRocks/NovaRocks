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
CREATE SCHEMA IF NOT EXISTS ${case_db};
USE ${case_db};
CREATE TABLE probe_5m (k INT, pad1 VARCHAR(100), pad2 VARCHAR(100));
-- The unit-level q9 canary covers the 2M-row arithmetic. This SQL golden uses
-- a 500k narrow build so the all-in-one optimizer fixture keeps BROADCAST under
-- be=3 with the suite's actual persisted column-width statistics.
CREATE TABLE build_500k_int (k INT);
INSERT INTO probe_5m
    SELECT generate_series, repeat('x', 100), repeat('y', 100)
    FROM TABLE(generate_series(1, 5000000));
INSERT INTO build_500k_int
    SELECT generate_series FROM TABLE(generate_series(1, 500000));
ANALYZE probe_5m;
ANALYZE build_500k_int;
SET SESSION cbo_broadcast_backend_count = 3;
SET SESSION cbo_broadcast_node_mem_budget_bytes = 268435456;
-- @explain_contains=HASH JOIN (BROADCAST
-- @explain_contains=bcast_verdict=feasible
-- @explain_not_contains=PARTITIONED, INNER
SELECT COUNT(p.pad1) AS c1, COUNT(p.pad2) AS c2
FROM probe_5m p JOIN build_500k_int b ON p.k = b.k;

EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT COUNT(p.pad1) AS c1, COUNT(p.pad2) AS c2
FROM probe_5m p JOIN build_500k_int b ON p.k = b.k;
