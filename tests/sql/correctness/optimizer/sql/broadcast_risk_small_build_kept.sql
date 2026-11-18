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
CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};
CREATE TABLE probe_1m (k INT, v BIGINT);
CREATE TABLE build_1000 (k INT);
INSERT INTO probe_1m
    SELECT generate_series, generate_series FROM TABLE(generate_series(1, 1000000));
INSERT INTO build_1000
    SELECT generate_series FROM TABLE(generate_series(1, 1000));
ANALYZE TABLE probe_1m;
ANALYZE TABLE build_1000;
SET cbo_broadcast_backend_count = 3;
SET cbo_broadcast_node_mem_budget_bytes = 268435456;
-- @explain_contains=HASH JOIN (BROADCAST
-- @explain_contains=bcast_verdict=feasible
SELECT COUNT() AS cnt FROM probe_1m p JOIN build_1000 b ON p.k = b.k;

EXPLAIN VERBOSE
SELECT COUNT() AS cnt FROM probe_1m p JOIN build_1000 b ON p.k = b.k;
