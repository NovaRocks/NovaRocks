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
CREATE TABLE probe_1x (k INT);
CREATE TABLE build_1x (k INT);
CREATE TABLE probe_10x (k INT);
CREATE TABLE build_10x (k INT);
INSERT INTO probe_1x
    SELECT generate_series FROM TABLE(generate_series(1, 100000));
INSERT INTO build_1x
    SELECT generate_series FROM TABLE(generate_series(1, 10000));
INSERT INTO probe_10x
    SELECT generate_series FROM TABLE(generate_series(1, 1000000));
INSERT INTO build_10x
    SELECT generate_series FROM TABLE(generate_series(1, 100000));
ANALYZE probe_1x;
ANALYZE build_1x;
ANALYZE probe_10x;
ANALYZE build_10x;
SET SESSION cbo_broadcast_backend_count = 3;
SET SESSION cbo_broadcast_node_mem_budget_bytes = 268435456;
-- @explain_contains=HASH JOIN (BROADCAST
SELECT COUNT() AS cnt FROM probe_1x p JOIN build_1x b ON p.k = b.k;

EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT COUNT() AS cnt FROM probe_1x p JOIN build_1x b ON p.k = b.k;

-- @explain_contains=HASH JOIN (BROADCAST
SELECT COUNT() AS cnt FROM probe_10x p JOIN build_10x b ON p.k = b.k;

EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT COUNT() AS cnt FROM probe_10x p JOIN build_10x b ON p.k = b.k;
