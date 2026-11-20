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

-- @tags=optimizer,g4,hash_source
-- Test Objective:
-- 1. A window partition above the left join must still introduce its own
--    HASH EXCHANGE for the partition key.
-- 2. ANALYZE TABLE supplies the scan row counts used by the optimizer; table
--    names must not carry any row-count heuristic.
DROP TABLE IF EXISTS ${case_db}.lineitem;
DROP TABLE IF EXISTS ${case_db}.orders;
CREATE TABLE ${case_db}.lineitem (k INT, v INT);
CREATE TABLE ${case_db}.orders (k INT, w INT);
INSERT INTO ${case_db}.lineitem VALUES ROW(1, 10), ROW(2, 20);
INSERT INTO ${case_db}.orders VALUES ROW(1, 100), ROW(2, 200);
ANALYZE ${case_db}.lineitem;
ANALYZE ${case_db}.orders;

SET SESSION disable_optimizer_rules = 'JoinCommutativity';

EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT l.k, l.v, r.w,
       ROW_NUMBER() OVER (PARTITION BY l.k ORDER BY l.v) AS rn
FROM ${case_db}.lineitem l
LEFT JOIN ${case_db}.orders r ON l.k = r.k;

SET SESSION disable_optimizer_rules = '';
