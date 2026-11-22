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

-- @tags=optimizer,session_rule_disable
-- Test Objective:
-- 1. Verify SET disable_optimizer_rules = 'JoinCommutativity' changes plan shape.
-- 2. Two EXPLAIN VERBOSE statements; the .result captures both and the
--    diff between them is the join order / distribution type.
-- Design note:
-- The query is an INNER JOIN written as date_dim LEFT, lineorder RIGHT.
-- JoinReorder is disabled for both EXPLAINs so the only remaining mechanism
-- that can swap the two-table join in Cascades is JoinCommutativity.
-- With JoinCommutativity: CBO swaps to lineorder LEFT (probe) + date_dim RIGHT
--   so the tiny date_dim table is the broadcast build side.
-- Without JoinCommutativity: CBO cannot swap, so lineorder remains on the
--   original right side.
DROP TABLE IF EXISTS ${case_db}.lineorder;
DROP TABLE IF EXISTS ${case_db}.date_dim;
CREATE TABLE ${case_db}.lineorder (lo_orderkey INT, lo_datekey INT, lo_revenue INT);
CREATE TABLE ${case_db}.date_dim (d_datekey INT, d_year INT);
INSERT INTO ${case_db}.lineorder
    SELECT generate_series, 19980101 + (generate_series % 2), generate_series * 10
    FROM TABLE(sequence(1, 20000)) AS generate_series(generate_series);
INSERT INTO ${case_db}.date_dim VALUES ROW(19980101, 1998), ROW(19980102, 1998);
ANALYZE ${case_db}.lineorder;
ANALYZE ${case_db}.date_dim;

-- Baseline: keep query-rewrite join reorder off, but allow Cascades
-- JoinCommutativity.
SET SESSION disable_optimizer_rules = 'JoinReorder';

-- Expected: CBO swaps to lineorder LEFT + date_dim RIGHT (BROADCAST, INNER).
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT lo.lo_orderkey, d.d_year
FROM ${case_db}.date_dim d
INNER JOIN ${case_db}.lineorder lo ON d.d_datekey = lo.lo_datekey;

-- Disable JoinCommutativity for the next query.
SET SESSION disable_optimizer_rules = 'JoinReorder,JoinCommutativity';

-- Without commutativity: cannot swap, date_dim stays LEFT and lineorder
-- remains on the original right side.
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT lo.lo_orderkey, d.d_year
FROM ${case_db}.date_dim d
INNER JOIN ${case_db}.lineorder lo ON d.d_datekey = lo.lo_datekey;

-- Restore.
SET SESSION disable_optimizer_rules = '';
