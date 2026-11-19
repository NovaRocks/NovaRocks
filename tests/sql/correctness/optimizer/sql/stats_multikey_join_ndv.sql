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
-- Capture the current multi-key join cardinality estimate with moderate NDV.
DROP TABLE IF EXISTS ${case_db}.oq12_stats_mj_l;
DROP TABLE IF EXISTS ${case_db}.oq12_stats_mj_r;
CREATE TABLE ${case_db}.oq12_stats_mj_l (k1 INT, k2 INT, payload INT);
CREATE TABLE ${case_db}.oq12_stats_mj_r (k1 INT, k2 INT, payload INT);
INSERT INTO ${case_db}.oq12_stats_mj_l
    SELECT generate_series % 50, generate_series % 20, generate_series
    FROM TABLE(generate_series(1, 1000));
INSERT INTO ${case_db}.oq12_stats_mj_r
    SELECT generate_series % 50, generate_series % 20, generate_series * 10
    FROM TABLE(generate_series(1, 100));
ANALYZE TABLE ${case_db}.oq12_stats_mj_l;
ANALYZE TABLE ${case_db}.oq12_stats_mj_r;

-- r is intentionally an order of magnitude smaller than l (100 vs 1000 rows)
-- while keeping comparable per-key NDV, so r is unambiguously the smaller
-- broadcast/build side. With equal-sized sides the build-side choice is a cost
-- tie whose tie-break varies with per-thread HashMap seeds, which made the
-- recorded plan-tree shape flaky under parallel workers. Commutativity is also
-- pinned for good measure; the objective here is the multi-key cardinality
-- estimate, not the join order.
SET disable_optimizer_rules = 'JoinCommutativity';

-- @explain_contains=HASH JOIN
-- @explain_contains=eq: [
-- @explain_contains=k1
-- @explain_contains=k2
-- @explain_contains=oq12_stats_mj_l
-- @explain_contains=oq12_stats_mj_r
EXPLAIN VERBOSE SELECT l.k1, l.k2, COUNT() AS match_count
FROM ${case_db}.oq12_stats_mj_l l
INNER JOIN ${case_db}.oq12_stats_mj_r r
    ON l.k1 = r.k1 AND l.k2 = r.k2
GROUP BY l.k1, l.k2;

SET disable_optimizer_rules = '';
