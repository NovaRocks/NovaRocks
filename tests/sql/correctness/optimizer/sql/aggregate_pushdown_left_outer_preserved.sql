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

-- @tags=optimizer,aggregate_pushdown,outer
-- Test Objective:
-- LEFT OUTER JOIN with aggregate on the preserved (left) side: the
-- rule must still push to the left side.
-- Data design: 20 000 rows on the left, NDV(k) = 100. ANALYZE TABLE
-- ensures the cost gate fires.
-- Golden derivation: contract_lowering::append_exchange retains the sender's
-- frozen display name, so the old v3 import is b.k in the join and its sink.
-- Preserve RIGHT OUTER after commutation, the a-side LOCAL/GLOBAL aggregates,
-- the final sum(sum(a.v)), all exchanges, RF endpoints, and row estimates.
DROP TABLE IF EXISTS ${case_db}.t_agg_pd_lo_a;
DROP TABLE IF EXISTS ${case_db}.t_agg_pd_lo_b;
CREATE TABLE ${case_db}.t_agg_pd_lo_a (k INT, v INT);
CREATE TABLE ${case_db}.t_agg_pd_lo_b (k INT);
INSERT INTO ${case_db}.t_agg_pd_lo_a
    SELECT generate_series % 100, generate_series FROM TABLE(sequence(1, 20000)) AS generate_series(generate_series);
INSERT INTO ${case_db}.t_agg_pd_lo_b
    SELECT DISTINCT generate_series % 50 FROM TABLE(sequence(1, 50)) AS generate_series(generate_series);
ANALYZE ${case_db}.t_agg_pd_lo_a;
ANALYZE ${case_db}.t_agg_pd_lo_b;
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT a.k, SUM(a.v)
FROM ${case_db}.t_agg_pd_lo_a a
LEFT OUTER JOIN ${case_db}.t_agg_pd_lo_b b ON a.k = b.k
GROUP BY a.k;
