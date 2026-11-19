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

-- @tags=optimizer,cost-model,explain
-- Test Objective:
-- Lock in dimensional CBO cost output for scan/filter, join, and TopN plans.
-- The EXPLAIN COSTS statements are recorded directly in the golden result;
-- do not use @explain_contains here because that directive reruns EXPLAIN VERBOSE.
-- Golden derivation: stats_input::QueryStatsSnapshot::display_rows and
-- completed_tree::render_completed_plan_tree print one TABLE STATS row for
-- each base scan binding at COSTS level. All three queries have one binding
-- (ref=0); GENERATE_SERIES contributes none. INSERT supplies 1000 rows with
-- no deletes, so manifest row-count evidence is Exact/IcebergManifest and
-- the label is the full iceberg_opt.case_db.cost_model_explain_t identity.
-- This adds one provenance row per query; preserve the old operators,
-- 990/100/10 output estimates, broadcast sizing, and both TopN phases.
SET SESSION cbo_broadcast_backend_count = 3;
DROP TABLE IF EXISTS ${case_db}.cost_model_explain_t;
CREATE TABLE ${case_db}.cost_model_explain_t (k INT, v INT);
INSERT INTO ${case_db}.cost_model_explain_t
    SELECT generate_series, generate_series * 10
    FROM TABLE(generate_series(1, 1000));
ANALYZE ${case_db}.cost_model_explain_t;

EXPLAIN WITH SESSION explain_detail = 'COSTS'
SELECT * FROM ${case_db}.cost_model_explain_t WHERE v > 10;

EXPLAIN WITH SESSION explain_detail = 'COSTS'
SELECT cost_model_explain_t.k, cost_model_explain_t.v, gs.generate_series AS rk
FROM ${case_db}.cost_model_explain_t
JOIN TABLE(generate_series(1, 100)) gs
    ON cost_model_explain_t.k = gs.generate_series;

EXPLAIN WITH SESSION explain_detail = 'COSTS'
SELECT * FROM ${case_db}.cost_model_explain_t ORDER BY v DESC LIMIT 10;
