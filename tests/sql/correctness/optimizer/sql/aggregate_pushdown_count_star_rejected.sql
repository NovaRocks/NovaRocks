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

-- @tags=optimizer,aggregate_pushdown,negative
-- Test Objective:
-- COUNT(*) must NOT be pushed underneath the join. OQ-4 may split the
-- top aggregate into Local/Global stages, but both stages must stay above
-- the join.
DROP TABLE IF EXISTS ${case_db}.t_agg_pd_neg_a;
DROP TABLE IF EXISTS ${case_db}.t_agg_pd_neg_b;
CREATE TABLE ${case_db}.t_agg_pd_neg_a (k INT);
CREATE TABLE ${case_db}.t_agg_pd_neg_b (k INT);
INSERT INTO ${case_db}.t_agg_pd_neg_a VALUES ROW(1), ROW(2);
INSERT INTO ${case_db}.t_agg_pd_neg_b VALUES ROW(1);
ANALYZE ${case_db}.t_agg_pd_neg_a;
ANALYZE ${case_db}.t_agg_pd_neg_b;
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT COUNT(*)
FROM ${case_db}.t_agg_pd_neg_a a
INNER JOIN ${case_db}.t_agg_pd_neg_b b ON a.k = b.k;
