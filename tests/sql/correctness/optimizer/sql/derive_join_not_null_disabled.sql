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

-- @tags=optimizer,derive_join_not_null,session_rule_disable
-- Test Objective:
-- SET disable_optimizer_rules='DeriveJoinNotNullPredicate' suppresses the
-- derivation. Keep JoinCommutativity disabled in both plans so this case only
-- tests the derived IS NOT NULL predicates instead of join orientation.
DROP TABLE IF EXISTS ${case_db}.t_dnn_dl;
DROP TABLE IF EXISTS ${case_db}.t_dnn_dr;
CREATE TABLE ${case_db}.t_dnn_dl (k INT, v INT);
CREATE TABLE ${case_db}.t_dnn_dr (k INT, v INT);
INSERT INTO ${case_db}.t_dnn_dl
    SELECT CASE WHEN generate_series % 12 = 0 THEN generate_series ELSE NULL END, generate_series
    FROM TABLE(sequence(1, 2000)) AS generate_series(generate_series);
INSERT INTO ${case_db}.t_dnn_dr
    SELECT CASE WHEN generate_series % 12 = 0 THEN generate_series ELSE NULL END, generate_series
    FROM TABLE(sequence(1, 2000)) AS generate_series(generate_series);
ANALYZE ${case_db}.t_dnn_dl;
ANALYZE ${case_db}.t_dnn_dr;

SET SESSION disable_optimizer_rules = 'JoinCommutativity';

-- @result_contains=predicates: l.k IS NOT NULL
-- @result_contains=predicates: r.k IS NOT NULL
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT l.v, r.v
FROM ${case_db}.t_dnn_dl l
INNER JOIN ${case_db}.t_dnn_dr r ON l.k = r.k;

SET SESSION disable_optimizer_rules = 'DeriveJoinNotNullPredicate,JoinCommutativity';

-- @result_not_contains=predicates: l.k IS NOT NULL
-- @result_not_contains=predicates: r.k IS NOT NULL
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT l.v, r.v
FROM ${case_db}.t_dnn_dl l
INNER JOIN ${case_db}.t_dnn_dr r ON l.k = r.k;

SET SESSION disable_optimizer_rules = '';
