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

-- @tags=optimizer,derive_join_not_null
-- Test Objective:
-- LEFT SEMI JOIN on nullable keys derives IS NOT NULL on the RIGHT (build)
-- side only; the left (probe) side is unchanged (StarRocks-faithful).
DROP TABLE IF EXISTS ${case_db}.t_dnn_sl;
DROP TABLE IF EXISTS ${case_db}.t_dnn_sr;
CREATE TABLE ${case_db}.t_dnn_sl (k INT, v INT);
CREATE TABLE ${case_db}.t_dnn_sr (k INT);
INSERT INTO ${case_db}.t_dnn_sl
    SELECT CASE WHEN generate_series % 12 = 0 THEN generate_series ELSE NULL END, generate_series
    FROM TABLE(sequence(1, 2000)) AS generate_series(generate_series);
INSERT INTO ${case_db}.t_dnn_sr
    SELECT CASE WHEN generate_series % 12 = 0 THEN generate_series ELSE NULL END
    FROM TABLE(sequence(1, 2000)) AS generate_series(generate_series);
ANALYZE ${case_db}.t_dnn_sl;
ANALYZE ${case_db}.t_dnn_sr;
-- @explain_contains=IS NOT NULL
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'  SELECT l.v
FROM ${case_db}.t_dnn_sl l
WHERE EXISTS (SELECT 1 FROM ${case_db}.t_dnn_sr r WHERE l.k = r.k);
