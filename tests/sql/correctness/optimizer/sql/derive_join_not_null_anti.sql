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
-- LEFT ANTI JOIN must NOT derive any IS NOT NULL (left NULL keys are emitted).
-- The recorded golden is the regression guard for absence.
DROP TABLE IF EXISTS ${case_db}.t_dnn_al;
DROP TABLE IF EXISTS ${case_db}.t_dnn_ar;
CREATE TABLE ${case_db}.t_dnn_al (k INT, v INT);
CREATE TABLE ${case_db}.t_dnn_ar (k INT);
INSERT INTO ${case_db}.t_dnn_al
    SELECT CASE WHEN generate_series % 12 = 0 THEN generate_series ELSE NULL END, generate_series
    FROM TABLE(generate_series(1, 2000));
INSERT INTO ${case_db}.t_dnn_ar
    SELECT CASE WHEN generate_series % 12 = 0 THEN generate_series ELSE NULL END
    FROM TABLE(generate_series(1, 2000));
ANALYZE TABLE ${case_db}.t_dnn_al;
ANALYZE TABLE ${case_db}.t_dnn_ar;
EXPLAIN VERBOSE
SELECT l.v
FROM ${case_db}.t_dnn_al l
WHERE NOT EXISTS (SELECT 1 FROM ${case_db}.t_dnn_ar r WHERE l.k = r.k);
