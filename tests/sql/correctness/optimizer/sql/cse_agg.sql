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

-- @tags=optimizer,cse
-- Test Objective:
-- Repeated aggregate argument scalar subexpressions are materialized once below
-- the Aggregate as an internal CSE Project item while aggregate results stay stable.
DROP TABLE IF EXISTS ${case_db}.cse_agg_t;
CREATE TABLE ${case_db}.cse_agg_t (a BIGINT, b BIGINT);
INSERT INTO ${case_db}.cse_agg_t VALUES ROW(2, 3), ROW(4, 5), ROW(6, 7);

SELECT SUM(a * b) AS sum_ab, AVG(a * b) AS avg_ab
FROM ${case_db}.cse_agg_t;

-- @explain_contains=__cse_0
-- @result_not_contains=__cse_
SELECT SUM(CAST(a AS DOUBLE) * CAST(b AS DOUBLE)) AS sum_ab,
       AVG(CAST(a AS DOUBLE) * CAST(b AS DOUBLE)) AS avg_ab
FROM ${case_db}.cse_agg_t;
