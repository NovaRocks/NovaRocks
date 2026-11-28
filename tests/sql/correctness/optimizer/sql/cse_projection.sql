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
-- Repeated projection-list scalar subexpressions are materialized once as an
-- internal CSE Project item while user-visible query output stays unchanged.
DROP TABLE IF EXISTS ${case_db}.cse_projection_t;
CREATE TABLE ${case_db}.cse_projection_t (a BIGINT, b BIGINT);
INSERT INTO ${case_db}.cse_projection_t VALUES ROW(3, 4), ROW(5, 6), ROW(7, NULL);

SELECT (a + b) AS x, (a + b) + a AS y
FROM ${case_db}.cse_projection_t
ORDER BY a;

-- @skip_result_check=true
-- @result_contains=__cse_
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT (CAST(a AS DOUBLE) + CAST(b AS DOUBLE)) AS x,
       (CAST(a AS DOUBLE) + CAST(b AS DOUBLE)) + CAST(a AS DOUBLE) AS y
FROM ${case_db}.cse_projection_t
ORDER BY a;

-- @explain_contains=__cse_0
-- @result_not_contains=__cse_
SELECT (CAST(a AS DOUBLE) + CAST(b AS DOUBLE)) AS x,
       (CAST(a AS DOUBLE) + CAST(b AS DOUBLE)) + CAST(a AS DOUBLE) AS y,
       typeof(CAST(a AS DOUBLE)) AS input_type
FROM ${case_db}.cse_projection_t
ORDER BY a;

SET SESSION enable_common_subexpr_reuse = false;

-- @skip_result_check=true
-- @result_not_contains=__cse_
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT (CAST(a AS DOUBLE) + CAST(b AS DOUBLE)) AS x,
       (CAST(a AS DOUBLE) + CAST(b AS DOUBLE)) + CAST(a AS DOUBLE) AS y
FROM ${case_db}.cse_projection_t
ORDER BY a;

-- @explain_not_contains=__cse_
-- @result_not_contains=__cse_
SELECT (CAST(a AS DOUBLE) + CAST(b AS DOUBLE)) AS x,
       (CAST(a AS DOUBLE) + CAST(b AS DOUBLE)) + CAST(a AS DOUBLE) AS y,
       typeof(CAST(a AS DOUBLE)) AS input_type
FROM ${case_db}.cse_projection_t
ORDER BY a;

SET SESSION enable_common_subexpr_reuse = true;

SET SESSION disable_optimizer_rules = 'CommonSubexpressionReuse';

-- @skip_result_check=true
-- @result_not_contains=__cse_
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT (CAST(a AS DOUBLE) + CAST(b AS DOUBLE)) AS x,
       (CAST(a AS DOUBLE) + CAST(b AS DOUBLE)) + CAST(a AS DOUBLE) AS y
FROM ${case_db}.cse_projection_t
ORDER BY a;

SET SESSION disable_optimizer_rules = '';
