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

-- @tags=optimizer,oq9,residual,or_side_filter
-- Test Objective:
-- Derive an opposite-side IN filter from an OR group on an inner join key
-- without expanding the original OR group into separate joins.
DROP TABLE IF EXISTS ${case_db}.residual_or_l;
DROP TABLE IF EXISTS ${case_db}.residual_or_r;
CREATE TABLE ${case_db}.residual_or_l (k INT, payload INT);
CREATE TABLE ${case_db}.residual_or_r (k INT, payload INT);

-- @skip_result_check=true
-- @result_contains=INNER, eq:
-- @result_contains=predicates: CAST(l.k AS Int64) = 1 OR CAST(l.k AS Int64) = 2
-- @result_contains=r.k IS NOT NULL
-- @result_not_contains=UNION ALL
-- @result_not_contains=NEST LOOP JOIN
EXPLAIN VERBOSE
SELECT l.payload, r.payload
FROM ${case_db}.residual_or_l l
JOIN ${case_db}.residual_or_r r ON l.k = r.k
WHERE l.k = 1 OR l.k = 2;
