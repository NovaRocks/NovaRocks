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

-- @tags=optimizer,oq6,subquery_alias_fold
-- @order_sensitive=true
-- Test Objective:
-- 1. Derived-table aliases are carried as an identity Project's output_qualifier
--    (predicate-pushdown rework), not as a dedicated alias plan operator.
-- 2. Derived-table column aliases still expose the renamed output column.
-- 3. Single-use CTE inline keeps a real join plan (no dedicated alias operator).
DROP TABLE IF EXISTS ${case_db}.oq6_alias_base;
CREATE TABLE ${case_db}.oq6_alias_base (k INT, v INT);
INSERT INTO ${case_db}.oq6_alias_base VALUES (1, 10), (2, 20), (3, 30);
ANALYZE TABLE ${case_db}.oq6_alias_base;

EXPLAIN VERBOSE
SELECT s.k
FROM (SELECT k, v FROM ${case_db}.oq6_alias_base) s
WHERE s.v > 10;

EXPLAIN VERBOSE
SELECT renamed_k
FROM (SELECT k FROM ${case_db}.oq6_alias_base) s(renamed_k)
ORDER BY renamed_k;

SELECT renamed_k
FROM (SELECT k FROM ${case_db}.oq6_alias_base) s(renamed_k)
ORDER BY renamed_k;

EXPLAIN VERBOSE WITH w AS (
    SELECT k, v FROM ${case_db}.oq6_alias_base WHERE k < 3
)
SELECT count()
FROM ${case_db}.oq6_alias_base b
JOIN w w_alias ON b.k = w_alias.k;
