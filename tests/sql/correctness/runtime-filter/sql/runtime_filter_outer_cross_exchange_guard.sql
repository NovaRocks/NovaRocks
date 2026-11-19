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

-- @order_sensitive=true
-- @tags=runtime_filter,outer_join,cross_exchange_guard
-- Test Objective:
-- 1. Validate FULL OUTER JOIN null-preserving semantics with runtime filter enabled.
-- 2. Compare against RuntimeFilterPushDown-disabled execution to guard cross-exchange placement.
-- Test Flow:
-- 1. Create/reset left, right, and dimension tables.
-- 2. Insert deterministic rows including NULL and right-only FULL OUTER keys.
-- 3. Run the same guarded query with runtime filter enabled and disabled.
DROP TABLE IF EXISTS ${case_db}.rf_outer_l;
DROP TABLE IF EXISTS ${case_db}.rf_outer_r;
DROP TABLE IF EXISTS ${case_db}.rf_outer_dim;
CREATE TABLE ${case_db}.rf_outer_l (
    id INT,
    k INT
) WITH (format_version = 3);
CREATE TABLE ${case_db}.rf_outer_r (
    k INT
) WITH (format_version = 3);
CREATE TABLE ${case_db}.rf_outer_dim (
    k INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.rf_outer_l VALUES
    ROW(1, 10),
    ROW(2, NULL),
    ROW(3, 30);
INSERT INTO ${case_db}.rf_outer_r VALUES ROW(10), ROW(40);
INSERT INTO ${case_db}.rf_outer_dim VALUES ROW(10), ROW(30), ROW(40);

SET SESSION disable_optimizer_rules = '';
-- @explain_contains=HASH JOIN (PARTITIONED, FULL OUTER
-- @explain_not_contains=RUNTIME FILTER GRAPH
WITH x AS (
    SELECT l.id, l.k, r.k AS rk
    FROM ${case_db}.rf_outer_l l
    FULL OUTER JOIN ${case_db}.rf_outer_r r
      ON l.k = r.k
),
q AS (
    SELECT id, k, rk
    FROM x
    WHERE k IS NULL AND rk IS NULL
    UNION ALL
    SELECT x.id, x.k, x.rk
    FROM x
    INNER JOIN ${case_db}.rf_outer_dim d
      ON COALESCE(x.k, x.rk) = d.k
)
SELECT id, k, rk
FROM q
ORDER BY COALESCE(id, 999999), COALESCE(k, rk);

SET SESSION disable_optimizer_rules = 'RuntimeFilterPushDown';
WITH x AS (
    SELECT l.id, l.k, r.k AS rk
    FROM ${case_db}.rf_outer_l l
    FULL OUTER JOIN ${case_db}.rf_outer_r r
      ON l.k = r.k
),
q AS (
    SELECT id, k, rk
    FROM x
    WHERE k IS NULL AND rk IS NULL
    UNION ALL
    SELECT x.id, x.k, x.rk
    FROM x
    INNER JOIN ${case_db}.rf_outer_dim d
      ON COALESCE(x.k, x.rk) = d.k
)
SELECT id, k, rk
FROM q
ORDER BY COALESCE(id, 999999), COALESCE(k, rk);

SET SESSION disable_optimizer_rules = '';
