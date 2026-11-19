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

-- @tags=optimizer,equivalence,join_reorder
-- Test Objective:
-- 1. Strict equivalence facts are the only facts exposed through logical properties.
-- 2. Null-safe equality remains a null-safe atom predicate in the final plan;
--    Rust unit tests cover the transitive edge synthesis guard directly.
DROP TABLE IF EXISTS ${case_db}.strict_eq_a;
DROP TABLE IF EXISTS ${case_db}.strict_eq_b;
DROP TABLE IF EXISTS ${case_db}.strict_eq_c;
SET SESSION cbo_max_reorder_node_use_exhaustive = 2;
CREATE TABLE ${case_db}.strict_eq_a (a1 BIGINT, a2 BIGINT, payload BIGINT);
CREATE TABLE ${case_db}.strict_eq_b (b BIGINT, payload BIGINT);
CREATE TABLE ${case_db}.strict_eq_c (c BIGINT, payload BIGINT);
INSERT INTO ${case_db}.strict_eq_a
    SELECT generate_series, generate_series, generate_series * 10
    FROM TABLE(generate_series(1, 10000));
INSERT INTO ${case_db}.strict_eq_b VALUES ROW(1, 100), ROW(2, 200), ROW(NULL, 300);
INSERT INTO ${case_db}.strict_eq_c VALUES ROW(1, 1000), ROW(2, 2000), ROW(NULL, 3000);
ANALYZE ${case_db}.strict_eq_a;
ANALYZE ${case_db}.strict_eq_b;
ANALYZE ${case_db}.strict_eq_c;

EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT a.payload, b.payload, c.payload
FROM (
    SELECT *
    FROM ${case_db}.strict_eq_a
    WHERE a1 = a2
) a
JOIN ${case_db}.strict_eq_b b ON a.a1 = b.b
JOIN ${case_db}.strict_eq_c c ON a.a2 = c.c;

SET SESSION cbo_max_reorder_node_use_exhaustive = 4;

EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT a.payload, b.payload, c.payload
FROM (
    SELECT *
    FROM ${case_db}.strict_eq_a
    WHERE a1  IS NOT DISTINCT FROM  a2
) a
JOIN ${case_db}.strict_eq_b b ON a.a1 = b.b
JOIN ${case_db}.strict_eq_c c ON a.a2 = c.c;
