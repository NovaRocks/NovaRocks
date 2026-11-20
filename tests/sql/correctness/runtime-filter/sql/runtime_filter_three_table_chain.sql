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
-- @tags=runtime_filter,multi_join
-- Test Objective:
-- 1. Validate multi-join chain semantics under runtime filter propagation.
-- 2. Prevent regressions where intermediate runtime filters over-prune downstream joins.
-- Test Flow:
-- 1. Create/reset three join tables.
-- 2. Insert deterministic chain keys.
-- 3. Execute chained inner joins and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_rf_three_table_chain_a;
DROP TABLE IF EXISTS ${case_db}.t_rf_three_table_chain_b;
DROP TABLE IF EXISTS ${case_db}.t_rf_three_table_chain_c;
CREATE TABLE ${case_db}.t_rf_three_table_chain_a (
    id INT,
    k1 INT
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_rf_three_table_chain_b (
    k1 INT,
    k2 INT
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_rf_three_table_chain_c (
    k2 INT,
    payload VARCHAR(20)
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_rf_three_table_chain_a VALUES
    ROW(1, 10),
    ROW(2, 20),
    ROW(3, 30);

INSERT INTO ${case_db}.t_rf_three_table_chain_b VALUES
    ROW(10, 100),
    ROW(20, 200),
    ROW(40, 400);

INSERT INTO ${case_db}.t_rf_three_table_chain_c VALUES
    ROW(100, 'c100'),
    ROW(200, 'c200'),
    ROW(300, 'c300');

SELECT a.id, a.k1, b.k2, c.payload
FROM ${case_db}.t_rf_three_table_chain_a a
JOIN ${case_db}.t_rf_three_table_chain_b b
  ON a.k1 = b.k1
JOIN ${case_db}.t_rf_three_table_chain_c c
  ON b.k2 = c.k2
ORDER BY a.id;
