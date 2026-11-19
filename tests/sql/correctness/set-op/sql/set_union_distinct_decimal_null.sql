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
-- @tags=set_op,union,decimal,null
-- Test Objective:
-- 1. Validate UNION DISTINCT semantics on DECIMAL values with NULLs.
-- 2. Prevent regressions in decimal key comparison and deduplication logic.
-- Test Flow:
-- 1. Build two DECIMAL row sets with duplicates and NULLs.
-- 2. Apply UNION (DISTINCT).
-- 3. Assert deterministic ordered output.
DROP TABLE IF EXISTS ${case_db}.t_set_union_decimal_l;
DROP TABLE IF EXISTS ${case_db}.t_set_union_decimal_r;
CREATE TABLE ${case_db}.t_set_union_decimal_l (
    d DECIMAL(10, 2)
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_set_union_decimal_r (
    d DECIMAL(10, 2)
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_set_union_decimal_l VALUES
    ROW(1.20),
    ROW(NULL),
    ROW(2.50);

INSERT INTO ${case_db}.t_set_union_decimal_r VALUES
    ROW(1.20),
    ROW(3.00),
    ROW(NULL);

SELECT d
FROM (
    (
        SELECT d
        FROM ${case_db}.t_set_union_decimal_l
    )
    UNION
    (
        SELECT d
        FROM ${case_db}.t_set_union_decimal_r
    )
) t
ORDER BY d IS NULL, d;
