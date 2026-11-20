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

-- @tags=optimizer,oq10,runtime_filter
-- Test Objective:
-- Runtime filters remap probe expressions through projection aliases and keep
-- conservative set-operation plan coverage.
DROP TABLE IF EXISTS ${case_db}.rf_remap_a;
DROP TABLE IF EXISTS ${case_db}.rf_remap_b;
DROP TABLE IF EXISTS ${case_db}.rf_remap_c;
CREATE TABLE ${case_db}.rf_remap_a (k INT, v INT);
CREATE TABLE ${case_db}.rf_remap_b (k INT, v INT);
CREATE TABLE ${case_db}.rf_remap_c (k INT, v INT);
INSERT INTO ${case_db}.rf_remap_a
    SELECT generate_series, generate_series
    FROM TABLE(generate_series(1, 100000));
INSERT INTO ${case_db}.rf_remap_b VALUES ROW(1, 10), ROW(2, 20), ROW(3, 30);
INSERT INTO ${case_db}.rf_remap_c VALUES ROW(4, 40), ROW(5, 50), ROW(6, 60);
ANALYZE ${case_db}.rf_remap_a;
ANALYZE ${case_db}.rf_remap_b;
ANALYZE ${case_db}.rf_remap_c;

-- @explain_contains=HASH JOIN (BROADCAST, LEFT SEMI
-- @explain_contains=producer binding
-- @explain_contains=consumer binding
SELECT count(*)
FROM (
    SELECT k AS ak, v
    FROM ${case_db}.rf_remap_a
) pa
WHERE EXISTS (SELECT 1 FROM ${case_db}.rf_remap_b b WHERE pa.ak = b.k);

-- @explain_contains=UNION ALL
-- @explain_contains=producer binding
SELECT count(*)
FROM (
    SELECT k
    FROM ${case_db}.rf_remap_a
    UNION ALL
    SELECT k
    FROM ${case_db}.rf_remap_c
) u
JOIN ${case_db}.rf_remap_b b ON u.k = b.k;
