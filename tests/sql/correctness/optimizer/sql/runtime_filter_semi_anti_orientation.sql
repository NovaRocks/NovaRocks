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
-- LEFT SEMI builds runtime filters now that the complete-only lifecycle and
-- presence-only build-key collection make them safe; LEFT ANTI stays RF-free.
-- The reversed ON predicate still preserves join shape.
DROP TABLE IF EXISTS ${case_db}.rf_side_l;
DROP TABLE IF EXISTS ${case_db}.rf_side_r;
CREATE TABLE ${case_db}.rf_side_l (k INT, v INT);
CREATE TABLE ${case_db}.rf_side_r (k INT, v INT);
INSERT INTO ${case_db}.rf_side_l
    SELECT generate_series, generate_series
    FROM TABLE(sequence(1, 100000)) AS generate_series(generate_series);
INSERT INTO ${case_db}.rf_side_r VALUES ROW(1, 10), ROW(2, 20), ROW(3, 30);
ANALYZE ${case_db}.rf_side_l;
ANALYZE ${case_db}.rf_side_r;

-- @explain_contains=HASH JOIN (BROADCAST, LEFT SEMI
-- @explain_contains=producer binding
-- @explain_contains=consumer binding
SELECT count(*)
FROM ${case_db}.rf_side_l l
WHERE EXISTS (SELECT 1 FROM ${case_db}.rf_side_r r WHERE r.k = l.k);

-- @explain_contains=HASH JOIN (BROADCAST, LEFT ANTI
-- @explain_not_contains=producer binding
-- @explain_not_contains=consumer binding
SELECT count(*)
FROM ${case_db}.rf_side_l l
WHERE NOT EXISTS (SELECT 1 FROM ${case_db}.rf_side_r r WHERE r.k = l.k);
