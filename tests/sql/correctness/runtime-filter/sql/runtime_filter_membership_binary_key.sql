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
-- @tags=runtime_filter,membership_domain,binary_key
-- Test Objective:
-- 1. A VARBINARY join key must not install a canonical membership filter that
--    cannot represent it.
-- 2. Both matching probe rows survive, with the same result when RF is disabled.
-- Probe IDs one and three carry the build key 0x01; ID two carries 0x02.
DROP TABLE IF EXISTS ${case_db}.rf_membership_binary_probe;
DROP TABLE IF EXISTS ${case_db}.rf_membership_binary_build;
CREATE TABLE ${case_db}.rf_membership_binary_probe (id INT, k VARBINARY)
TBLPROPERTIES ("format-version" = "3");
CREATE TABLE ${case_db}.rf_membership_binary_build (id INT, k VARBINARY)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.rf_membership_binary_probe VALUES
    (1, x'01'), (2, x'02'), (3, x'01');
INSERT INTO ${case_db}.rf_membership_binary_build VALUES (10, x'01');

-- Keep the small deterministic input eligible for ordinary RF placement gates.
SET global_runtime_filter_build_min_size = 0;
SET global_runtime_filter_probe_min_size = 0;
SET global_runtime_filter_probe_min_selectivity = 0.0;
SET enable_global_runtime_filter = true;
-- @explain_contains=HASH JOIN
-- @explain_not_contains=RUNTIME FILTER GRAPH
SELECT p.id
FROM ${case_db}.rf_membership_binary_probe p
JOIN ${case_db}.rf_membership_binary_build b ON p.k = b.k
ORDER BY p.id;

SET enable_global_runtime_filter = false;
SELECT p.id
FROM ${case_db}.rf_membership_binary_probe p
JOIN ${case_db}.rf_membership_binary_build b ON p.k = b.k
ORDER BY p.id;

SET enable_global_runtime_filter = true;
DROP TABLE ${case_db}.rf_membership_binary_probe;
DROP TABLE ${case_db}.rf_membership_binary_build;
