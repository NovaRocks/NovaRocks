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

-- @tags=complex,exchange,count-only
-- Test Objective:
-- Preserve source multiplicity when unused outputs are pruned, and preserve
-- count-only results across broadcast and explicit partitioned join paths.
-- The real Driver unit tests additionally assert zero-column wire geometry;
-- COUNT output alone is not evidence that a particular SQL edge has no columns.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.count_seed (id INT NOT NULL)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.count_seed VALUES (7), (19);

-- query 2
SELECT COUNT(*) AS row_count
FROM ${case_db}.count_seed l CROSS JOIN ${case_db}.count_seed r;

-- query 3
-- @skip_result_check=true
SET cbo_broadcast_node_mem_budget_bytes = 0;

-- query 4
-- @explain_contains=HASH JOIN (PARTITIONED, INNER
SELECT COUNT(*) AS row_count
FROM ${case_db}.count_seed l JOIN [shuffle] ${case_db}.count_seed r ON l.id = r.id;
