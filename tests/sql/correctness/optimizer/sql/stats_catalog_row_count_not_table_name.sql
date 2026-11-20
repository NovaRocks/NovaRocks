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

-- @tags=optimizer,stats,regression
-- Test Objective:
-- Regression input for the removed name-based scan row fallback. The table
-- name intentionally contains sales/fact/dim/lineitem tokens; scan row count
-- must come from ANALYZE/catalog statistics, not table-name heuristics.
DROP TABLE IF EXISTS ${case_db}.misleading_sales_fact_dim_lineitem;
CREATE TABLE ${case_db}.misleading_sales_fact_dim_lineitem (
    id INT,
    payload INT
);
INSERT INTO ${case_db}.misleading_sales_fact_dim_lineitem VALUES
    ROW(1, 10),
    ROW(2, 20),
    ROW(3, 30);
ANALYZE ${case_db}.misleading_sales_fact_dim_lineitem;

EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT payload
FROM ${case_db}.misleading_sales_fact_dim_lineitem;
