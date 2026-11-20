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
-- @tags=aggregate,count_distinct,multi_column
-- Test Objective:
-- 1. Validate COUNT(DISTINCT a,b) for composite keys with duplicates and NULLs.
-- 2. Prevent regressions in multi-column distinct cardinality.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic duplicated and distinct value pairs.
-- 3. Compute COUNT(DISTINCT a,b) and assert scalar output.
CREATE TABLE ${case_db}.t_agg_count_distinct_multi_column (
    a INT,
    b VARCHAR(20)
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_agg_count_distinct_multi_column VALUES
    ROW(1, 'x'),
    ROW(1, 'x'),
    ROW(1, 'y'),
    ROW(2, 'x'),
    ROW(2, 'x'),
    ROW(1, NULL),
    ROW(NULL, 'x'),
    ROW(NULL, NULL);

SELECT COUNT(DISTINCT a, b) AS cd_ab
FROM ${case_db}.t_agg_count_distinct_multi_column;

SELECT COUNT(DISTINCT a, b) AS cd_ab
FROM ${case_db}.t_agg_count_distinct_multi_column
WHERE FALSE;
