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
-- @tags=aggregate,having
-- Test Objective:
-- 1. Validate HAVING filtering on aggregate outputs.
-- 2. Prevent regressions in post-aggregation predicate evaluation.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows across groups.
-- 3. Apply GROUP BY + HAVING and assert ordered output.
CREATE TABLE ${case_db}.t_agg_having_threshold (
    g INT,
    v INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_agg_having_threshold VALUES
    ROW(1, 5),
    ROW(1, 7),
    ROW(2, 9),
    ROW(2, 15),
    ROW(3, 30);

SELECT
    g,
    SUM(v) AS s_v
FROM ${case_db}.t_agg_having_threshold
GROUP BY g
HAVING SUM(v) >= 20
ORDER BY g;
