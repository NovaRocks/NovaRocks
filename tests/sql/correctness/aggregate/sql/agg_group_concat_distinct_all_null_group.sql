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
-- @tags=aggregate,group_concat,distinct,null
-- Test Objective:
-- 1. Validate group_concat DISTINCT+ORDER output when one group has only NULL values.
-- 2. Prevent regressions where all-NULL groups return empty string instead of NULL.
-- Test Flow:
-- 1. Create/reset grouped source table.
-- 2. Insert deterministic rows with one all-NULL group and one mixed group.
-- 3. Group and assert ordered group_concat outputs.
CREATE TABLE ${case_db}.t_agg_group_concat_distinct_all_null_group (
    g INT,
    s VARCHAR
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_agg_group_concat_distinct_all_null_group VALUES
    ROW(1, NULL),
    ROW(1, NULL),
    ROW(2, 'a'),
    ROW(2, 'c'),
    ROW(2, 'a'),
    ROW(2, NULL),
    ROW(2, 'b');

SELECT
    g,
    LISTAGG(DISTINCT CAST(s AS VARCHAR), '|') WITHIN GROUP (ORDER BY CAST(s AS VARCHAR) DESC) AS gc
FROM ${case_db}.t_agg_group_concat_distinct_all_null_group
GROUP BY g
ORDER BY g;
