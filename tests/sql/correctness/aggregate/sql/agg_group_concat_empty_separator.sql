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
-- @tags=aggregate,group_concat,separator
-- Test Objective:
-- 1. Validate group_concat ORDER BY behavior with empty-string separator.
-- 2. Prevent regressions where empty separator falls back to default comma.
-- Test Flow:
-- 1. Create/reset grouped source table.
-- 2. Insert deterministic ordered rows including NULL values.
-- 3. Group and assert separator-sensitive concatenation outputs.
CREATE TABLE ${case_db}.t_agg_group_concat_empty_separator (
    g INT,
    ord INT,
    s STRING
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_agg_group_concat_empty_separator VALUES
    ROW(1, 1, 'a'),
    ROW(1, 2, 'b'),
    ROW(1, 3, NULL),
    ROW(1, 4, 'c'),
    ROW(2, 1, NULL),
    ROW(2, 2, 'x'),
    ROW(2, 3, 'y');

SELECT
    g,
    string_agg(s
,
 ''
ORDER BY ord
) AS gc
FROM ${case_db}.t_agg_group_concat_empty_separator
GROUP BY g
ORDER BY g;
