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
-- @tags=aggregate,group_concat
-- Test Objective:
-- 1. Validate group_concat ORDER BY behavior with explicit separator.
-- 2. Prevent regressions where merge-stage extra separator args break intermediate decoding.
-- Test Flow:
-- 1. Create/reset source table with nullable string input.
-- 2. Insert deterministic rows with duplicates and NULL.
-- 3. Assert ordered global group_concat output.
CREATE TABLE ${case_db}.t_agg_group_concat_ordered (
    k INT,
    s STRING
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_agg_group_concat_ordered VALUES
    ROW(1, 'b'),
    ROW(2, 'a'),
    ROW(3, 'b'),
    ROW(4, NULL),
    ROW(5, 'c');

SELECT string_agg(s
,
 '|'
ORDER BY s
) AS gc
FROM ${case_db}.t_agg_group_concat_ordered;
