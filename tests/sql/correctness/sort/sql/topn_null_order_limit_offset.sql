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
-- @tags=sort,topn,null_order,offset
-- Test Objective:
-- 1. Validate ORDER BY with explicit NULLS FIRST/LAST under LIMIT/OFFSET.
-- 2. Prevent regressions in multi-key null ordering for TopN output slicing.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows containing NULL and non-NULL sort keys.
-- 3. Query ordered page with LIMIT/OFFSET and assert exact row order.
DROP TABLE IF EXISTS ${case_db}.t_topn_null_order_limit_offset;
CREATE TABLE ${case_db}.t_topn_null_order_limit_offset (
  id INT,
  k INT,
  s VARCHAR
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_topn_null_order_limit_offset VALUES
  ROW(1, NULL, 'a'),
  ROW(2, 2, 'x'),
  ROW(3, 1, 'z'),
  ROW(4, NULL, 'b'),
  ROW(5, 1, NULL),
  ROW(6, 3, 'm');
SELECT id, k, s
FROM ${case_db}.t_topn_null_order_limit_offset
ORDER BY k ASC NULLS LAST, s DESC NULLS FIRST, id ASC
OFFSET 1 LIMIT 4;
