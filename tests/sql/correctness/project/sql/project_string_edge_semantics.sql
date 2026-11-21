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
-- @tags=project,string,regexp,field,unhex
-- Test Objective:
-- 1. Validate regexp_extract no-match behavior returns empty string, not NULL.
-- 3. Validate unhex() returns empty string for invalid/odd-length hex inputs.
-- Test Flow:
-- 1. Evaluate regexp_extract with no-match and NULL input.
-- 2. Evaluate field with NULL first argument.
-- 3. Evaluate unhex invalid/odd inputs and assert empty-string semantics.
SELECT
  regexp_extract('foo=123', 'bar=([0-9]+)', 1) AS re_no_match,
  regexp_extract(NULL, 'x', 1) AS re_null_input,
  hex(unhex('ZZ')) AS unhex_bad_hex,
  unhex('ZZ') IS NULL AS unhex_bad_is_null,
  hex(unhex('F')) AS unhex_odd_hex,
  unhex('F') IS NULL AS unhex_odd_is_null;
