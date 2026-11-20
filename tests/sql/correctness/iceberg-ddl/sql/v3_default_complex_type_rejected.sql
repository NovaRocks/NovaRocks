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
-- Test Point: DEFAULT for complex/unsupported types (Array/Struct/etc.) is rejected at DDL time.
-- Method: ALTER ADD COLUMN with an ARRAY type and a DEFAULT expression; expect a "DEFAULT not supported" error.
-- Scope: D2 — complex-type defaults are out of scope; reject at parse time.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_default_complex ;
CREATE TABLE ${case_db}.t_v3_default_complex (
  id INT
) WITH (format_version = 3);

-- query 2
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
ALTER TABLE ${case_db}.t_v3_default_complex ADD COLUMN c ARRAY<INT> DEFAULT [1,2];

-- query 3
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_default_complex ;
