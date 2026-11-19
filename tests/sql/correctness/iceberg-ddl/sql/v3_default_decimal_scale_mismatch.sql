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
-- Test Point: DEFAULT for a DECIMAL column whose scale does not match the column's scale is rejected at parse/DDL time.
-- Method: CREATE v3 table with `c DECIMAL(10,2) DEFAULT 1.234`; expect a scale-mismatch error.
-- Scope: D2 type validation in parse_default_literal / default_literal_to_iceberg.

-- query 1
-- @expect_error=scale
CREATE TABLE ${case_db}.t_v3_default_dec (
  c DECIMAL(10,2) DEFAULT 1.234
) WITH (format_version = 3);
