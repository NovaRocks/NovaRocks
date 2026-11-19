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
-- @tags=iceberg_dml
-- Test Objective:
-- 1. Validate CREATE TABLE and multi-row INSERT for primitive + nullable values on Iceberg.
-- 2. Validate deterministic read-back for inserted rows.
DROP TABLE IF EXISTS ${case_db}.t_basic;
CREATE TABLE ${case_db}.t_basic (
  id BIGINT,
  name STRING,
  qty BIGINT
);
INSERT INTO ${case_db}.t_basic VALUES
  ROW(1, 'apple', 10),
  ROW(2, 'banana', 20),
  ROW(3, 'banana', NULL);
SELECT id, name, qty
FROM ${case_db}.t_basic
ORDER BY id;
