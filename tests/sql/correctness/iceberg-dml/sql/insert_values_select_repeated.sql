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

-- @order_sensitive=false
-- @tags=iceberg_dml,insert_values
-- Test Objective:
-- 1. Validate repeated INSERT ... VALUES writes are visible through SELECT.
-- 2. Prevent regressions where first insert succeeds but subsequent inserts are lost.
DROP TABLE IF EXISTS ${case_db}.t_insert_values_select_repeated;
CREATE TABLE ${case_db}.t_insert_values_select_repeated (
  c1 BIGINT
);
INSERT INTO ${case_db}.t_insert_values_select_repeated VALUES ROW(11);
INSERT INTO ${case_db}.t_insert_values_select_repeated VALUES ROW(22), ROW(33);
INSERT INTO ${case_db}.t_insert_values_select_repeated VALUES ROW(44);
SELECT c1
FROM ${case_db}.t_insert_values_select_repeated;
