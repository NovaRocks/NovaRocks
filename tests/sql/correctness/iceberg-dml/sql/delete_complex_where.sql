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
-- @tags=iceberg_dml,delete
-- Test Objective:
-- DELETE with a function-call WHERE on an Iceberg v3 table.
DROP TABLE IF EXISTS ${case_db}.t_delete_complex_where;
CREATE TABLE ${case_db}.t_delete_complex_where (
  id INT,
  k INT,
  label STRING
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_delete_complex_where VALUES ROW(1, 10, 'X'), ROW(2, 20, 'Y'), ROW(3, 30, 'Z');
DELETE FROM ${case_db}.t_delete_complex_where WHERE LOWER(label) = 'y';
SELECT id, k, label FROM ${case_db}.t_delete_complex_where ORDER BY id;
