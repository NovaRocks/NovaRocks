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
-- 1. Validate Iceberg v3 table accepts INSERT then DELETE and exposes correct visible rows.
-- 2. Prevent regression where DELETE is accepted but reads still return the deleted row.
DROP TABLE IF EXISTS ${case_db}.t_insert_delete_select;
CREATE TABLE ${case_db}.t_insert_delete_select (
  city_id INT,
  population INT,
  city STRING
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_insert_delete_select VALUES
  ROW(1, 100, 'Beijing'),
  ROW(2, 200, 'Shanghai'),
  ROW(3, 300, 'Shenzhen');
DELETE FROM ${case_db}.t_insert_delete_select
WHERE city_id = 2;
SELECT city_id, population, city
FROM ${case_db}.t_insert_delete_select
ORDER BY city_id;
