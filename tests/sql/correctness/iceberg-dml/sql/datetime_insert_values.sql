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
-- @tags=iceberg_dml,datetime
-- Test Objective:
-- 1. Validate DATETIME writes through direct VALUES and INSERT-SELECT constant expressions.
-- 2. Validate derived function result (YEAR) is consistent after sink persistence.
DROP TABLE IF EXISTS ${case_db}.t_datetime_insert_values;
CREATE TABLE ${case_db}.t_datetime_insert_values (
  id INT,
  dt DATETIME
);
INSERT INTO ${case_db}.t_datetime_insert_values VALUES
  ROW(1, '2024-03-01 10:20:30');
INSERT INTO ${case_db}.t_datetime_insert_values
SELECT 2, CAST('2024-12-31 23:59:59' AS TIMESTAMP);
INSERT INTO ${case_db}.t_datetime_insert_values
SELECT 3, NULL;
SELECT id, dt, YEAR(dt) AS y
FROM ${case_db}.t_datetime_insert_values
ORDER BY id;
