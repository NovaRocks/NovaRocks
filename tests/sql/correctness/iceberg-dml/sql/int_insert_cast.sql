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
-- @tags=iceberg_dml,cast
-- Test Objective:
-- 1. Regression coverage for writing INT target columns through Iceberg sink.
-- 2. Validate INT INSERT casts with statement-local integer inputs.
DROP TABLE IF EXISTS ${case_db}.t_int_insert_regression;
CREATE TABLE ${case_db}.t_int_insert_regression (
  id INT,
  v INT
);
SET @i = 1;
INSERT INTO ${case_db}.t_int_insert_regression VALUES (1, 1);
INSERT INTO ${case_db}.t_int_insert_regression VALUES (2, 2);
INSERT INTO ${case_db}.t_int_insert_regression VALUES (3, 3);
SELECT id, v
FROM ${case_db}.t_int_insert_regression
ORDER BY id;
