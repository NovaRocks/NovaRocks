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
-- @tags=iceberg_dml,decimal
-- Test Objective:
-- 1. Validate INSERT-SELECT writing DECIMAL values from a wider DECIMAL source into a narrower sink schema.
-- 2. Validate NULL propagation for DECIMAL columns through Iceberg table sink.
DROP TABLE IF EXISTS ${case_db}.t_decimal_insert_src;
DROP TABLE IF EXISTS ${case_db}.t_decimal_insert_sink;
CREATE TABLE ${case_db}.t_decimal_insert_src (
  id BIGINT,
  v DECIMAL(20, 6)
);
CREATE TABLE ${case_db}.t_decimal_insert_sink (
  id BIGINT,
  v DECIMAL(10, 3)
);
INSERT INTO ${case_db}.t_decimal_insert_src VALUES ROW(1, DECIMAL '123.456000'), ROW(2, (- (DECIMAL '99.125000'))), ROW(3, NULL);
INSERT INTO ${case_db}.t_decimal_insert_sink
SELECT id, v
FROM ${case_db}.t_decimal_insert_src;
SELECT id, v
FROM ${case_db}.t_decimal_insert_sink
ORDER BY id;
