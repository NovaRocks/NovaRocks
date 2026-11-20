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
-- @tags=iceberg_dml,decimal,rounding
-- Test Objective:
-- 1. Validate decimal scale narrowing writes apply deterministic rounding for positive and negative values.
-- 2. Validate NULL propagation for decimal rows during sink writes.
DROP TABLE IF EXISTS ${case_db}.t_decimal_rounding_sink;
CREATE TABLE ${case_db}.t_decimal_rounding_sink (
  id INT,
  v DECIMAL(10, 2)
);
INSERT INTO ${case_db}.t_decimal_rounding_sink
SELECT CAST(1 AS INT), CAST(1.2344 AS DECIMAL(10, 4));
INSERT INTO ${case_db}.t_decimal_rounding_sink
SELECT CAST(2 AS INT), CAST(1.2356 AS DECIMAL(10, 4));
INSERT INTO ${case_db}.t_decimal_rounding_sink
SELECT CAST(3 AS INT), CAST((- (DECIMAL '2.3444')) AS DECIMAL(10, 4));
INSERT INTO ${case_db}.t_decimal_rounding_sink
SELECT CAST(4 AS INT), CAST((- (DECIMAL '2.3456')) AS DECIMAL(10, 4));
INSERT INTO ${case_db}.t_decimal_rounding_sink
SELECT CAST(5 AS INT), CAST(NULL AS DECIMAL(10, 4));
SELECT id, v
FROM ${case_db}.t_decimal_rounding_sink
ORDER BY id;
