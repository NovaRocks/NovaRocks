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
-- @tags=iceberg_dml,decimal,overflow
-- Test Objective:
-- 1. Validate overflow-range decimal rows can be sanitized to NULL before sink writes.
-- 2. Validate in-range boundary rows are still written after scale narrowing.
DROP TABLE IF EXISTS ${case_db}.t_decimal_overflow_sink;
CREATE TABLE ${case_db}.t_decimal_overflow_sink (
  id INT,
  v DECIMAL(10, 2)
);
INSERT INTO ${case_db}.t_decimal_overflow_sink
SELECT
  CAST(1 AS INT),
  CASE
    WHEN ABS(CAST(99999999.9949 AS DECIMAL(13, 4))) > 99999999.9999 THEN NULL
    ELSE CAST(99999999.9949 AS DECIMAL(13, 4))
  END;
INSERT INTO ${case_db}.t_decimal_overflow_sink
SELECT CAST(2 AS INT), CASE WHEN (ABS(CAST((- (DECIMAL '99999999.9949')) AS DECIMAL(13, 4))) > DECIMAL '99999999.9999') THEN NULL ELSE CAST((- (DECIMAL '99999999.9949')) AS DECIMAL(13, 4)) END;
INSERT INTO ${case_db}.t_decimal_overflow_sink
SELECT
  CAST(3 AS INT),
  CASE
    WHEN ABS(CAST(100000000.0000 AS DECIMAL(13, 4))) > 99999999.9999 THEN NULL
    ELSE CAST(100000000.0000 AS DECIMAL(13, 4))
  END;
INSERT INTO ${case_db}.t_decimal_overflow_sink
SELECT CAST(4 AS INT), CASE WHEN (ABS(CAST((- (DECIMAL '100000000.0000')) AS DECIMAL(13, 4))) > DECIMAL '99999999.9999') THEN NULL ELSE CAST((- (DECIMAL '100000000.0000')) AS DECIMAL(13, 4)) END;
INSERT INTO ${case_db}.t_decimal_overflow_sink
SELECT
  CAST(5 AS INT),
  CAST(NULL AS DECIMAL(13, 4));
SELECT
  id,
  v,
  IF(v IS NULL, 1, 0) AS is_null_v
FROM ${case_db}.t_decimal_overflow_sink
ORDER BY id;
