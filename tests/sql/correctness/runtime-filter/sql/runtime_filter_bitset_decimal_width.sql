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

-- @tags=runtime_filter,bitset,decimal
-- Validate DECIMAL32/DECIMAL64 runtime-filter semantics while DECIMAL128 remains
-- a non-bitset decimal width that still filters correctly through bloom/min-max.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.rf_bitset_decimal_probe (
  id INT NULL,
  d32 DECIMAL(9,0) NULL,
  d64 DECIMAL(18,0) NULL,
  d128 DECIMAL(19,0) NULL
)
TBLPROPERTIES ("format-version" = "3");

CREATE TABLE ${case_db}.rf_bitset_decimal_build (
  id INT NULL,
  d32 DECIMAL(9,0) NULL,
  d64 DECIMAL(18,0) NULL,
  d128 DECIMAL(19,0) NULL
)
TBLPROPERTIES ("format-version" = "3");

CREATE TABLE ${case_db}.rf_bitset_decimal_wide_build (
  id INT NULL,
  d64 DECIMAL(18,0) NULL
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.rf_bitset_decimal_probe VALUES
  (1, 1, 1, 1),
  (2, 2, 2, 2),
  (3, 3, 3, 3),
  (4, 4, 4, 4),
  (5, NULL, NULL, NULL);

INSERT INTO ${case_db}.rf_bitset_decimal_build VALUES
  (2, 2, 2, 2),
  (4, 4, 4, 4),
  (5, NULL, NULL, NULL);

INSERT INTO ${case_db}.rf_bitset_decimal_wide_build VALUES
  (1, 1),
  (6, 1000000000000);

-- query 2
SELECT count(1)
FROM ${case_db}.rf_bitset_decimal_probe p
JOIN  ${case_db}.rf_bitset_decimal_build b USING(d32);

-- query 3
SELECT count(1)
FROM ${case_db}.rf_bitset_decimal_probe p
JOIN  ${case_db}.rf_bitset_decimal_build b USING(d64);

-- query 4
SELECT count(1)
FROM ${case_db}.rf_bitset_decimal_probe p
JOIN  ${case_db}.rf_bitset_decimal_build b USING(d128);

-- query 5
SELECT count(1)
FROM ${case_db}.rf_bitset_decimal_probe p
JOIN  ${case_db}.rf_bitset_decimal_build b ON p.d64 <=> b.d64;

-- query 6
SELECT count(1)
FROM ${case_db}.rf_bitset_decimal_probe p
JOIN  ${case_db}.rf_bitset_decimal_wide_build b USING(d64);
