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

-- Migrated from dev/test/sql/test_function/T/test_hash
-- Test Objective:
-- 1. Validate xx_hash3_128() function for single-column and multi-column inputs.
-- 2. Verify high/low 64-bit extraction using bit_shift_right and bitand on 128-bit hash result.
-- 3. Cover string columns with different values to confirm distinct hashes.

-- query 1
-- Setup: create table and insert test data
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t0 (id int, a string, b string)
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t0(id, a, b) VALUES (0, "hello", "world"), (1, "hello", "starrocks");

-- query 3
-- xx_hash3_128 on individual columns, extracting high and low 64 bits
USE ${case_db};
SELECT CAST(bit_shift_right(xx_hash3_128(a), 64) AS VARCHAR) AS `bit_shift_right(xx_hash3_128(a), 64)`,
    CAST(bitand(xx_hash3_128(a), CAST('18446744073709551615' AS LARGEINT)) AS VARCHAR) AS `bitand(xx_hash3_128(a), 18446744073709551615)`,
    a,
    CAST(bit_shift_right(xx_hash3_128(b), 64) AS VARCHAR) AS `bit_shift_right(xx_hash3_128(b), 64)`,
    CAST(bitand(xx_hash3_128(b), CAST('18446744073709551615' AS LARGEINT)) AS VARCHAR) AS `bitand(xx_hash3_128(b), 18446744073709551615)`,
    b
FROM t0 WHERE id = 1;

-- query 4
-- xx_hash3_128 on multiple columns combined
USE ${case_db};
SELECT CAST(bit_shift_right(xx_hash3_128(a,b), 64) AS VARCHAR) AS `bit_shift_right(xx_hash3_128(a, b), 64)`,
    CAST(bitand(xx_hash3_128(a,b), CAST('18446744073709551615' AS LARGEINT)) AS VARCHAR) AS `bitand(xx_hash3_128(a, b), 18446744073709551615)`,
    concat(a, b) FROM t0 ORDER BY id;
