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

-- Test Objective: typed numeric Murmur inputs share the explicit VARCHAR conversion contract.
-- query 1
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE murmur_numeric_text (id INT, f FLOAT, d DOUBLE, n BIGINT)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO murmur_numeric_text VALUES (1,0,7,-9223372036854775808),(2,1.25,1.25,0),(3,NULL,NULL,NULL);

-- query 2
USE ${case_db};
SELECT id, CAST(f AS VARCHAR) AS f_text, CAST(d AS VARCHAR) AS d_text,
       murmur_hash3_32(f) <=> murmur_hash3_32(CAST(f AS VARCHAR)) AS f_same,
       murmur_hash3_32(d) <=> murmur_hash3_32(CAST(d AS VARCHAR)) AS d_same,
       murmur_hash3_32(n) <=> murmur_hash3_32(CAST(n AS VARCHAR)) AS n_same
FROM murmur_numeric_text ORDER BY id;

-- query 3
USE ${case_db};
SELECT murmur_hash3_32(CAST(0 AS FLOAT)) AS float_zero,
       murmur_hash3_32(CAST(7 AS DOUBLE)) AS double_integral,
       murmur_hash3_32(CAST(NULL AS DOUBLE)) AS null_hash,
       murmur_hash3_32(CAST('-170141183460469231731687303715884105728' AS LARGEINT)) AS largeint_min,
       murmur_hash3_32(CAST(0 AS FLOAT), CAST(7 AS DOUBLE), CAST('-9223372036854775808' AS BIGINT)) AS mixed_variadic;

-- query 4
USE ${case_db};
SELECT CAST(CAST(123456789012345678901234567890.123456789 AS DOUBLE) AS VARCHAR) AS double_text,
       murmur_hash3_32(CAST(123456789012345678901234567890.123456789 AS DOUBLE)) AS double_hash,
       murmur_hash3_32(123456789012345678901234567890.123456789) AS exact_hash;

-- query 5
-- @skip_result_check=true
USE ${case_db};
DROP TABLE murmur_numeric_text;
