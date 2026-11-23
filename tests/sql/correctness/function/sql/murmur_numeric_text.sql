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

-- Assert the exact VARCHAR rendering and CRC32 of REAL, DOUBLE, and integral inputs.
-- query 1
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE murmur_numeric_text (id INT, f REAL, d DOUBLE, n BIGINT) WITH (format_version = 3);
INSERT INTO murmur_numeric_text VALUES ROW(1,0,7,-9223372036854775808),ROW(2,1.25,1.25,0),ROW(3,NULL,NULL,NULL);

-- query 2
USE ${case_db};
SELECT id, CAST(f AS VARCHAR) AS f_text, CAST(d AS VARCHAR) AS d_text,
       CAST(n AS VARCHAR) AS n_text,
       crc32(to_utf8(CAST(f AS VARCHAR))) AS f_crc32,
       crc32(to_utf8(CAST(d AS VARCHAR))) AS d_crc32,
       crc32(to_utf8(CAST(n AS VARCHAR))) AS n_crc32
FROM murmur_numeric_text ORDER BY id;

-- query 3
USE ${case_db};
SELECT crc32(to_utf8(CAST(CAST(0 AS REAL) AS VARCHAR))) AS float_zero,
       crc32(to_utf8(CAST(CAST(7 AS DOUBLE) AS VARCHAR))) AS double_integral,
       crc32(to_utf8(CAST(CAST(NULL AS DOUBLE) AS VARCHAR))) AS null_crc32,
       crc32(to_utf8(CAST(CAST('-170141183460469231731687303715884105728' AS LARGEINT) AS VARCHAR))) AS largeint_min;

-- query 4
USE ${case_db};
SELECT CAST(CAST(123456789012345678901234567890.123456789 AS DOUBLE) AS VARCHAR) AS double_text,
       crc32(to_utf8(CAST(CAST(123456789012345678901234567890.123456789 AS DOUBLE) AS VARCHAR))) AS double_crc32,
       crc32(to_utf8(CAST(123456789012345678901234567890.123456789 AS VARCHAR))) AS exact_crc32;

-- query 5
-- @skip_result_check=true
USE ${case_db};
DROP TABLE murmur_numeric_text;
