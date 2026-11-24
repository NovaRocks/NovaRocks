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
-- Check complete groups under ordered LIMIT with DOP 2. The expected keys
-- and counts are calculated from the input sequence and the two inserted rows.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- query 2
-- @skip_result_check=true
CREATE TABLE ${case_db}.all_t0 (
    c1 TINYINT,
    c2 SMALLINT,
    c3 INTEGER,
    c6 DATE,
    c7 TIMESTAMP(6),
    c8 VARCHAR,
    c9 VARCHAR,
    c13 TINYINT NOT NULL,
    c14 SMALLINT NOT NULL,
    c15 INTEGER NOT NULL
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.all_t0
SELECT CAST(x % 200 - 100 AS TINYINT),
       CAST(x % 200 AS SMALLINT),
       CAST(x % 200 AS INTEGER),
       date_add('day', x % 200, DATE '2000-01-01'),
       date_add('day', x % 200, TIMESTAMP '2000-01-01 00:00:00.000000'),
       CAST(x % 200 AS VARCHAR),
       CAST(x AS VARCHAR),
       CAST(x % 8 AS TINYINT),
       CAST(x % 8 AS SMALLINT),
       CAST(x % 16 AS INTEGER)
FROM TABLE(sequence(1, 30000)) AS g(x);

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.all_t0 VALUES ROW(NULL, NULL, NULL, NULL, NULL, NULL, NULL, -1, -2, -3);

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.all_t0 VALUES ROW(-1, -2, -3, NULL, NULL, NULL, NULL, -1, -2, -3);

-- query 6
-- @skip_result_check=true
SET SESSION pipeline_dop = 2;

-- query 7
SELECT COUNT(*) AS input_rows FROM ${case_db}.all_t0;

-- query 8
SELECT c1, c2, c3, COUNT(*) AS cnt
FROM ${case_db}.all_t0
GROUP BY c1, c2, c3
ORDER BY c1 NULLS FIRST, c2, c3
LIMIT 100;

-- query 9
SELECT c6, c7, c8, COUNT(*) AS cnt
FROM ${case_db}.all_t0
GROUP BY c6, c7, c8
ORDER BY c6 NULLS FIRST, c7, c8
LIMIT 100;

-- query 10
SELECT c13, c14, c15, COUNT(*) AS cnt
FROM ${case_db}.all_t0
GROUP BY c13, c14, c15
ORDER BY c13, c14, c15
LIMIT 100;

-- query 11
SELECT c9, COUNT(*) AS cnt
FROM ${case_db}.all_t0
GROUP BY c9
ORDER BY c9 NULLS FIRST
LIMIT 100;
