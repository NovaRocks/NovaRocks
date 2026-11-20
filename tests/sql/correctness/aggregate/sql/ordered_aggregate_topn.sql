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

-- Test Objective:
-- Keep complete-group TopN results reachable independently of the unsupported
-- Global ordered-filter queries earlier in compressed_key.sql. Preserve that
-- case and its original results; this case does not qualify its skipped tail.
-- Inputs: the original 24-column table, 30002 rows, DOP 2, and concat limit.
-- Oracle: verbatim compressed_key.result blocks 137-140, with markers mapped
-- to 8-11. No expected value is recorded from this implementation.
-- Maintained by the aggregate/runtime-filter owners; run with the aggregate
-- suite when those owners change and before merging related changes. The SQL
-- runner owns case database isolation, suite cleanup, and cluster shutdown.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_agg_compressed_key
-- query 2
-- @skip_result_check=true
USE ${case_db};
create table all_t0 (
    c1 tinyint,
    c2 smallint,
    c3 int,
    c4 bigint,
    c5 largeint,
    c6 date,
    c7 datetime,
    c8 string,
    c9 string,
    c10 char(100),
    c11 float,
    c12 double,
    c13 tinyint NOT NULL,
    c14 smallint NOT NULL,
    c15 int NOT NULL,
    c16 bigint NOT NULL,
    c17 largeint NOT NULL,
    c18 date NOT NULL,
    c19 datetime NOT NULL,
    c20 string NOT NULL,
    c21 string NOT NULL,
    c22 char(100) NOT NULL,
    c23 float NOT NULL,
    c24 double NOT NULL
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into all_t0 SELECT x%200, x%200, x%200, x%200, x%200, x, x, x%200, x, x, x, x, x % 8, x % 8, x % 16, x %200, x%200, '2020-02-02', '2020-02-02', x%200, x, x, x, x FROM TABLE(generate_series(1,  30000)) as g(x);

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into all_t0 values ROW(null, null, null, null, null, null, null, null, null, null, null, null, -1,-2,-3,-4,-5, '2000-01-28', '2000-01-28', 'literal', 'literal', 'literal', -1, -1);

-- query 5
-- @skip_result_check=true
USE ${case_db};
insert into all_t0 values ROW(-1, -2, -3, null, null, null, null, null, null, null, null, null, -1,-2,-3,-4,-5, '2000-01-28', '2000-01-28', 'literal', 'literal', 'literal', -1, -1);

-- query 6
-- @skip_result_check=true
USE ${case_db};
SET SESSION pipeline_dop = 2;

-- query 7
-- @skip_result_check=true
USE ${case_db};
SET SESSION group_concat_max_len = 65535;

-- query 8
USE ${case_db};
WITH result AS (
    SELECT c1, c2, c3, COUNT() AS cnt FROM all_t0 GROUP BY c1, c2, c3 ORDER BY c1, c2, c3 LIMIT 100
) SELECT 'Test Case 39' AS test_name, MD5(GROUP_CONCAT(CAST(CAST(c1 AS STRING) AS BOOLEAN) OR CAST(':' AS BOOLEAN) OR CAST(CAST(c2 AS STRING) AS BOOLEAN) OR CAST(':' AS BOOLEAN) OR CAST(CAST(c3 AS STRING) AS BOOLEAN) OR CAST(':' AS BOOLEAN) OR CAST(CAST(cnt AS STRING) AS BOOLEAN))) AS result_hash FROM result;

-- query 9
USE ${case_db};
WITH result AS (
    SELECT c6, c7, c8, COUNT() AS cnt FROM all_t0 GROUP BY c6, c7, c8 ORDER BY c6, c7, c8 LIMIT 100
) SELECT 'Test Case 40' AS test_name, MD5(GROUP_CONCAT(CAST(CAST(c6 AS STRING) AS BOOLEAN) OR CAST(':' AS BOOLEAN) OR CAST(CAST(c7 AS STRING) AS BOOLEAN) OR CAST(':' AS BOOLEAN) OR CAST(c8 AS BOOLEAN) OR CAST(':' AS BOOLEAN) OR CAST(CAST(cnt AS STRING) AS BOOLEAN))) AS result_hash FROM result;

-- query 10
USE ${case_db};
WITH result AS (
    SELECT c13, c14, c15, COUNT() AS cnt FROM all_t0 GROUP BY c13, c14, c15 ORDER BY c13, c14, c15 LIMIT 100
) SELECT 'Test Case 42' AS test_name, MD5(GROUP_CONCAT(CAST(CAST(c13 AS STRING) AS BOOLEAN) OR CAST(':' AS BOOLEAN) OR CAST(CAST(c14 AS STRING) AS BOOLEAN) OR CAST(':' AS BOOLEAN) OR CAST(CAST(c15 AS STRING) AS BOOLEAN) OR CAST(':' AS BOOLEAN) OR CAST(CAST(cnt AS STRING) AS BOOLEAN))) AS result_hash FROM result;

-- query 11
USE ${case_db};
WITH result AS (
    SELECT c9, COUNT() AS cnt FROM all_t0 GROUP BY c9 ORDER BY c9 LIMIT 100
) SELECT 'Test Case 51' AS test_name, MD5(GROUP_CONCAT(CAST(c9 AS BOOLEAN) OR CAST(':' AS BOOLEAN) OR CAST(CAST(cnt AS STRING) AS BOOLEAN))) AS result_hash FROM result;
