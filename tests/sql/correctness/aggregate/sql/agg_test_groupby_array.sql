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

-- Migrated from dev/test/sql/test_agg/R/test_groupby_array
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained SQL test case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_groupby_array @slow @mac
-- query 2
-- @skip_result_check=true
USE ${case_db};

-- query 3
USE ${case_db};
WITH input AS (
    SELECT array_agg(CAST(x AS VARCHAR) ORDER BY x) AS a
    FROM TABLE(sequence(1, 2000000, 1)) AS series(x)
    UNION ALL SELECT ARRAY['A', 'B', 'C'] AS a
)
SELECT count(*) FROM (SELECT a FROM input GROUP BY 1) AS t;

-- query 4
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE "t1" (
  "id" INTEGER,
  "array_varchar" array<varchar(100)>
) WITH (format_version = 3);

-- query 5
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1
SELECT CAST(series_value AS INTEGER),
       transform(
           IF(series_value % 100 = 0,
              CAST(ARRAY[] AS ARRAY(BIGINT)),
              sequence(1, series_value % 100, 1)),
           x -> CAST(x AS VARCHAR(100)))
FROM TABLE(sequence(1, 10000)) AS input_rows(series_value);

-- query 6
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1
WITH long_input AS (
    SELECT array_agg(x ORDER BY x) AS a
    FROM TABLE(sequence(1, 100000, 1)) AS input_rows(x)
)
SELECT id, transform(a, x -> CAST(x AS VARCHAR(100)))
FROM long_input CROSS JOIN (VALUES ROW(0), ROW(10001)) AS ids(id);

-- query 7
USE ${case_db};
select count() from (select distinct array_varchar from t1) t;
