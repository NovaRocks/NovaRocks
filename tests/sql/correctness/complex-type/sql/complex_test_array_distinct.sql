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

-- Migrated from dev/test/sql/test_array_fn/R/test_array_distinct
-- Test Objective:
-- Preserve array test coverage migrated from dev/test.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_array_distinct @slow @mac
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1 (
    c1 INT,
    c2 ARRAY<BIGINT>
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t2 (
    c1 INT,
    c2 ARRAY<ARRAY<BIGINT>>
) WITH (format_version = 3);

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into t1 select generate_series, concat([], generate_series) from TABLE(sequence(1, 100000)) AS generate_series(generate_series);

-- query 5
-- @skip_result_check=true
USE ${case_db};
insert into t2 select 1, array_agg(c2) from t1;

-- query 6
USE ${case_db};
select cardinality(array_distinct(c2)) from t2;

-- query 7
-- The four distinct values fit the host budget. Trino 484 exceeds its 4 MiB output check.
SELECT cardinality(array_distinct(a)) AS distinct_count
FROM (
    SELECT array_agg(rpad(CAST(x AS VARCHAR), 1048572, 'x') ORDER BY x) AS a
    FROM TABLE(sequence(1, 4)) AS input(x)
) AS t;

-- query 8
SELECT array_distinct(ARRAY[CAST('-0.0' AS DOUBLE), NULL, CAST('NaN' AS DOUBLE), 0e0, CAST('NaN' AS DOUBLE), NULL]) AS first_representatives;
