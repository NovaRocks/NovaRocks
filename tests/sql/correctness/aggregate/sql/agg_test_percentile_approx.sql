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

-- Migrated from dev/test/sql/test_agg_function/R/test_percentile_approx
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained SQL test case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_percentile_approx
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1 (
    c1 int,
    c2 double
    ) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into t1 select generate_series, generate_series from table(generate_series(1, 50000, 3));

-- query 4
-- @skip_result_check=true
USE ${case_db};
SET SESSION pipeline_dop = 1;

-- query 5
USE ${case_db};
select cast(percentile_approx(c2, 0.5) as int) from t1;

-- query 6
USE ${case_db};
select cast(percentile_approx(c2, 0.9) as int) from t1;

-- query 7
USE ${case_db};
select cast(percentile_approx(c2, 0.9, 2048) as int) from t1;

-- query 8
USE ${case_db};
select cast(percentile_approx(c2, 0.9, 5000) as int) from t1;

-- query 9
USE ${case_db};
select cast(percentile_approx(c2, 0.9, 10000) as int) from t1;

-- query 10
USE ${case_db};
with tt as (select (0.5) as v1, c1, c2 from t1) select  cast(percentile_approx(c2, v1) as int) from tt;

-- query 11
USE ${case_db};
with tt as (select (0.5) as v1, (4096) as v2, c1, c2 from t1) select  cast(percentile_approx(c2, v1, v2 + 1) as int) from tt;

-- query 12
USE ${case_db};
select  percentile_approx(c2, CAST([0.25, 0.5, 0.75] AS array<double>)) from t1;

-- query 13
USE ${case_db};
select  percentile_approx(c2, CAST([0.0, 0.5, 1.0] AS array<double>)) from t1;

-- query 14
USE ${case_db};
select  percentile_approx(c2, [0.1, 0.25, 0.5, 0.75, 0.9]) from t1;

-- query 15
USE ${case_db};
select  percentile_approx(c2, [0.25, 0.5, 0.75], 2048) from t1;

-- query 16
USE ${case_db};
select percentile_approx(c2, CAST([0.5, 0.9] AS array<double>), 5000) from t1;

-- query 17
USE ${case_db};
select percentile_approx(c2, [0.1, 0.5, 0.9], 10000) from t1;

-- query 18
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE "test_sorted_streaming_agg_percentile"
(
    "id_int" INTEGER NOT NULL,
    "value" double NOT NULL
) WITH (format_version = 3);

-- query 19
-- @skip_result_check=true
USE ${case_db};
insert into test_sorted_streaming_agg_percentile values ROW(2,1),ROW(2,6),ROW(4,3),ROW(4,8);

-- query 20
USE ${case_db};
select  percentile_approx(value, CAST([0.5, 0.9] AS array<double>), 5000) from test_sorted_streaming_agg_percentile group by id_int;

-- query 21
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE "t0_convert_to_serialize_format" (
  "v1" BIGINT,
  "v2" BIGINT,
  "v3" BIGINT
) WITH (format_version = 3);

-- query 22
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE "t1_convert_to_serialize_format" (
  "v4" BIGINT,
  "v5" BIGINT,
  "v6" BIGINT
) WITH (format_version = 3);

-- query 23
-- @skip_result_check=true
USE ${case_db};
insert into t0_convert_to_serialize_format values ROW(1,2,3),ROW(4,5,6),ROW(7,8,9);

-- query 24
-- @skip_result_check=true
USE ${case_db};
insert into t1_convert_to_serialize_format values ROW(1,2,3),ROW(4,5,6),ROW(7,8,9);

-- query 25
USE ${case_db};
select  percentile_approx(v2, CAST([0.1, 0.5, 0.9] AS array<double>), 5000) from t0_convert_to_serialize_format CROSS JOIN t1_convert_to_serialize_format group by 'a';
