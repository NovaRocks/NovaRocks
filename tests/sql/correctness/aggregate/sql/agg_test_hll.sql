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

-- Migrated from dev/test/sql/test_agg_function/R/test_hll
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained SQL test case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_hll_function
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1 (
    c1 int,
    c2 int
    ) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into t1 select generate_series, generate_series from table(sequence(1, 1000)) AS generate_series(generate_series);

-- query 4
-- @skip_result_check=true
USE ${case_db};
SET SESSION pipeline_dop = 1;

-- query 5
USE ${case_db};
select ndv(c1) from t1;

-- query 6
USE ${case_db};
select c2, ndv(c1) from t1 group by c2 order by c2 limit 10;

-- query 7
USE ${case_db};
select approx_count_distinct(c1) from t1;

-- query 8
USE ${case_db};
select c2,approx_count_distinct(c1) from t1 group by c2 order by c2 limit 10;
