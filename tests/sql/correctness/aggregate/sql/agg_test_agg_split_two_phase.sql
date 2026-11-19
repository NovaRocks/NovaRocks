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

-- Migrated from dev/test/sql/test_agg/R/test_agg_split_two_phase
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained SQL test case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_agg_split_two_phase @mac
-- query 2
-- @skip_result_check=true
USE ${case_db};
create table t0 (
    c0 STRING,
    c1 STRING
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into t0 SELECT generate_series, generate_series FROM TABLE(generate_series(1,  1500));

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into t0 SELECT generate_series, NULL FROM TABLE(generate_series(1,  1500));

-- query 5
USE ${case_db};
select c1 from t0 where c1 is null group by c1;

-- query 6
USE ${case_db};
select c1, count() from t0 where c1 is null group by c1;
