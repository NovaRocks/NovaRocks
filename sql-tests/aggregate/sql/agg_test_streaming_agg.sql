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

-- Migrated from dev/test/sql/test_agg/R/test_streaming_agg
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_streaming_agg @sequential
-- query 2
-- @skip_result_check=true
USE ${case_db};
create table t0(
    c0 INT,
    c1 BIGINT
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into t0 values (1,1),(2,2),(3,3),(4,4),(5,5);

-- query 4
-- @skip_result_check=true
USE ${case_db};
set pipeline_dop=1;

-- query 5
-- @skip_result_check=true
USE ${case_db};
set new_planner_agg_stage=2;

-- query 6
USE ${case_db};
select c0, sum(c1) from t0 group by c0 order by c0;

-- query 7
-- @skip_result_check=true
USE ${case_db};

-- query 8
USE ${case_db};
select c0, sum(c1) from t0 group by c0 order by c0;

-- query 9
-- @skip_result_check=true
USE ${case_db};

-- query 10
-- @skip_result_check=true
USE ${case_db};
create table t1 (
    c0 INT,
    c1 BIGINT
)
TBLPROPERTIES ("format-version" = "3");

-- query 11
-- @skip_result_check=true
USE ${case_db};
insert into t1 SELECT generate_series, 4096 - generate_series FROM TABLE(generate_series(1,  4096)) union all select null,null;

-- query 12
USE ${case_db};
select c0, sum(c1) from t1 group by c0 order by 2 desc limit 10;

-- Legacy cleanup step.
-- query 13
-- @skip_result_check=true
USE ${case_db};
