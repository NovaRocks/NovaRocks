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

-- Migrated from dev/test/sql/test_agg/R/test_agg_with_limit_seq
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_agg_with_limit_seq @sequential
-- query 2
-- @skip_result_check=true
USE ${case_db};
create table base_table (
    c0 int,
    c1 int,
    c2 string,
    c3 int
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into base_table SELECT generate_series % 4, generate_series % 9, generate_series % 9, generate_series %9 FROM TABLE(generate_series(1,  10000));

-- query 4
-- @skip_result_check=true
USE ${case_db};
create table agg_with_limit_seq (
    c0 int,
    c1 int,
    c2 string,
    c3 int
)
TBLPROPERTIES ("format-version" = "3");

-- query 5
-- @skip_result_check=true
USE ${case_db};
insert into agg_with_limit_seq SELECT * FROM base_table;

-- query 6
-- @skip_result_check=true
USE ${case_db};

-- query 7
-- @skip_result_check=true
USE ${case_db};
set streaming_preaggregation_mode="force_streaming";

-- query 8
USE ${case_db};
select * from (select max(c3), sum(c3) sc3, c0 from agg_with_limit_seq group by c0 limit 10) t order by 3;

-- query 9
USE ${case_db};
select * from (select max(c3), sum(c3) sc3, c1 from agg_with_limit_seq group by c1 limit 10) t order by 3;

-- query 10
-- @skip_result_check=true
USE ${case_db};

-- Legacy cleanup step.
-- query 11
-- @skip_result_check=true
USE ${case_db};
