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

-- Migrated from dev/test/sql/test_agg/R/test_empty_input
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained SQL test case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_empty_input @mac
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `t0` (
  `v1` bigint(20) COMMENT "",
  `v2` bigint(20) COMMENT "",
  `v3` bigint(20) COMMENT "",
  `v4` varchar COMMENT ""
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
USE ${case_db};
select percentile_disc(v1,0.5) from t0;

-- query 4
USE ${case_db};
select percentile_cont(v1,0.5) from t0;

-- query 5
USE ${case_db};
select percentile_disc_lc(v1,0.5) from t0;

-- query 6
USE ${case_db};
select max(v1),min(v1) from t0;

-- query 7
USE ${case_db};
select max(v1),count() from t0;

-- query 8
USE ${case_db};
select count(v1) from t0;

-- query 9
USE ${case_db};
select count() from t0;

-- query 10
USE ${case_db};
select count(distinct v1) from t0;

-- name: test_empty_input_not_null_string
-- query 11
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `tt0` (
  `c0` int(11) NULL COMMENT "",
  `c1` varchar(20) not NULL COMMENT "",
  `c2` varchar(200) not NULL COMMENT "",
  `c3` int(11) NULL COMMENT ""
)
TBLPROPERTIES ("format-version" = "3");

-- query 12
-- @skip_result_check=true
USE ${case_db};
insert into tt0 values (1,"test","test",1);

-- query 13
USE ${case_db};
select max(c2) from tt0 where 1=0;

-- query 14
USE ${case_db};
select min(c2) from tt0 where 1=0;
