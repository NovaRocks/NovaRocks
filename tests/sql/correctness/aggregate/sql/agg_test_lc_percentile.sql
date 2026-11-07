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

-- Migrated from dev/test/sql/test_agg_function/R/test_lc_percentile
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained SQL test case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_lc_percentile
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `test_pc` (
  `date` date NULL COMMENT "",
  `datetime` datetime NULL COMMENT "",
  `db` double NULL COMMENT "",
  `id` int(11) NULL COMMENT "",
  `name` varchar(255) NULL COMMENT "",
  `subject` varchar(255) NULL COMMENT "",
  `score` int(11) NULL COMMENT ""
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into test_pc values ("2018-01-01","2018-01-01 00:00:01",11.1,1,"Tom","English",90);

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into test_pc values ("2019-01-01","2019-01-01 00:00:01",11.2,1,"Tom","English",91);

-- query 5
-- @skip_result_check=true
USE ${case_db};
insert into test_pc values ("2020-01-01","2020-01-01 00:00:01",11.3,1,"Tom","English",92);

-- query 6
-- @skip_result_check=true
USE ${case_db};
insert into test_pc values ("2021-01-01","2021-01-01 00:00:01",11.4,1,"Tom","English",93);

-- query 7
-- @skip_result_check=true
USE ${case_db};
insert into test_pc values ("2022-01-01","2022-01-01 00:00:01",11.5,1,"Tom","English",94);

-- query 8
-- @skip_result_check=true
USE ${case_db};
insert into test_pc values (NULL,NULL,NULL,NULL,"Tom","English",NULL);

-- query 9
USE ${case_db};
select percentile_disc_lc(score, 0) from test_pc;

-- query 10
USE ${case_db};
select percentile_disc_lc(score, 0.25) from test_pc;

-- query 11
USE ${case_db};
select percentile_disc_lc(score, 0.5) from test_pc;

-- query 12
USE ${case_db};
select percentile_disc_lc(score, 0.75) from test_pc;

-- query 13
USE ${case_db};
select percentile_disc_lc(score, 1) from test_pc;

-- query 14
USE ${case_db};
select percentile_disc_lc(date, 0) from test_pc;

-- query 15
USE ${case_db};
select percentile_disc_lc(date, 0.25) from test_pc;

-- query 16
USE ${case_db};
select percentile_disc_lc(date, 0.5) from test_pc;

-- query 17
USE ${case_db};
select percentile_disc_lc(date, 0.75) from test_pc;

-- query 18
USE ${case_db};
select percentile_disc_lc(date, 1) from test_pc;

-- query 19
USE ${case_db};
select percentile_disc_lc(datetime, 0) from test_pc;

-- query 20
USE ${case_db};
select percentile_disc_lc(datetime, 0.25) from test_pc;

-- query 21
USE ${case_db};
select percentile_disc_lc(datetime, 0.5) from test_pc;

-- query 22
USE ${case_db};
select percentile_disc_lc(datetime, 0.75) from test_pc;

-- query 23
USE ${case_db};
select percentile_disc_lc(datetime, 1) from test_pc;

-- query 24
USE ${case_db};
select percentile_disc_lc(db, 0) from test_pc;

-- query 25
USE ${case_db};
select percentile_disc_lc(db, 0.25) from test_pc;

-- query 26
USE ${case_db};
select percentile_disc_lc(db, 0.5) from test_pc;

-- query 27
USE ${case_db};
select percentile_disc_lc(db, 0.75) from test_pc;

-- query 28
USE ${case_db};
select percentile_disc_lc(db, 1) from test_pc;

-- query 29
-- @skip_result_check=true
USE ${case_db};

-- query 30
-- @skip_result_check=true
USE ${case_db};

-- query 31
USE ${case_db};
select `date`, percentile_disc_lc(score, 0) from test_pc group by `date` order by 1,2;

-- query 32
-- @skip_result_check=true
USE ${case_db};

-- query 33
USE ${case_db};
select `date`, percentile_disc_lc(score, 0) from test_pc group by `date` order by 1,2;

-- query 34
-- @expect_error=between 0 and 1
USE ${case_db};
select percentile_disc_lc(score, 1.1) from test_pc;
