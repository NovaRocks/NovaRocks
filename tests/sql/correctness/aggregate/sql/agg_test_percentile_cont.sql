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

-- Migrated from dev/test/sql/test_agg_function/R/test_percentile_cont
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained SQL test case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_percentile_cont
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE "test_pc" (
  "date" date,
  "datetime" datetime,
  "db" double,
  "id" int(11),
  "name" varchar(255),
  "subject" varchar(255),
  "score" int(11)
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into test_pc values ROW('2018-01-01','2018-01-01 00:00:01',11.1,1,'Tom','English',90);

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into test_pc values ROW('2019-01-01','2019-01-01 00:00:01',11.2,1,'Tom','English',91);

-- query 5
-- @skip_result_check=true
USE ${case_db};
insert into test_pc values ROW('2020-01-01','2020-01-01 00:00:01',11.3,1,'Tom','English',92);

-- query 6
-- @skip_result_check=true
USE ${case_db};
insert into test_pc values ROW('2021-01-01','2021-01-01 00:00:01',11.4,1,'Tom','English',93);

-- query 7
-- @skip_result_check=true
USE ${case_db};
insert into test_pc values ROW('2022-01-01','2022-01-01 00:00:01',11.5,1,'Tom','English',94);

-- query 8
-- @skip_result_check=true
USE ${case_db};
insert into test_pc values ROW(NULL,NULL,NULL,NULL,'Tom','English',NULL);

-- query 9
USE ${case_db};
select percentile_cont(score, 0) from test_pc;

-- query 10
USE ${case_db};
select percentile_cont(score, 0.25) from test_pc;

-- query 11
USE ${case_db};
select percentile_cont(score, 0.5) from test_pc;

-- query 12
USE ${case_db};
select percentile_cont(score, 0.75) from test_pc;

-- query 13
USE ${case_db};
select percentile_cont(score, 1) from test_pc;

-- query 14
USE ${case_db};
select percentile_cont(date, 0) from test_pc;

-- query 15
USE ${case_db};
select percentile_cont(date, 0.25) from test_pc;

-- query 16
USE ${case_db};
select percentile_cont(date, 0.5) from test_pc;

-- query 17
USE ${case_db};
select percentile_cont(date, 0.75) from test_pc;

-- query 18
USE ${case_db};
select percentile_cont(date, 1) from test_pc;

-- query 19
USE ${case_db};
select percentile_cont(datetime, 0) from test_pc;

-- query 20
USE ${case_db};
select percentile_cont(datetime, 0.25) from test_pc;

-- query 21
USE ${case_db};
select percentile_cont(datetime, 0.5) from test_pc;

-- query 22
USE ${case_db};
select percentile_cont(datetime, 0.75) from test_pc;

-- query 23
USE ${case_db};
select percentile_cont(datetime, 1) from test_pc;

-- query 24
USE ${case_db};
select percentile_cont(db, 0) from test_pc;

-- query 25
USE ${case_db};
select percentile_cont(db, 0.25) from test_pc;

-- query 26
USE ${case_db};
select percentile_cont(db, 0.5) from test_pc;

-- query 27
USE ${case_db};
select percentile_cont(db, 0.75) from test_pc;

-- query 28
USE ${case_db};
select percentile_cont(db, 1) from test_pc;

-- query 29
-- @expect_error=between 0 and 1
USE ${case_db};
select percentile_cont(db, 2) from test_pc;

-- query 30
-- @expect_error=between 0 and 1
USE ${case_db};
select percentile_cont(db, -1) from test_pc;

-- query 31
USE ${case_db};
select percentile_cont(db, cast(1.0 as double)) from test_pc;

-- query 32
USE ${case_db};
select percentile_cont(db, cast(0.5 as double)) from test_pc;

-- query 33
-- @expect_error=between 0 and 1
USE ${case_db};
select percentile_cont(db, cast(1.5 as double)) from test_pc;

-- query 34
USE ${case_db};
select percentile_cont(1, cast(1.0 as double));

-- query 35
USE ${case_db};
select percentile_cont(1, cast(0.5 as double));

-- query 36
-- @expect_error=between 0 and 1
USE ${case_db};
select percentile_cont(1, cast(1.5 as double));
