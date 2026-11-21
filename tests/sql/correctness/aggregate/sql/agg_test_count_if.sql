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

-- Migrated from dev/test/sql/test_agg_function/R/test_count_if
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained SQL test case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_count_if
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE "test_count_if" (
  "v1" varchar(65533),
  "v2" varchar(65533),
  "v3" datetime,
  "v4" int
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into test_count_if values ROW('a','a', '2022-04-18 01:01:00', 1);

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into test_count_if values ROW('a','b', '2022-04-18 02:01:00', NULL);

-- query 5
-- @skip_result_check=true
USE ${case_db};
insert into test_count_if values ROW('a',NULL, '2022-04-18 02:05:00', 1);

-- query 6
-- @skip_result_check=true
USE ${case_db};
insert into test_count_if values ROW('a','b', '2022-04-18 02:15:00', 3);

-- query 7
-- @skip_result_check=true
USE ${case_db};
insert into test_count_if values ROW('a','b', '2022-04-18 03:15:00', 7);

-- query 8
-- @skip_result_check=true
USE ${case_db};
insert into test_count_if values ROW('c',NULL, '2022-04-18 03:45:00', NULL);

-- query 9
-- @skip_result_check=true
USE ${case_db};
insert into test_count_if values ROW('c',NULL, '2022-04-18 03:25:00', 2);

-- query 10
-- @skip_result_check=true
USE ${case_db};
insert into test_count_if values ROW('c','a', '2022-04-18 03:27:00', 3);

-- query 11
USE ${case_db};
select v1, count_if(v2 is not null) from test_count_if group by v1;

-- query 12
USE ${case_db};
select v1, count_if(v4 + v4 is not null) from test_count_if group by v1;

-- query 13
USE ${case_db};
select v1, count_if(v2 is null), count_if(v4) from test_count_if group by v1;

-- query 14
USE ${case_db};
select count_if(v4 >= 3), count_if(v4 < 3),count_if(v3 = TIMESTAMP '2022-04-18 03:45:00') from test_count_if;

-- query 15
USE ${case_db};
select count_if(v2), count_if(v2 is not null), count_if(null), count_if(v4+1) from test_count_if;

-- query 16
-- @expect_error=Unexpected input '(', the most similar input is {<EOF>, ';'}.
USE ${case_db};
select count_if(DISTINCT v2) from test_count_if;

-- query 17
USE ${case_db};
select count_if(v1 >= v2), count_if(v1 >= v2 or v4 = 1), count_if(v1 >= v2 and v4 = 1) from test_count_if;

-- query 18
USE ${case_db};
select count_if(true), count_if(false), count_if('') from test_count_if;

-- query 19
USE ${case_db};
select count_if(v1 = 'a'), count(v1), count(if(v1 = 'a', 1, null)) from test_count_if;
