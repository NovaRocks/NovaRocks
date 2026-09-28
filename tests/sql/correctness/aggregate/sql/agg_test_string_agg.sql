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

-- Migrated from dev/test/sql/test_agg_function/R/test_string_agg
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained SQL test case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: testStringAgg
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE IF NOT EXISTS `lineorder` (
    `lo_orderkey` int(11) NOT NULL COMMENT "",
    `lo_shipmode` varchar(11) NOT NULL COMMENT ""
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
USE ${case_db};
SELECT STRING_AGG(lo_shipmode, ',') orgs FROM lineorder WHERE 1 = 2;

-- query 4
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `ss` (
  `id` int(11) NULL COMMENT "",
  `name` varchar(255) NULL COMMENT "",
  `subject` varchar(255) NULL COMMENT "",
  `score` int(11) NULL COMMENT ""
)
TBLPROPERTIES ("format-version" = "3");

-- query 5
-- @skip_result_check=true
USE ${case_db};
insert into ss values (1,"Tom","English",90);

-- query 6
-- @skip_result_check=true
USE ${case_db};
insert into ss values (1,"Tom","Math",80);

-- query 7
-- @skip_result_check=true
USE ${case_db};
insert into ss values (2,"Tom","English",NULL);

-- query 8
-- @skip_result_check=true
USE ${case_db};
insert into ss values (2,"Tom",NULL,NULL);

-- query 9
-- @skip_result_check=true
USE ${case_db};
insert into ss values (3,"May",NULL,NULL);

-- query 10
-- @skip_result_check=true
USE ${case_db};
insert into ss values (3,"Ti","English",98);

-- query 11
-- @skip_result_check=true
USE ${case_db};
insert into ss values (4,NULL,NULL,NULL);

-- query 12
-- @skip_result_check=true
USE ${case_db};
insert into ss values (NULL,NULL,NULL,NULL);

-- query 13
-- @skip_result_check=true
USE ${case_db};
insert into ss values (NULL,"Ti","物理Phy",99);

-- query 14
-- @skip_result_check=true
USE ${case_db};
insert into ss values (11,"张三此地无银三百两","英文English",98);

-- query 15
-- @skip_result_check=true
USE ${case_db};
insert into ss values (11,"张三掩耳盗铃","Math数学欧拉方程",78);

-- query 16
-- @skip_result_check=true
USE ${case_db};
insert into ss values (12,"李四大闹天空","英语外语美誉",NULL);

-- query 17
-- @skip_result_check=true
USE ${case_db};
insert into ss values (2,"王武程咬金","语文北京上海",22);

-- query 18
-- @skip_result_check=true
USE ${case_db};
insert into ss values (3,"欧阳诸葛方程","数学大不列颠",NULL);

-- query 19
-- @expect_error=ORDER BY position 2 is not in group_concat output list.
USE ${case_db};
select string_agg(distinct score, ',' order by 1,2) from ss group by id order by 1;

-- query 20
USE ${case_db};
select string_agg(distinct score, ',' order by 1,name) from ss group by id order by 1;

-- query 21
-- @expect_error=ORDER BY position 2 is not in group_concat output list.
USE ${case_db};
select string_agg(score, ',' order by 1,2) from ss group by id order by 1;

-- query 22
USE ${case_db};
select string_agg(score, ',' order by 1,name) from ss group by id order by 1;

-- query 23
USE ${case_db};
select string_agg(score, ',' order by 1,name) from ss order by 1;

-- query 24
USE ${case_db};
select string_agg(name, ',') from ss where id = 1;

-- query 25
USE ${case_db};
select string_agg(distinct name, ',') from ss where id = 1;

-- query 26
USE ${case_db};
select string_agg(concat(name, ':', score), ',' order by score desc) as ordered_scores from ss where id = 1;
