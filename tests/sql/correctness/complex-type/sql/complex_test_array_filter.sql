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

-- Migrated from dev/test/sql/test_array_fn/R/test_array_filter
-- Test Objective:
-- Preserve array test coverage migrated from dev/test.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_array_filter @mac
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `t` (
  `k` bigint(20) NOT NULL COMMENT "",
  `arr_0` array<bigint(20)> NOT NULL COMMENT "",
  `arr_1` array<bigint(20)> NULL COMMENT "",
  `arr_2` array<bigint(20)> NULL COMMENT ""
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into t values (1,[1,2],[1,2],[0,1]),(2,[1,2],null,[1,1]),(3,[1,2],[1,2],null),(4,[1,2],[null,null],[null,null]),(5,[1],[1,2],[0,0,1]);

-- query 4
USE ${case_db};
select array_filter(arr_0, arr_2) from t order by k;

-- query 5
USE ${case_db};
select array_filter(arr_1, arr_2) from t order by k;

-- query 6
USE ${case_db};
select array_filter(arr_0,[0,0,0,1]) from t order by k;

-- query 7
USE ${case_db};
select array_filter(arr_0,[1,0,1]) from t order by k;

-- query 8
USE ${case_db};
select array_filter(arr_0,null) from t order by k;

-- query 9
USE ${case_db};
select array_filter(arr_1,null) from t order by k;

-- query 10
USE ${case_db};
select array_filter([1,2,3,4],arr_2) from t order by k;

-- query 11
USE ${case_db};
select array_filter(null, arr_2) from t order by k;

-- query 12
USE ${case_db};
select array_filter([1,2,3,4],[0,0,1,1]) from t;

-- query 13
USE ${case_db};
select array_filter(null, null) from t;

-- query 14
USE ${case_db};
select array_filter([1,2,3,4],null) from t;

-- query 15
USE ${case_db};
select array_filter(null, [1,0,1,null]) from t;

-- query 16
-- @skip_result_check=true
-- A catalog that cannot hold views cannot answer view enumeration, so
-- DROP DATABASE ... FORCE is refused here rather than silently assuming
-- the namespace holds none. Drop the tables explicitly instead.
USE ${case_db};
DROP TABLE IF EXISTS t;
