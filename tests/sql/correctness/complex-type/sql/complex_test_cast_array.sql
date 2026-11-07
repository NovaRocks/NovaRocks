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

-- Migrated from dev/test/sql/test_array/R/test_cast_array
-- Test Objective:
-- Preserve array test coverage migrated from dev/test.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_cast_array
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `tbl` (k1 string,k2 string,k3 int)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into tbl values
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1),
('abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'ab', 1);

-- query 4

-- query 5
USE ${case_db};
select  element_at(array_agg(array_length((['abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq']))), 5) AS fifth_length from (select t1.k3 from tbl t1 join tbl t2 join tbl t3 join tbl t4) t;

-- query 6
-- @skip_result_check=true
USE ${case_db};
select  * from (select array_length((['abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq'])) array_len from (select t1.k3 from tbl t1 join tbl t2 join tbl t3 join tbl t4) t) t where array_len = 1;

-- query 7
USE ${case_db};
select  count(*) from (select (['abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq']) arr from (select t1.k3 from tbl t1 join tbl t2) t) t group by arr[1];

-- query 8
USE ${case_db};
select   count(*) from (select (['abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq']) arr from (select t1.k3 from tbl t1 join tbl t2) t) t group by arr[1];

-- query 9
USE ${case_db};
select  count(*), array_length(array_agg(arr)) from (select (['abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq', 'abcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopqabcdefghijklmnopq']) arr from (select t1.k3 from tbl t1) t) t group by arr[1];

-- query 10
USE ${case_db};
select array_length(array_agg(arr)) from (select cast("[[1, 2, 3], [1, 2, 3]]" as array<array<int>>) arr from (select t1.k3 from tbl t1 join tbl t2) t)  t;

-- query 11
USE ${case_db};
select array_agg(arr)[1] from (select cast("[[1, 2, 3], [1, 2, 3]]" as array<array<int>>) arr from (select t1.k3 from tbl t1 join tbl t2) t)  t;

-- query 12
-- @skip_result_check=true
-- A catalog that cannot hold views cannot answer view enumeration, so
-- DROP DATABASE ... FORCE is refused here rather than silently assuming
-- the namespace holds none. Drop the tables explicitly instead.
USE ${case_db};
DROP TABLE IF EXISTS tbl;
