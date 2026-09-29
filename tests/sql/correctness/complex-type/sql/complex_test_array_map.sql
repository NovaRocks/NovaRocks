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

-- Migrated from dev/test/sql/test_array/R/test_array_map
-- Test Objective:
-- Preserve array test coverage migrated from dev/test.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_array_map_1
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1 (
    k1 bigint,
    c1 array < varchar(65536) > 
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t2 (
    k1 bigint,
    c1 bigint
)
TBLPROPERTIES ("format-version" = "3");

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into t1
values
    (1, ["1","2"]        ), 
    (2, ["0","2","1"]    ), 
    (3, ["0","2","1"]    ), 
    (4, ["1","2"]        ), 
    (5, ["0","2","1"]    ), 
    (6, ["0","2","1","1"]), 
    (7, ["0","2","1"]    ), 
    (8, ["1","2"]        ), 
    (9, ["L","2","1"]    ), 
    (10, ["1","2"]       );

-- query 5
-- @skip_result_check=true
USE ${case_db};
insert into t2
values
    (1, 1),
    (2, 1),
    (3, 3),
    (4, 5);

-- query 6
USE ${case_db};
with w1 as (
    select
        k1, c1, array_map (x -> true, c1) as c2
    from
        t1
)
select
    w1.*
from
    w1
    join [broadcast] t2 using(k1)
where
    array_sum(w1.c1) <= t2.c1
order by
    w1.k1;

-- query 7
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 (k1, c1)
VALUES 
(1, ARRAY_MAP(
    x -> CAST(x AS STRING), 
    ARRAY_GENERATE(1, 1000)
)),
(2, ARRAY_MAP(
    x -> CAST(x AS STRING), 
    ARRAY_GENERATE(1, 1000)
)),
(3, ARRAY_MAP(
    x -> CAST(x AS STRING), 
    ARRAY_GENERATE(1, 1000)
)),
(4, ARRAY_MAP(
    x -> CAST(x AS STRING), 
    ARRAY_GENERATE(1, 1000)
)),
(5, ARRAY_MAP(
    x -> CAST(x AS STRING), 
    ARRAY_GENERATE(1, 1000)
));

-- query 8
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE table1 (
    id INT,
    arr_largeint ARRAY<INT> NOT NULL
)
TBLPROPERTIES ("format-version" = "3");

-- query 9
-- @skip_result_check=true
USE ${case_db};
INSERT INTO table1 (id, arr_largeint) VALUES
(1, [1, 2]),
(2, [3, 4, 5]),
(3, [6]);

-- query 10
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE table2 (
    id INT,
    arr_str ARRAY<INT> NOT NULL
)
TBLPROPERTIES ("format-version" = "3");

-- query 11
-- @skip_result_check=true
USE ${case_db};
INSERT INTO table2 (id, arr_str) VALUES
(1, [1, 2, 3]),
(2, [4, 5]),
(3, [6, 7, 8, 9]);

-- query 12
USE ${case_db};
SELECT t1.id AS t1_id, t2.id AS t2_id, t1.arr_largeint
FROM table1 t1
LEFT JOIN[broadcast] table2 t2
ON t1.id = t2.id
AND array_length(array_map(x -> x + array_length(t2.arr_str), t1.arr_largeint)) >= 2;

-- query 13
USE ${case_db};
WITH `CTE` AS (
    SELECT TRUE AS bool_1, TRUE AS bool_2, TRUE AS bool_3, ["a"] AS arr
    UNION ALL
    SELECT TRUE AS bool_1, TRUE AS bool_2, TRUE AS bool_3, ["a"] AS arr
) SELECT ARRAY_MAP((arg)->`bool_1` AND `bool_2` AND `bool_3`, arr), ARRAY_MAP((arg)->`bool_1` AND `bool_3` AND `bool_2`, arr) FROM `CTE`;

-- query 14
USE ${case_db};
with t1 as (
 select parse_json('[{"open_id": "aaa", "num": 1},{"open_id": "bbb", "num": 2},{"open_id": "ccc", "num": 3}]') as price_list
),t2 as (
 select price_list,array_map(x -> get_json_string(x,'$.open_id'), cast(price_list as array<json>)  ) as  fields
 from t1  
)
select * from t2 
where  array_contains(fields,'bbb');

-- query 15
-- @skip_result_check=true
-- A catalog that cannot hold views cannot answer view enumeration, so
-- DROP DATABASE ... FORCE is refused here rather than silently assuming
-- the namespace holds none. Drop the tables explicitly instead.
USE ${case_db};
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS table1;
DROP TABLE IF EXISTS table2;
