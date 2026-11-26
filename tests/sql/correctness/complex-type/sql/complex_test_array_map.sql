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
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t2 (
    k1 bigint,
    c1 bigint
) WITH (format_version = 3);

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into t1
values
    ROW(1, ['1','2']        ),
    ROW(2, ['0','2','1']    ),
    ROW(3, ['0','2','1']    ),
    ROW(4, ['1','2']        ),
    ROW(5, ['0','2','1']    ),
    ROW(6, ['0','2','1','1']),
    ROW(7, ['0','2','1']    ),
    ROW(8, ['1','2']        ),
    ROW(9, ['L','2','1']    ),
    ROW(10, ['1','2']       );

-- query 5
-- @skip_result_check=true
USE ${case_db};
insert into t2
values
    ROW(1, 1),
    ROW(2, 1),
    ROW(3, 3),
    ROW(4, 5);

-- query 6
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 (k1, c1)
VALUES 
ROW(1, transform(sequence(1, 1000), x -> CAST(x AS VARCHAR))),
ROW(2, transform(sequence(1, 1000), x -> CAST(x AS VARCHAR))),
ROW(3, transform(sequence(1, 1000), x -> CAST(x AS VARCHAR))),
ROW(4, transform(sequence(1, 1000), x -> CAST(x AS VARCHAR))),
ROW(5, transform(sequence(1, 1000), x -> CAST(x AS VARCHAR)));

-- query 7
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE table1 (
    id INT,
    arr_largeint ARRAY<INT> NOT NULL
) WITH (format_version = 3);

-- query 8
-- @skip_result_check=true
USE ${case_db};
INSERT INTO table1 (id, arr_largeint) VALUES
ROW(1, [1, 2]),
ROW(2, [3, 4, 5]),
ROW(3, [6]);

-- query 9
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE table2 (
    id INT,
    arr_str ARRAY<INT> NOT NULL
) WITH (format_version = 3);

-- query 10
-- @skip_result_check=true
USE ${case_db};
INSERT INTO table2 (id, arr_str) VALUES
ROW(1, [1, 2, 3]),
ROW(2, [4, 5]),
ROW(3, [6, 7, 8, 9]);

-- query 11
USE ${case_db};
SELECT t1.id AS t1_id, t2.id AS t2_id, t1.arr_largeint
FROM table1 t1
LEFT JOIN table2 t2
ON t1.id = t2.id
AND cardinality(transform(t1.arr_largeint, x -> x + cardinality(t2.arr_str))) >= 2;

-- query 12
USE ${case_db};
WITH "CTE" AS (
    SELECT TRUE AS bool_1, TRUE AS bool_2, TRUE AS bool_3, ['a'] AS arr
    UNION ALL
    SELECT TRUE AS bool_1, TRUE AS bool_2, TRUE AS bool_3, ['a'] AS arr
) SELECT transform(arr, (arg)->"bool_1" AND "bool_2" AND "bool_3"), transform(arr, (arg)->"bool_1" AND "bool_3" AND "bool_2") FROM "CTE";

-- query 13
USE ${case_db};
with t1 as (
 select json_parse('[{"open_id": "aaa", "num": 1},{"open_id": "bbb", "num": 2},{"open_id": "ccc", "num": 3}]') as price_list
),t2 as (
 select price_list,transform(cast(price_list as array<json>), x -> get_json_string(x,'$.open_id')) as  fields
 from t1  
)
select * from t2 
where  contains(fields,'bbb');

-- query 14
-- @skip_result_check=true
-- A catalog that cannot hold views cannot answer view enumeration, so
-- DROP DATABASE ... FORCE is refused here rather than silently assuming
-- the namespace holds none. Drop the tables explicitly instead.
USE ${case_db};
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS table1;
DROP TABLE IF EXISTS table2;
