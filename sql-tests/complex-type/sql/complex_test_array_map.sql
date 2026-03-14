-- Migrated from dev/test/sql/test_array/R/test_array_map
-- Test Objective:
-- Preserve array test coverage migrated from dev/test.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_complex_test_array_map FORCE;
CREATE DATABASE sql_tests_complex_test_array_map;
USE sql_tests_complex_test_array_map;

-- name: test_array_map_1
-- query 2
-- @skip_result_check=true
USE sql_tests_complex_test_array_map;
CREATE TABLE t1 (
    k1 bigint,
    c1 array < varchar(65536) > 
) ENGINE = OLAP 
DUPLICATE KEY(k1) PROPERTIES (
    "replication_num" = "1"
);

-- query 3
-- @skip_result_check=true
USE sql_tests_complex_test_array_map;
CREATE TABLE t2 (
    k1 bigint,
    c1 bigint
) ENGINE = OLAP 
DUPLICATE KEY(k1) PROPERTIES (
    "replication_num" = "1"
);

-- query 4
-- @skip_result_check=true
USE sql_tests_complex_test_array_map;
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
USE sql_tests_complex_test_array_map;
insert into t2
values
    (1, 1),
    (2, 1),
    (3, 3),
    (4, 5);

-- query 6
USE sql_tests_complex_test_array_map;
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
USE sql_tests_complex_test_array_map;
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
USE sql_tests_complex_test_array_map;
CREATE TABLE table1 (
    id INT,
    arr_largeint ARRAY<INT> NOT NULL
)PROPERTIES ("replication_num" = "1");

-- query 9
-- @skip_result_check=true
USE sql_tests_complex_test_array_map;
INSERT INTO table1 (id, arr_largeint) VALUES
(1, [1, 2]),
(2, [3, 4, 5]),
(3, [6]);

-- query 10
-- @skip_result_check=true
USE sql_tests_complex_test_array_map;
CREATE TABLE table2 (
    id INT,
    arr_str ARRAY<INT> NOT NULL
) PROPERTIES ("replication_num" = "1");

-- query 11
-- @skip_result_check=true
USE sql_tests_complex_test_array_map;
INSERT INTO table2 (id, arr_str) VALUES
(1, [1, 2, 3]),
(2, [4, 5]),
(3, [6, 7, 8, 9]);

-- query 12
USE sql_tests_complex_test_array_map;
SELECT t1.id AS t1_id, t2.id AS t2_id, t1.arr_largeint
FROM table1 t1
LEFT JOIN[broadcast] table2 t2
ON t1.id = t2.id
AND array_length(array_map(x -> x + array_length(t2.arr_str), t1.arr_largeint)) >= 2;

-- query 13
USE sql_tests_complex_test_array_map;
WITH `CTE` AS (
    SELECT TRUE AS bool_1, TRUE AS bool_2, TRUE AS bool_3, ["a"] AS arr
    UNION ALL
    SELECT TRUE AS bool_1, TRUE AS bool_2, TRUE AS bool_3, ["a"] AS arr
) SELECT ARRAY_MAP((arg)->`bool_1` AND `bool_2` AND `bool_3`, arr), ARRAY_MAP((arg)->`bool_1` AND `bool_3` AND `bool_2`, arr) FROM `CTE`;

-- query 14
USE sql_tests_complex_test_array_map;
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
DROP DATABASE IF EXISTS sql_tests_complex_test_array_map FORCE;
