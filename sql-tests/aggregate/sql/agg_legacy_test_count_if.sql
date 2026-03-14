-- Migrated from dev/test/sql/test_agg_function/R/test_count_if
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_test_count_if FORCE;
CREATE DATABASE sql_tests_test_count_if;
USE sql_tests_test_count_if;

-- name: test_count_if
-- query 2
-- @skip_result_check=true
USE sql_tests_test_count_if;
CREATE TABLE `test_count_if` (
  `v1` varchar(65533) NULL COMMENT "",
  `v2` varchar(65533) NULL COMMENT "",
  `v3` datetime NULL COMMENT "",
  `v4` int null
) ENGINE=OLAP
DUPLICATE KEY(v1, v2, v3)
PARTITION BY RANGE(`v3`)
(PARTITION p20220418 VALUES [("2022-04-18 00:00:00"), ("2022-04-19 00:00:00")),
PARTITION p20220419 VALUES [("2022-04-19 00:00:00"), ("2022-04-20 00:00:00")),
PARTITION p20220420 VALUES [("2022-04-20 00:00:00"), ("2022-04-21 00:00:00")),
PARTITION p20220421 VALUES [("2022-04-21 00:00:00"), ("2022-04-22 00:00:00")))
DISTRIBUTED BY HASH(`v1`) BUCKETS 4
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);

-- query 3
-- @skip_result_check=true
USE sql_tests_test_count_if;
insert into test_count_if values('a','a', '2022-04-18 01:01:00', 1);

-- query 4
-- @skip_result_check=true
USE sql_tests_test_count_if;
insert into test_count_if values('a','b', '2022-04-18 02:01:00', NULL);

-- query 5
-- @skip_result_check=true
USE sql_tests_test_count_if;
insert into test_count_if values('a',NULL, '2022-04-18 02:05:00', 1);

-- query 6
-- @skip_result_check=true
USE sql_tests_test_count_if;
insert into test_count_if values('a','b', '2022-04-18 02:15:00', 3);

-- query 7
-- @skip_result_check=true
USE sql_tests_test_count_if;
insert into test_count_if values('a','b', '2022-04-18 03:15:00', 7);

-- query 8
-- @skip_result_check=true
USE sql_tests_test_count_if;
insert into test_count_if values('c',NULL, '2022-04-18 03:45:00', NULL);

-- query 9
-- @skip_result_check=true
USE sql_tests_test_count_if;
insert into test_count_if values('c',NULL, '2022-04-18 03:25:00', 2);

-- query 10
-- @skip_result_check=true
USE sql_tests_test_count_if;
insert into test_count_if values('c','a', '2022-04-18 03:27:00', 3);

-- query 11
USE sql_tests_test_count_if;
select v1, count_if(v2 is not null) from test_count_if group by v1;

-- query 12
USE sql_tests_test_count_if;
select v1, count_if(v4 + v4 is not null) from test_count_if group by v1;

-- query 13
USE sql_tests_test_count_if;
select v1, count_if(v2 is null), count_if(v4) from test_count_if group by v1;

-- query 14
USE sql_tests_test_count_if;
select count_if(v4 >= 3), count_if(v4 < 3),count_if(v3 = '2022-04-18 03:45:00') from test_count_if;

-- query 15
USE sql_tests_test_count_if;
select count_if(v2), count_if(v2 is not null), count_if(null), count_if(v4+1) from test_count_if;

-- query 16
-- @expect_error=Unexpected input '(', the most similar input is {<EOF>, ';'}.
USE sql_tests_test_count_if;
select count_if(DISTINCT v2) from test_count_if;

-- query 17
USE sql_tests_test_count_if;
select count_if(v1 >= v2), count_if(v1 >= v2 or v4 = 1), count_if(v1 >= v2 and v4 = 1) from test_count_if;

-- query 18
USE sql_tests_test_count_if;
select count_if(true), count_if(false), count_if('') from test_count_if;

-- query 19
USE sql_tests_test_count_if;
select count_if(v1 = 'a'), count(v1), count(if(v1 = 'a', 1, null)) from test_count_if;
