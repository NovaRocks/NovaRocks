-- Migrated from dev/test/sql/test_agg_function/R/test_bitmap_agg
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_test_bitmap_agg FORCE;
CREATE DATABASE sql_tests_test_bitmap_agg;
USE sql_tests_test_bitmap_agg;

-- name: test_bitmap_agg
-- query 2
-- @skip_result_check=true
USE sql_tests_test_bitmap_agg;
CREATE TABLE t1 (
    c1 int,
    c2 boolean,
    c3 tinyint,
    c4 int,
    c5 bigint,
    c6 largeint,
    c7 string
    )
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 3
-- @skip_result_check=true
USE sql_tests_test_bitmap_agg;
INSERT INTO t1 values
    (1, true, 11, 111, 1111, 11111, "111111"),
    (2, false, 22, 222, 2222, 22222, "222222"),
    (3, true, 33, 333, 3333, 33333, "333333"),
    (4, null, null, null, null, null, null),
    (5, -1, -11, -111, -1111, -11111, "-111111"),
    (6, null, null, null, null, "36893488147419103232", "680564733841876926926749214863536422912");

-- query 4
USE sql_tests_test_bitmap_agg;
SELECT BITMAP_TO_STRING(BITMAP_AGG(c2)) FROM t1;

-- query 5
USE sql_tests_test_bitmap_agg;
SELECT BITMAP_TO_STRING(BITMAP_AGG(c3)) FROM t1;

-- query 6
USE sql_tests_test_bitmap_agg;
SELECT BITMAP_TO_STRING(BITMAP_AGG(c4)) FROM t1;

-- query 7
USE sql_tests_test_bitmap_agg;
SELECT BITMAP_TO_STRING(BITMAP_AGG(c5)) FROM t1;

-- query 8
USE sql_tests_test_bitmap_agg;
SELECT BITMAP_TO_STRING(BITMAP_AGG(c6)) FROM t1;

-- query 9
USE sql_tests_test_bitmap_agg;
SELECT BITMAP_TO_STRING(BITMAP_AGG(c7)) FROM t1;

-- query 10
USE sql_tests_test_bitmap_agg;
SELECT BITMAP_TO_STRING(BITMAP_UNION(TO_BITMAP(c2))) FROM t1;

-- query 11
USE sql_tests_test_bitmap_agg;
SELECT BITMAP_TO_STRING(BITMAP_UNION(TO_BITMAP(c3))) FROM t1;

-- query 12
USE sql_tests_test_bitmap_agg;
SELECT BITMAP_TO_STRING(BITMAP_UNION(TO_BITMAP(c4))) FROM t1;

-- query 13
USE sql_tests_test_bitmap_agg;
SELECT BITMAP_TO_STRING(BITMAP_UNION(TO_BITMAP(c5))) FROM t1;

-- query 14
USE sql_tests_test_bitmap_agg;
SELECT BITMAP_TO_STRING(BITMAP_UNION(TO_BITMAP(c6))) FROM t1;

-- query 15
-- @skip_result_check=true
USE sql_tests_test_bitmap_agg;
create materialized view mv1 as select c1, bitmap_agg(c2), bitmap_agg(c3), bitmap_agg(c4) from t1 group by c1;

-- query 16
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=FINISHED
-- @skip_result_check=true
USE sql_tests_test_bitmap_agg;
SHOW ALTER MATERIALIZED VIEW ORDER BY CreateTime DESC LIMIT 1;

-- query 17
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=mv1
-- @skip_result_check=true
USE sql_tests_test_bitmap_agg;
EXPLAIN select bitmap_agg(c2), bitmap_agg(c3), bitmap_agg(c4) from t1 group by c1;

-- query 18
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=mv1
-- @skip_result_check=true
USE sql_tests_test_bitmap_agg;
EXPLAIN select bitmap_union(to_bitmap(c2)), bitmap_union(to_bitmap(c3)), bitmap_agg(c4) from t1 group by c1;

-- query 19
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=mv1
-- @skip_result_check=true
USE sql_tests_test_bitmap_agg;
EXPLAIN select bitmap_union(to_bitmap(c2)), bitmap_union(to_bitmap(c3)), bitmap_agg(c4) from t1 group by c1;

-- query 20
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=mv1
-- @skip_result_check=true
USE sql_tests_test_bitmap_agg;
EXPLAIN select count(distinct c2), bitmap_union(to_bitmap(c3)), bitmap_agg(c4) from t1 group by c1;

-- query 21
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=mv1
-- @skip_result_check=true
USE sql_tests_test_bitmap_agg;
EXPLAIN SELECT BITMAP_TO_STRING(BITMAP_UNION(TO_BITMAP(c2))) FROM t1;

-- query 22
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=mv1
-- @skip_result_check=true
USE sql_tests_test_bitmap_agg;
EXPLAIN select bitmap_union_count(to_bitmap(c4)), bitmap_agg(c4) from t1 group by c1;

-- query 23
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=mv1
-- @skip_result_check=true
USE sql_tests_test_bitmap_agg;
EXPLAIN select c1, count(distinct c2), count(distinct c3), count(distinct c4) from t1 group by c1;

-- query 24
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=mv1
-- @skip_result_check=true
USE sql_tests_test_bitmap_agg;
EXPLAIN select c1, multi_distinct_count(c3), multi_distinct_count(c4) from t1 group by c1;

-- query 25
USE sql_tests_test_bitmap_agg;
SELECT BITMAP_TO_STRING(BITMAP_UNION(TO_BITMAP(c2))) FROM t1;

-- query 26
USE sql_tests_test_bitmap_agg;
SELECT BITMAP_TO_STRING(BITMAP_UNION(TO_BITMAP(c3))) FROM t1;

-- query 27
USE sql_tests_test_bitmap_agg;
SELECT BITMAP_TO_STRING(BITMAP_UNION(TO_BITMAP(c4))) FROM t1;

-- query 28
USE sql_tests_test_bitmap_agg;
select c1, BITMAP_TO_STRING(bitmap_union(to_bitmap(c2))), BITMAP_TO_STRING(bitmap_union(to_bitmap(c3))), BITMAP_TO_STRING(bitmap_agg(c4)) from t1 group by c1 order by c1;

-- query 29
USE sql_tests_test_bitmap_agg;
select c1, count(distinct c2), bitmap_union(to_bitmap(c3)), bitmap_agg(c4) from t1 group by c1 order by c1;

-- query 30
USE sql_tests_test_bitmap_agg;
select c1, bitmap_union_count(to_bitmap(c4)), BITMAP_TO_STRING(bitmap_agg(c4)) from t1 group by c1 order by c1;

-- query 31
USE sql_tests_test_bitmap_agg;
select c1, multi_distinct_count(c2), multi_distinct_count(c3), multi_distinct_count(c4) from t1 group by c1 order by c1;

-- query 32
USE sql_tests_test_bitmap_agg;
select c1, count(distinct c2), count(distinct c3), count(distinct c4) from t1 group by c1 order by c1;

-- query 33
-- @skip_result_check=true
USE sql_tests_test_bitmap_agg;
drop materialized view mv1;

-- query 34
-- @skip_result_check=true
USE sql_tests_test_bitmap_agg;
create materialized view mv2
distributed by random
refresh deferred manual
as select c1, bitmap_agg(c2), bitmap_agg(c3), bitmap_agg(c4) from t1 group by c1;

-- query 35
-- @skip_result_check=true
USE sql_tests_test_bitmap_agg;
refresh materialized view mv2 with sync mode;

-- query 36
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=mv2
-- @skip_result_check=true
USE sql_tests_test_bitmap_agg;
EXPLAIN select bitmap_agg(c2), bitmap_agg(c3), bitmap_agg(c4) from t1 group by c1;

-- query 37
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=mv2
-- @skip_result_check=true
USE sql_tests_test_bitmap_agg;
EXPLAIN select bitmap_union(to_bitmap(c2)), bitmap_union(to_bitmap(c3)), bitmap_agg(c4) from t1 group by c1;

-- query 38
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=mv2
-- @skip_result_check=true
USE sql_tests_test_bitmap_agg;
EXPLAIN select bitmap_union(to_bitmap(c2)), bitmap_union(to_bitmap(c3)), bitmap_agg(c4) from t1 group by c1;

-- query 39
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=mv2
-- @skip_result_check=true
USE sql_tests_test_bitmap_agg;
EXPLAIN select count(distinct c2), bitmap_union(to_bitmap(c3)), bitmap_agg(c4) from t1 group by c1;

-- query 40
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=mv2
-- @skip_result_check=true
USE sql_tests_test_bitmap_agg;
EXPLAIN SELECT BITMAP_TO_STRING(BITMAP_UNION(TO_BITMAP(c2))) FROM t1;

-- query 41
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=mv2
-- @skip_result_check=true
USE sql_tests_test_bitmap_agg;
EXPLAIN select bitmap_union_count(to_bitmap(c4)), bitmap_agg(c4) from t1 group by c1;

-- query 42
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=mv2
-- @skip_result_check=true
USE sql_tests_test_bitmap_agg;
EXPLAIN select c1, multi_distinct_count(c3), multi_distinct_count(c4) from t1 group by c1;

-- query 43
USE sql_tests_test_bitmap_agg;
SELECT BITMAP_TO_STRING(BITMAP_UNION(TO_BITMAP(c2))) FROM t1;

-- query 44
USE sql_tests_test_bitmap_agg;
SELECT BITMAP_TO_STRING(BITMAP_UNION(TO_BITMAP(c3))) FROM t1;

-- query 45
USE sql_tests_test_bitmap_agg;
SELECT BITMAP_TO_STRING(BITMAP_UNION(TO_BITMAP(c4))) FROM t1;

-- query 46
USE sql_tests_test_bitmap_agg;
select ifnull(nullif(BITMAP_TO_STRING(bitmap_union(to_bitmap(c2))), ''), 'NULL'),
       ifnull(nullif(BITMAP_TO_STRING(bitmap_union(to_bitmap(c3))), ''), 'NULL'),
       ifnull(nullif(BITMAP_TO_STRING(bitmap_agg(c4)), ''), 'NULL')
from t1
group by c1
order by c1;

-- query 47
USE sql_tests_test_bitmap_agg;
select c1, count(distinct c2), bitmap_union(to_bitmap(c3)), bitmap_agg(c4) from t1 group by c1 order by c1;

-- query 48
USE sql_tests_test_bitmap_agg;
select c1, bitmap_union_count(to_bitmap(c4)), BITMAP_TO_STRING(bitmap_agg(c4)) from t1 group by c1 order by c1;

-- query 49
-- @skip_result_check=true
USE sql_tests_test_bitmap_agg;
drop materialized view mv2;
