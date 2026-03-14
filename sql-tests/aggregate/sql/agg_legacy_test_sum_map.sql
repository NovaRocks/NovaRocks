-- Migrated from dev/test/sql/test_agg_function/R/test_sum_map
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_test_sum_map FORCE;
CREATE DATABASE sql_tests_test_sum_map;
USE sql_tests_test_sum_map;

-- name: test_sum_map
-- query 2
-- @skip_result_check=true
USE sql_tests_test_sum_map;
CREATE TABLE t1 (
    c1 int,
    c2 map<int, int>,
    c3 map<varchar(10), bigint>,
    c4 map<int, double>
)
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 3
-- @skip_result_check=true
USE sql_tests_test_sum_map;
INSERT INTO t1 values
    (1, map{1:10, 2:20, 3:30}, map{"a":100, "b":200}, map{1:1.5, 2:2.5}),
    (2, map{1:5, 2:15, 4:40}, map{"a":50, "c":150}, map{1:0.5, 3:3.5}),
    (3, map{2:25, 3:35, 5:50}, map{"b":250, "c":350}, map{2:1.0, 3:2.0});

-- query 4
USE sql_tests_test_sum_map;
select sum_map(c2)[1], sum_map(c2)[2], sum_map(c2)[3], sum_map(c2)[4], sum_map(c2)[5] from t1;

-- query 5
USE sql_tests_test_sum_map;
select sum_map(c3)["a"], sum_map(c3)["b"], sum_map(c3)["c"] from t1;

-- query 6
USE sql_tests_test_sum_map;
select sum_map(c4)[1], sum_map(c4)[2], sum_map(c4)[3] from t1;

-- query 7
USE sql_tests_test_sum_map;
select c1, sum_map(c2)[1], sum_map(c2)[2], sum_map(c2)[3], sum_map(c2)[4], sum_map(c2)[5] from t1 group by c1 order by c1;

-- query 8
USE sql_tests_test_sum_map;
select sum_map(c2)[1], sum_map(c2)[2], sum_map(c2)[3], sum_map(c2)[4] from t1 where c1 <= 2;

-- query 9
-- @skip_result_check=true
USE sql_tests_test_sum_map;
INSERT INTO t1 values (4, null, null, null);

-- query 10
USE sql_tests_test_sum_map;
select sum_map(c2)[1], sum_map(c2)[2], sum_map(c2)[3], sum_map(c2)[4], sum_map(c2)[5] from t1;

-- query 11
-- @skip_result_check=true
USE sql_tests_test_sum_map;
INSERT INTO t1 values (5, map{}, map{}, map{});

-- query 12
USE sql_tests_test_sum_map;
select sum_map(c2)[1], sum_map(c2)[2], sum_map(c2)[3], sum_map(c2)[4], sum_map(c2)[5] from t1;

-- query 13
USE sql_tests_test_sum_map;
select sum_map(c2)[1] from t1;

-- query 14
USE sql_tests_test_sum_map;
select sum_map(c2)[2] from t1;

-- query 15
USE sql_tests_test_sum_map;
select sum_map(c2)[3] from t1;

-- query 16
USE sql_tests_test_sum_map;
select sum_map(c2)[4] from t1;

-- query 17
USE sql_tests_test_sum_map;
select sum_map(c2)[5] from t1;

-- query 18
-- @skip_result_check=true
USE sql_tests_test_sum_map;
set streaming_preaggregation_mode=force_streaming;

-- query 19
USE sql_tests_test_sum_map;
select sum_map(c2)[1], sum_map(c2)[2], sum_map(c2)[3], sum_map(c2)[4], sum_map(c2)[5] from t1;

-- query 20
USE sql_tests_test_sum_map;
select sum_map(c3)["a"], sum_map(c3)["b"], sum_map(c3)["c"] from t1;

-- query 21
USE sql_tests_test_sum_map;
select sum_map(c4)[1], sum_map(c4)[2], sum_map(c4)[3] from t1;

-- query 22
USE sql_tests_test_sum_map;
select c1, sum_map(c2)[1], sum_map(c2)[2], sum_map(c2)[3], sum_map(c2)[4], sum_map(c2)[5] from t1 group by c1 order by c1;

-- query 23
-- @skip_result_check=true
USE sql_tests_test_sum_map;
set streaming_preaggregation_mode=force_preaggregation;

-- query 24
USE sql_tests_test_sum_map;
select sum_map(c2)[1], sum_map(c2)[2], sum_map(c2)[3], sum_map(c2)[4], sum_map(c2)[5] from t1;

-- query 25
USE sql_tests_test_sum_map;
select sum_map(c3)["a"], sum_map(c3)["b"], sum_map(c3)["c"] from t1;

-- query 26
USE sql_tests_test_sum_map;
select sum_map(c4)[1], sum_map(c4)[2], sum_map(c4)[3] from t1;

-- query 27
USE sql_tests_test_sum_map;
select c1, sum_map(c2)[1], sum_map(c2)[2], sum_map(c2)[3], sum_map(c2)[4], sum_map(c2)[5] from t1 group by c1 order by c1;

-- query 28
-- @skip_result_check=true
USE sql_tests_test_sum_map;
set streaming_preaggregation_mode=auto;

-- query 29
-- @skip_result_check=true
USE sql_tests_test_sum_map;
CREATE TABLE t2 (
    id int,
    m_tinyint map<int, tinyint>,
    m_smallint map<int, smallint>,
    m_int map<int, int>,
    m_bigint map<int, bigint>,
    m_largeint map<int, largeint>,
    m_float map<int, float>,
    m_double map<int, double>
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 30
-- @skip_result_check=true
USE sql_tests_test_sum_map;
INSERT INTO t2 values
    (1, map{1:10, 2:20}, map{1:100, 2:200}, map{1:1000, 2:2000}, map{1:10000, 2:20000},
        map{1:100000, 2:200000}, map{1:1.5, 2:2.5}, map{1:10.5, 2:20.5}),
    (2, map{1:5, 3:30}, map{1:50, 3:300}, map{1:500, 3:3000}, map{1:5000, 3:30000},
        map{1:50000, 3:300000}, map{1:0.5, 3:3.5}, map{1:5.5, 3:30.5});

-- query 31
USE sql_tests_test_sum_map;
select sum_map(m_tinyint)[1], sum_map(m_tinyint)[2], sum_map(m_tinyint)[3] from t2;

-- query 32
USE sql_tests_test_sum_map;
select sum_map(m_smallint)[1], sum_map(m_smallint)[2], sum_map(m_smallint)[3] from t2;

-- query 33
USE sql_tests_test_sum_map;
select sum_map(m_int)[1], sum_map(m_int)[2], sum_map(m_int)[3] from t2;

-- query 34
USE sql_tests_test_sum_map;
select sum_map(m_bigint)[1], sum_map(m_bigint)[2], sum_map(m_bigint)[3] from t2;

-- query 35
USE sql_tests_test_sum_map;
select sum_map(m_largeint)[1], sum_map(m_largeint)[2], sum_map(m_largeint)[3] from t2;

-- query 36
USE sql_tests_test_sum_map;
select sum_map(m_float)[1], sum_map(m_float)[2], sum_map(m_float)[3] from t2;

-- query 37
USE sql_tests_test_sum_map;
select sum_map(m_double)[1], sum_map(m_double)[2], sum_map(m_double)[3] from t2;

-- query 38
-- @skip_result_check=true
USE sql_tests_test_sum_map;
CREATE TABLE t3 (
    id int,
    m_key_tinyint map<tinyint, int>,
    m_key_smallint map<smallint, int>,
    m_key_int map<int, int>,
    m_key_bigint map<bigint, int>,
    m_key_largeint map<largeint, int>,
    m_key_varchar map<varchar(20), int>
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 39
-- @skip_result_check=true
USE sql_tests_test_sum_map;
INSERT INTO t3 values
    (1, map{1:10, 2:20}, map{1:100, 2:200}, map{1:1000, 2:2000},
        map{1:10000, 2:20000}, map{1:100000, 2:200000}, map{"a":10, "b":20}),
    (2, map{1:5, 3:30}, map{1:50, 3:300}, map{1:500, 3:3000},
        map{1:5000, 3:30000}, map{1:50000, 3:300000}, map{"a":5, "c":30});

-- query 40
USE sql_tests_test_sum_map;
select sum_map(m_key_tinyint)[1], sum_map(m_key_tinyint)[2], sum_map(m_key_tinyint)[3] from t3;

-- query 41
USE sql_tests_test_sum_map;
select sum_map(m_key_smallint)[1], sum_map(m_key_smallint)[2], sum_map(m_key_smallint)[3] from t3;

-- query 42
USE sql_tests_test_sum_map;
select sum_map(m_key_int)[1], sum_map(m_key_int)[2], sum_map(m_key_int)[3] from t3;

-- query 43
USE sql_tests_test_sum_map;
select sum_map(m_key_bigint)[1], sum_map(m_key_bigint)[2], sum_map(m_key_bigint)[3] from t3;

-- query 44
USE sql_tests_test_sum_map;
select sum_map(m_key_largeint)[1], sum_map(m_key_largeint)[2], sum_map(m_key_largeint)[3] from t3;

-- query 45
USE sql_tests_test_sum_map;
select sum_map(m_key_varchar)["a"], sum_map(m_key_varchar)["b"], sum_map(m_key_varchar)["c"] from t3;

-- query 46
USE sql_tests_test_sum_map;
select sum_map(NULL);

-- query 47
USE sql_tests_test_sum_map;
select sum_map(m)[1], sum_map(m)[2], sum_map(m)[3] from (
    select map{1:10, 2:20} as m
    union all
    select NULL as m
    union all
    select map{1:5, 3:30} as m
) t;

-- query 48
USE sql_tests_test_sum_map;
select sum_map(m) from (
    select NULL as m
    union all
    select NULL as m
    union all
    select NULL as m
) t;

-- query 49
-- @skip_result_check=true
USE sql_tests_test_sum_map;
CREATE TABLE t5 (
    id int,
    m_nullable map<int, int>
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 50
-- @skip_result_check=true
USE sql_tests_test_sum_map;
INSERT INTO t5 values
    (1, map{1:10, 2:20, NULL:100}),
    (2, map{1:5, NULL:50, 3:30});

-- query 51
USE sql_tests_test_sum_map;
select sum_map(m_nullable)[1], sum_map(m_nullable)[2], sum_map(m_nullable)[3], sum_map(m_nullable)[NULL] from t5;

-- query 52
-- @skip_result_check=true
USE sql_tests_test_sum_map;
INSERT INTO t5 values
    (3, map{1:15, 2:NULL, 4:40});

-- query 53
USE sql_tests_test_sum_map;
select sum_map(m_nullable)[1], sum_map(m_nullable)[2], sum_map(m_nullable)[3], sum_map(m_nullable)[4], sum_map(m_nullable)[NULL] from t5;

-- query 54
-- @skip_result_check=true
USE sql_tests_test_sum_map;
INSERT INTO t5 values
    (4, map{NULL:25, 5:NULL, 6:60});

-- query 55
USE sql_tests_test_sum_map;
select sum_map(m_nullable)[1], sum_map(m_nullable)[2], sum_map(m_nullable)[3], sum_map(m_nullable)[4], sum_map(m_nullable)[5], sum_map(m_nullable)[6], sum_map(m_nullable)[NULL] from t5;

-- query 56
USE sql_tests_test_sum_map;
select sum_map(m)[1], sum_map(m)[2] from (select map{1:10, 2:20} as m) t;

-- query 57
USE sql_tests_test_sum_map;
select sum_map(m) from ( select map{} as m union all select map{} as m union all select map{} as m ) t;

-- query 58
USE sql_tests_test_sum_map;
select sum_map(m)[1], sum_map(m)[2] from (
    select map{1:10} as m
    union all
    select map{} as m
    union all
    select map{1:5, 2:20} as m
) t;

-- query 59
USE sql_tests_test_sum_map;
select sum_map(m)[1], sum_map(m)[2], sum_map(m)[5], sum_map(m)[10] from (
    select map{1:1, 2:2, 3:3, 4:4, 5:5, 6:6, 7:7, 8:8, 9:9, 10:10} as m
    union all
    select map{1:1, 2:2, 3:3, 4:4, 5:5, 6:6, 7:7, 8:8, 9:9, 10:10} as m
) t;

-- query 60
USE sql_tests_test_sum_map;
select sum_map(m)[1], sum_map(m)[2], sum_map(m)[3], sum_map(m)[4] from (
    select map{1:-10, 2:20, 3:-30} as m
    union all
    select map{1:5, 2:-15, 4:40} as m
) t;

-- query 61
USE sql_tests_test_sum_map;
select sum_map(m)[1], sum_map(m)[2] from (
    select map{1:0, 2:10} as m
    union all
    select map{1:5, 2:0} as m
) t;

-- query 62
-- @expect_error=sum_map only support scalar KV
USE sql_tests_test_sum_map;
select sum_map(m) from (
    select map{1:map{1:10}} as m
) t;

-- query 63
-- @expect_error=sum_map only support scalar KV
USE sql_tests_test_sum_map;
select sum_map(m) from (
    select map(1,[1,2,3]) as m
) t;

-- query 64
-- @expect_error=sum_map only support scalar KV
USE sql_tests_test_sum_map;
select sum_map(m) from (
    select map{1:row(1, 'a')} as m
) t;

-- query 65
-- @skip_result_check=true
USE sql_tests_test_sum_map;
CREATE TABLE t8 (
    category varchar(20),
    metrics map<varchar(20), int>
)
DUPLICATE KEY(category)
DISTRIBUTED BY HASH(category) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 66
-- @skip_result_check=true
USE sql_tests_test_sum_map;
INSERT INTO t8 values
    ('A', map{"clicks":100, "views":200}),
    ('A', map{"clicks":50, "impressions":150}),
    ('B', map{"views":300, "impressions":250}),
    ('B', map{"clicks":75, "views":100});

-- query 67
USE sql_tests_test_sum_map;
select category, sum_map(metrics)["clicks"], sum_map(metrics)["views"], sum_map(metrics)["impressions"], count(*) from t8 group by category order by category;

-- query 68
USE sql_tests_test_sum_map;
select category, sum_map(metrics)["clicks"] as clicks_total, sum_map(metrics)["views"] as views_total, sum_map(metrics)["impressions"] as impressions_total from t8 group by category having count(*) > 1 order by category;

-- query 69
USE sql_tests_test_sum_map;
select sum_map(m)[1], sum_map(m)[2], sum_map(m)[3] from (
    select map{3:30, 1:10, 2:20} as m
    union all
    select map{2:25, 3:35, 1:15} as m
) t;

-- query 70
-- @skip_result_check=true
USE sql_tests_test_sum_map;
CREATE TABLE t10 (
    id int,
    data map<int, bigint>
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 71
-- @skip_result_check=true
USE sql_tests_test_sum_map;
INSERT INTO t10 select
    generate_series as id,
    map{1:generate_series, 2:generate_series*2, 3:generate_series*3} as data
from table(generate_series(1, 100));

-- query 72
USE sql_tests_test_sum_map;
select sum_map(data)[1], sum_map(data)[2], sum_map(data)[3] from t10;

-- query 73
-- @skip_result_check=true
USE sql_tests_test_sum_map;
CREATE TABLE t11_type_matrix (
    id int,
    -- Numeric key types with numeric value types (VALID - 7×7=49 combinations)
    m_ti_ti map<tinyint, tinyint>,
    m_ti_si map<tinyint, smallint>,
    m_ti_i map<tinyint, int>,
    m_ti_bi map<tinyint, bigint>,
    m_ti_li map<tinyint, largeint>,
    m_ti_f map<tinyint, float>,
    m_ti_d map<tinyint, double>,

    m_si_ti map<smallint, tinyint>,
    m_si_si map<smallint, smallint>,
    m_si_i map<smallint, int>,
    m_si_bi map<smallint, bigint>,
    m_si_li map<smallint, largeint>,
    m_si_f map<smallint, float>,
    m_si_d map<smallint, double>,

    m_i_ti map<int, tinyint>,
    m_i_si map<int, smallint>,
    m_i_i map<int, int>,
    m_i_bi map<int, bigint>,
    m_i_li map<int, largeint>,
    m_i_f map<int, float>,
    m_i_d map<int, double>,

    m_bi_ti map<bigint, tinyint>,
    m_bi_si map<bigint, smallint>,
    m_bi_i map<bigint, int>,
    m_bi_bi map<bigint, bigint>,
    m_bi_li map<bigint, largeint>,
    m_bi_f map<bigint, float>,
    m_bi_d map<bigint, double>,

    m_li_ti map<largeint, tinyint>,
    m_li_si map<largeint, smallint>,
    m_li_i map<largeint, int>,
    m_li_bi map<largeint, bigint>,
    m_li_li map<largeint, largeint>,
    m_li_f map<largeint, float>,
    m_li_d map<largeint, double>,

    m_f_ti map<float, tinyint>,
    m_f_si map<float, smallint>,
    m_f_i map<float, int>,
    m_f_bi map<float, bigint>,
    m_f_li map<float, largeint>,
    m_f_f map<float, float>,
    m_f_d map<float, double>,

    m_d_ti map<double, tinyint>,
    m_d_si map<double, smallint>,
    m_d_i map<double, int>,
    m_d_bi map<double, bigint>,
    m_d_li map<double, largeint>,
    m_d_f map<double, float>,
    m_d_d map<double, double>,

    -- Date/datetime keys with numeric values (7 combinations)
    m_date_i map<date, int>,
    m_dt_i map<datetime, int>,

    -- String key with numeric values
    m_str_i map<varchar(10), int>,

    -- Non-numeric value types (should fail - documenting expected errors)
    m_i_date map<int, date>,
    m_i_dt map<int, datetime>,
    m_i_str map<int, varchar(10)>
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 74
-- @skip_result_check=true
USE sql_tests_test_sum_map;
INSERT INTO t11_type_matrix VALUES (
    1,
    -- tinyint keys
    map{1:10, 2:20}, map{1:100, 2:200}, map{1:1000, 2:2000}, map{1:10000, 2:20000},
    map{1:100000, 2:200000}, map{1:1.5, 2:2.5}, map{1:10.5, 2:20.5},
    -- smallint keys
    map{1:10, 2:20}, map{1:100, 2:200}, map{1:1000, 2:2000}, map{1:10000, 2:20000},
    map{1:100000, 2:200000}, map{1:1.5, 2:2.5}, map{1:10.5, 2:20.5},
    -- int keys
    map{1:10, 2:20}, map{1:100, 2:200}, map{1:1000, 2:2000}, map{1:10000, 2:20000},
    map{1:100000, 2:200000}, map{1:1.5, 2:2.5}, map{1:10.5, 2:20.5},
    -- bigint keys
    map{1:10, 2:20}, map{1:100, 2:200}, map{1:1000, 2:2000}, map{1:10000, 2:20000},
    map{1:100000, 2:200000}, map{1:1.5, 2:2.5}, map{1:10.5, 2:20.5},
    -- largeint keys
    map{1:10, 2:20}, map{1:100, 2:200}, map{1:1000, 2:2000}, map{1:10000, 2:20000},
    map{1:100000, 2:200000}, map{1:1.5, 2:2.5}, map{1:10.5, 2:20.5},
    -- float keys
    map{1.0:10, 2.0:20}, map{1.0:100, 2.0:200}, map{1.0:1000, 2.0:2000}, map{1.0:10000, 2.0:20000},
    map{1.0:100000, 2.0:200000}, map{1.0:1.5, 2.0:2.5}, map{1.0:10.5, 2.0:20.5},
    -- double keys
    map{1.0:10, 2.0:20}, map{1.0:100, 2.0:200}, map{1.0:1000, 2.0:2000}, map{1.0:10000, 2.0:20000},
    map{1.0:100000, 2.0:200000}, map{1.0:1.5, 2.0:2.5}, map{1.0:10.5, 2.0:20.5},
    -- date/datetime/string keys with numeric values
    map{'2024-01-01':100, '2024-01-02':200}, map{'2024-01-01 10:00:00':100, '2024-01-01 11:00:00':200},
    map{"a":100, "b":200},
    -- Non-numeric values (will cause errors if tested)
    map{1:'2024-01-01', 2:'2024-01-02'}, map{1:'2024-01-01 10:00:00', 2:'2024-01-01 11:00:00'},
    map{1:"a", 2:"b"}
);

-- query 75
-- @skip_result_check=true
USE sql_tests_test_sum_map;
INSERT INTO t11_type_matrix VALUES (
    2,
    -- tinyint keys
    map{1:5, 3:30}, map{1:50, 3:300}, map{1:500, 3:3000}, map{1:5000, 3:30000},
    map{1:50000, 3:300000}, map{1:0.5, 3:3.5}, map{1:5.5, 3:30.5},
    -- smallint keys
    map{1:5, 3:30}, map{1:50, 3:300}, map{1:500, 3:3000}, map{1:5000, 3:30000},
    map{1:50000, 3:300000}, map{1:0.5, 3:3.5}, map{1:5.5, 3:30.5},
    -- int keys
    map{1:5, 3:30}, map{1:50, 3:300}, map{1:500, 3:3000}, map{1:5000, 3:30000},
    map{1:50000, 3:300000}, map{1:0.5, 3:3.5}, map{1:5.5, 3:30.5},
    -- bigint keys
    map{1:5, 3:30}, map{1:50, 3:300}, map{1:500, 3:3000}, map{1:5000, 3:30000},
    map{1:50000, 3:300000}, map{1:0.5, 3:3.5}, map{1:5.5, 3:30.5},
    -- largeint keys
    map{1:5, 3:30}, map{1:50, 3:300}, map{1:500, 3:3000}, map{1:5000, 3:30000},
    map{1:50000, 3:300000}, map{1:0.5, 3:3.5}, map{1:5.5, 3:30.5},
    -- float keys
    map{1.0:5, 3.0:30}, map{1.0:50, 3.0:300}, map{1.0:500, 3.0:3000}, map{1.0:5000, 3.0:30000},
    map{1.0:50000, 3.0:300000}, map{1.0:0.5, 3.0:3.5}, map{1.0:5.5, 3.0:30.5},
    -- double keys
    map{1.0:5, 3.0:30}, map{1.0:50, 3.0:300}, map{1.0:500, 3.0:3000}, map{1.0:5000, 3.0:30000},
    map{1.0:50000, 3.0:300000}, map{1.0:0.5, 3.0:3.5}, map{1.0:5.5, 3.0:30.5},
    -- date/datetime/string keys with numeric values
    map{'2024-01-01':50, '2024-01-03':300}, map{'2024-01-01 10:00:00':50, '2024-01-01 12:00:00':300},
    map{"a":50, "c":300},
    -- Non-numeric values
    map{1:'2024-01-03', 3:'2024-01-04'}, map{1:'2024-01-01 12:00:00', 3:'2024-01-01 13:00:00'},
    map{1:"c", 3:"d"}
);

-- query 76
USE sql_tests_test_sum_map;
select sum_map(m_ti_ti)[1], sum_map(m_ti_ti)[2], sum_map(m_ti_ti)[3] from t11_type_matrix;

-- query 77
USE sql_tests_test_sum_map;
select sum_map(m_ti_si)[1], sum_map(m_ti_si)[2], sum_map(m_ti_si)[3] from t11_type_matrix;

-- query 78
USE sql_tests_test_sum_map;
select sum_map(m_ti_i)[1], sum_map(m_ti_i)[2], sum_map(m_ti_i)[3] from t11_type_matrix;

-- query 79
USE sql_tests_test_sum_map;
select sum_map(m_ti_bi)[1], sum_map(m_ti_bi)[2], sum_map(m_ti_bi)[3] from t11_type_matrix;

-- query 80
USE sql_tests_test_sum_map;
select sum_map(m_ti_li)[1], sum_map(m_ti_li)[2], sum_map(m_ti_li)[3] from t11_type_matrix;

-- query 81
USE sql_tests_test_sum_map;
select sum_map(m_ti_f)[1], sum_map(m_ti_f)[2], sum_map(m_ti_f)[3] from t11_type_matrix;

-- query 82
USE sql_tests_test_sum_map;
select sum_map(m_ti_d)[1], sum_map(m_ti_d)[2], sum_map(m_ti_d)[3] from t11_type_matrix;

-- query 83
USE sql_tests_test_sum_map;
select sum_map(m_si_ti)[1], sum_map(m_si_ti)[2], sum_map(m_si_ti)[3] from t11_type_matrix;

-- query 84
USE sql_tests_test_sum_map;
select sum_map(m_si_si)[1], sum_map(m_si_si)[2], sum_map(m_si_si)[3] from t11_type_matrix;

-- query 85
USE sql_tests_test_sum_map;
select sum_map(m_si_i)[1], sum_map(m_si_i)[2], sum_map(m_si_i)[3] from t11_type_matrix;

-- query 86
USE sql_tests_test_sum_map;
select sum_map(m_si_bi)[1], sum_map(m_si_bi)[2], sum_map(m_si_bi)[3] from t11_type_matrix;

-- query 87
USE sql_tests_test_sum_map;
select sum_map(m_si_li)[1], sum_map(m_si_li)[2], sum_map(m_si_li)[3] from t11_type_matrix;

-- query 88
USE sql_tests_test_sum_map;
select sum_map(m_si_f)[1], sum_map(m_si_f)[2], sum_map(m_si_f)[3] from t11_type_matrix;

-- query 89
USE sql_tests_test_sum_map;
select sum_map(m_si_d)[1], sum_map(m_si_d)[2], sum_map(m_si_d)[3] from t11_type_matrix;

-- query 90
USE sql_tests_test_sum_map;
select sum_map(m_i_ti)[1], sum_map(m_i_ti)[2], sum_map(m_i_ti)[3] from t11_type_matrix;

-- query 91
USE sql_tests_test_sum_map;
select sum_map(m_i_si)[1], sum_map(m_i_si)[2], sum_map(m_i_si)[3] from t11_type_matrix;

-- query 92
USE sql_tests_test_sum_map;
select sum_map(m_i_i)[1], sum_map(m_i_i)[2], sum_map(m_i_i)[3] from t11_type_matrix;

-- query 93
USE sql_tests_test_sum_map;
select sum_map(m_i_bi)[1], sum_map(m_i_bi)[2], sum_map(m_i_bi)[3] from t11_type_matrix;

-- query 94
USE sql_tests_test_sum_map;
select sum_map(m_i_li)[1], sum_map(m_i_li)[2], sum_map(m_i_li)[3] from t11_type_matrix;

-- query 95
USE sql_tests_test_sum_map;
select sum_map(m_i_f)[1], sum_map(m_i_f)[2], sum_map(m_i_f)[3] from t11_type_matrix;

-- query 96
USE sql_tests_test_sum_map;
select sum_map(m_i_d)[1], sum_map(m_i_d)[2], sum_map(m_i_d)[3] from t11_type_matrix;

-- query 97
USE sql_tests_test_sum_map;
select sum_map(m_bi_ti)[1], sum_map(m_bi_ti)[2], sum_map(m_bi_ti)[3] from t11_type_matrix;

-- query 98
USE sql_tests_test_sum_map;
select sum_map(m_bi_si)[1], sum_map(m_bi_si)[2], sum_map(m_bi_si)[3] from t11_type_matrix;

-- query 99
USE sql_tests_test_sum_map;
select sum_map(m_bi_i)[1], sum_map(m_bi_i)[2], sum_map(m_bi_i)[3] from t11_type_matrix;

-- query 100
USE sql_tests_test_sum_map;
select sum_map(m_bi_bi)[1], sum_map(m_bi_bi)[2], sum_map(m_bi_bi)[3] from t11_type_matrix;

-- query 101
USE sql_tests_test_sum_map;
select sum_map(m_bi_li)[1], sum_map(m_bi_li)[2], sum_map(m_bi_li)[3] from t11_type_matrix;

-- query 102
USE sql_tests_test_sum_map;
select sum_map(m_bi_f)[1], sum_map(m_bi_f)[2], sum_map(m_bi_f)[3] from t11_type_matrix;

-- query 103
USE sql_tests_test_sum_map;
select sum_map(m_bi_d)[1], sum_map(m_bi_d)[2], sum_map(m_bi_d)[3] from t11_type_matrix;

-- query 104
USE sql_tests_test_sum_map;
select sum_map(m_li_ti)[1], sum_map(m_li_ti)[2], sum_map(m_li_ti)[3] from t11_type_matrix;

-- query 105
USE sql_tests_test_sum_map;
select sum_map(m_li_si)[1], sum_map(m_li_si)[2], sum_map(m_li_si)[3] from t11_type_matrix;

-- query 106
USE sql_tests_test_sum_map;
select sum_map(m_li_i)[1], sum_map(m_li_i)[2], sum_map(m_li_i)[3] from t11_type_matrix;

-- query 107
USE sql_tests_test_sum_map;
select sum_map(m_li_bi)[1], sum_map(m_li_bi)[2], sum_map(m_li_bi)[3] from t11_type_matrix;

-- query 108
USE sql_tests_test_sum_map;
select sum_map(m_li_li)[1], sum_map(m_li_li)[2], sum_map(m_li_li)[3] from t11_type_matrix;

-- query 109
USE sql_tests_test_sum_map;
select sum_map(m_li_f)[1], sum_map(m_li_f)[2], sum_map(m_li_f)[3] from t11_type_matrix;

-- query 110
USE sql_tests_test_sum_map;
select sum_map(m_li_d)[1], sum_map(m_li_d)[2], sum_map(m_li_d)[3] from t11_type_matrix;

-- query 111
USE sql_tests_test_sum_map;
select sum_map(m_f_ti)[1.0], sum_map(m_f_ti)[2.0], sum_map(m_f_ti)[3.0] from t11_type_matrix;

-- query 112
USE sql_tests_test_sum_map;
select sum_map(m_f_si)[1.0], sum_map(m_f_si)[2.0], sum_map(m_f_si)[3.0] from t11_type_matrix;

-- query 113
USE sql_tests_test_sum_map;
select sum_map(m_f_i)[1.0], sum_map(m_f_i)[2.0], sum_map(m_f_i)[3.0] from t11_type_matrix;

-- query 114
USE sql_tests_test_sum_map;
select sum_map(m_f_bi)[1.0], sum_map(m_f_bi)[2.0], sum_map(m_f_bi)[3.0] from t11_type_matrix;

-- query 115
USE sql_tests_test_sum_map;
select sum_map(m_f_li)[1.0], sum_map(m_f_li)[2.0], sum_map(m_f_li)[3.0] from t11_type_matrix;

-- query 116
USE sql_tests_test_sum_map;
select sum_map(m_f_f)[1.0], sum_map(m_f_f)[2.0], sum_map(m_f_f)[3.0] from t11_type_matrix;

-- query 117
USE sql_tests_test_sum_map;
select sum_map(m_f_d)[1.0], sum_map(m_f_d)[2.0], sum_map(m_f_d)[3.0] from t11_type_matrix;

-- query 118
USE sql_tests_test_sum_map;
select sum_map(m_d_ti)[1.0], sum_map(m_d_ti)[2.0], sum_map(m_d_ti)[3.0] from t11_type_matrix;

-- query 119
USE sql_tests_test_sum_map;
select sum_map(m_d_si)[1.0], sum_map(m_d_si)[2.0], sum_map(m_d_si)[3.0] from t11_type_matrix;

-- query 120
USE sql_tests_test_sum_map;
select sum_map(m_d_i)[1.0], sum_map(m_d_i)[2.0], sum_map(m_d_i)[3.0] from t11_type_matrix;

-- query 121
USE sql_tests_test_sum_map;
select sum_map(m_d_bi)[1.0], sum_map(m_d_bi)[2.0], sum_map(m_d_bi)[3.0] from t11_type_matrix;

-- query 122
USE sql_tests_test_sum_map;
select sum_map(m_d_li)[1.0], sum_map(m_d_li)[2.0], sum_map(m_d_li)[3.0] from t11_type_matrix;

-- query 123
USE sql_tests_test_sum_map;
select sum_map(m_d_f)[1.0], sum_map(m_d_f)[2.0], sum_map(m_d_f)[3.0] from t11_type_matrix;

-- query 124
USE sql_tests_test_sum_map;
select sum_map(m_d_d)[1.0], sum_map(m_d_d)[2.0], sum_map(m_d_d)[3.0] from t11_type_matrix;

-- query 125
USE sql_tests_test_sum_map;
select sum_map(m_date_i)['2024-01-01'], sum_map(m_date_i)['2024-01-02'], sum_map(m_date_i)['2024-01-03'] from t11_type_matrix;

-- query 126
USE sql_tests_test_sum_map;
select sum_map(m_dt_i)['2024-01-01 10:00:00'], sum_map(m_dt_i)['2024-01-01 11:00:00'], sum_map(m_dt_i)['2024-01-01 12:00:00'] from t11_type_matrix;

-- query 127
USE sql_tests_test_sum_map;
select sum_map(m_str_i)["a"], sum_map(m_str_i)["b"], sum_map(m_str_i)["c"] from t11_type_matrix;

-- query 128
-- @expect_error=unsupported value type:DATE
USE sql_tests_test_sum_map;
select sum_map(m_i_date) from t11_type_matrix;

-- query 129
-- @expect_error=unsupported value type:DATETIME
USE sql_tests_test_sum_map;
select sum_map(m_i_dt) from t11_type_matrix;

-- query 130
-- @expect_error=unsupported value type:VARCHAR
USE sql_tests_test_sum_map;
select sum_map(m_i_str) from t11_type_matrix;
