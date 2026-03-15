-- Migrated from dev/test/sql/test_agg_function/R/test_percentile_approx_weighted
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_cast(percentile_approx_weighted
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1 (
    c1 int,
    c2 double,
    c3 tinyint,
    c4 int,
    c5 bigint,
    c6 largeint,
    c7 string,
    c8 double,
    c9 date,
    c10 datetime,
    c11 array<int>,
    c12 map<double, double>,
    c13 struct<a bigint, b double>
    )
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1)
BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into t1
    select generate_series, generate_series,  11, 111, 1111, 11111, "111111", 1.1, "2024-09-01", "2024-09-01 18:00:00", [1, 2, 3], map(1, 5.5), row(100, 100)
    from table(generate_series(1, 50000, 3));

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into t1 values
    (1, 1, 11, 111, 1111, 11111, "111111", 1.1, "2024-09-01", "2024-09-01 18:00:00", [1, 2, 3], map(1, 5.5), row(100, 100)),
    (2, 2, 22, 222, 2222, 22222, "222222", 2.2, "2024-09-02", "2024-09-02 11:00:00", [3, 4, 5], map(1, 511.2), row(200, 200)),
    (3, 3, 33, 333, 3333, 33333, "333333", 3.3,  "2024-09-03", "2024-09-03 00:00:00", [4, 1, 2], map(1, 666.6), row(300, 300)),
    (4, 4, 11, 444, 4444, 44444, "444444", 4.4, "2024-09-04", "2024-09-04 12:00:00", [7, 7, 5], map(1, 444.4), row(400, 400)),
    (5, null, null, null, null, null, null, null, null, null, null, null, null);

-- query 5
USE ${case_db};
select cast(percentile_approx_weighted(c1, c2, 0.5) as int) from t1;

-- query 6
USE ${case_db};
select cast(percentile_approx_weighted(c1, 1, 0.9) as int) from t1;

-- query 7
-- @expect_error=percentile_approx_weighted requires the second parameter (weight) to be numeric type, but got: NULL_TYPE.
USE ${case_db};
select cast(percentile_approx_weighted(c1, NULL, 0.9) as int) from t1;

-- query 8
USE ${case_db};
select cast(percentile_approx_weighted(c1, c1, 0.9, 10000) as int) from t1;

-- query 9
USE ${case_db};
select cast(percentile_approx_weighted(c2, c1, 0.5) as int) from t1;

-- query 10
USE ${case_db};
select cast(percentile_approx_weighted(c2, 1, 0.9) as int) from t1;

-- query 11
-- @expect_error=Type check failed. compression parameter must be positive in percentile_approx_weighted, but got: 0.0
USE ${case_db};
select cast(percentile_approx_weighted(c2, 0, 0.9, 0) as int) from t1;

-- query 12
USE ${case_db};
select cast(percentile_approx_weighted(c2, 0, 0.9, 1) as int) from t1;

-- query 13
-- @expect_error=Type check failed. compression parameter must be positive in percentile_approx_weighted, but got: 0.0
USE ${case_db};
select cast(percentile_approx_weighted(c2, 1, 0.9, 0) as int) from t1;

-- query 14
USE ${case_db};
select cast(percentile_approx_weighted(c2, 1, 0.9, 1) as int) from t1;

-- query 15
USE ${case_db};
select cast(percentile_approx_weighted(c2, 1, 0.9, 2048) as int) from t1;

-- query 16
USE ${case_db};
select cast(percentile_approx_weighted(c2, 1, 0.9, 4096) as int) from t1;

-- query 17
USE ${case_db};
select cast(percentile_approx_weighted(c2, 1, 0.9, 5000) as int) from t1;

-- query 18
USE ${case_db};
select cast(percentile_approx_weighted(c2, 1, 0.9, 6000) as int) from t1;

-- query 19
USE ${case_db};
select cast(percentile_approx_weighted(c2, 1, 0.9, 7000) as int) from t1;

-- query 20
USE ${case_db};
select cast(percentile_approx_weighted(c2, 1, 0.9, 8000) as int) from t1;

-- query 21
USE ${case_db};
select cast(percentile_approx_weighted(c2, c1, 0.9, 10000) as int) from t1;

-- query 22
USE ${case_db};
select cast(percentile_approx_weighted(c3, c1, 0.5) as int) from t1;

-- query 23
USE ${case_db};
select cast(percentile_approx_weighted(c3, 1, 0.9) as int) from t1;

-- query 24
USE ${case_db};
select cast(percentile_approx_weighted(c3, 0, 0.9, 2048) as int) from t1;

-- query 25
USE ${case_db};
select cast(percentile_approx_weighted(c3, 10, 0.9, 5000) as int) from t1;

-- query 26
USE ${case_db};
select cast(percentile_approx_weighted(c3, c1, 0.9, 10000) as int) from t1;

-- query 27
USE ${case_db};
select cast(percentile_approx_weighted(c4, c1, 0.5) as int) from t1;

-- query 28
USE ${case_db};
select cast(percentile_approx_weighted(c4, 1, 0.9) as int) from t1;

-- query 29
USE ${case_db};
select cast(percentile_approx_weighted(c4, 0, 0.9, 2048) as int) from t1;

-- query 30
USE ${case_db};
select cast(percentile_approx_weighted(c4, 10, 0.9, 5000) as int) from t1;

-- query 31
USE ${case_db};
select cast(percentile_approx_weighted(c4, c1, 0.9, 10000) as int) from t1;

-- query 32
USE ${case_db};
select cast(percentile_approx_weighted(c5, c1, 0.5) as int) from t1;

-- query 33
USE ${case_db};
select cast(percentile_approx_weighted(c5, c1, 0.9, 10000) as int) from t1;

-- query 34
USE ${case_db};
select cast(percentile_approx_weighted(c5, 1, 0.9) as int) from t1;

-- query 35
USE ${case_db};
select cast(percentile_approx_weighted(c5, 0, 0.9, 2048) as int) from t1;

-- query 36
USE ${case_db};
select cast(percentile_approx_weighted(c5, 10, 0.9, 5000) as int) from t1;

-- query 37
USE ${case_db};
select cast(percentile_approx_weighted(c6, c1, 0.5) as int) from t1;

-- query 38
USE ${case_db};
select cast(percentile_approx_weighted(c6, c1, 0.9, 10000) as int) from t1;

-- query 39
USE ${case_db};
select cast(percentile_approx_weighted(c6, 1, 0.9) as int) from t1;

-- query 40
USE ${case_db};
select cast(percentile_approx_weighted(c6, 0, 0.9, 2048) as int) from t1;

-- query 41
USE ${case_db};
select cast(percentile_approx_weighted(c6, 10, 0.9, 5000) as int) from t1;

-- query 42
-- @expect_error=percentile_approx_weighted requires the first parameter (value) to be numeric type, but got: varchar(65533).
USE ${case_db};
select cast(percentile_approx_weighted(c7, c1, 0.5) as int) from t1;

-- query 43
-- @expect_error=percentile_approx_weighted requires the first parameter (value) to be numeric type, but got: date.
USE ${case_db};
select cast(percentile_approx_weighted(c9, c1, 0.5) as int) from t1;

-- query 44
-- @expect_error=percentile_approx_weighted requires the first parameter (value) to be numeric type, but got: datetime.
USE ${case_db};
select cast(percentile_approx_weighted(c10, c1, 0.5) as int) from t1;

-- query 45
USE ${case_db};
select cast(percentile_approx_weighted(c11[0], c1, 0.5) as int) from t1;

-- query 46
USE ${case_db};
select cast(percentile_approx_weighted(c11[0], 1, 0.5) as int) from t1;

-- query 47
USE ${case_db};
select cast(percentile_approx_weighted(c11[1], 1, 0.5) as int) from t1;

-- query 48
USE ${case_db};
select cast(percentile_approx_weighted(c11[1], 1, 0.5) as int) from t1;

-- query 49
USE ${case_db};
select cast(percentile_approx_weighted(c12[1], c1, 0.5) as int) from t1;

-- query 50
USE ${case_db};
select cast(percentile_approx_weighted(c12[2], c1, 0.5) as int) from t1;

-- query 51
USE ${case_db};
select cast(percentile_approx_weighted(c12[1], 1, 0.5) as int) from t1;

-- query 52
USE ${case_db};
select cast(percentile_approx_weighted(c13.a, c1, 0.5) as int) from t1;

-- query 53
USE ${case_db};
select cast(percentile_approx_weighted(c13.b, c1, 0.5) as int) from t1;

-- query 54
USE ${case_db};
select cast(percentile_approx_weighted(c13.a, 1, 0.5) as int) from t1;

-- query 55
USE ${case_db};
select cast(percentile_approx_weighted(c13.b, 1, 0.5) as int) from t1;

-- query 56
USE ${case_db};
with tt as (select @v1 as v1, c1, c2 from t1) select /*+ set_user_variable(@v1 = 0.5) */ cast(percentile_approx_weighted(c2, c1, v1) as int) from tt;

-- query 57
USE ${case_db};
with tt as (select @v1 as v1, @v2 as v2, c1, c2 from t1) select /*+ set_user_variable(@v1= 0.5, @v2 = 4096) */ cast(percentile_approx_weighted(c2, c1, v1, v2 + 1) as int) from tt;

-- query 58
-- @skip_result_check=true
USE ${case_db};
set pipeline_dop=1;

-- query 59
USE ${case_db};
select cast(percentile_approx_weighted(c1, c2, 0.5) as int) from t1;

-- query 60
USE ${case_db};
select cast(percentile_approx_weighted(c1, 1, 0.9) as int) from t1;

-- query 61
-- @expect_error=percentile_approx_weighted requires the second parameter (weight) to be numeric type, but got: NULL_TYPE.
USE ${case_db};
select cast(percentile_approx_weighted(c1, NULL, 0.9) as int) from t1;

-- query 62
USE ${case_db};
select cast(percentile_approx_weighted(c1, c1, 0.9, 10000) as int) from t1;

-- query 63
USE ${case_db};
select cast(percentile_approx_weighted(c2, c1, 0.5) as int) from t1;

-- query 64
USE ${case_db};
select cast(percentile_approx_weighted(c2, 1, 0.9) as int) from t1;

-- query 65
USE ${case_db};
select cast(percentile_approx_weighted(c2, 0, 0.9, 2048) as int) from t1;

-- query 66
USE ${case_db};
select cast(percentile_approx_weighted(c2, 10, 0.9, 5000) as int) from t1;

-- query 67
USE ${case_db};
select cast(percentile_approx_weighted(c2, c1, 0.9, 10000) as int) from t1;

-- query 68
USE ${case_db};
select cast(percentile_approx_weighted(c3, c1, 0.5) as int) from t1;

-- query 69
USE ${case_db};
select cast(percentile_approx_weighted(c3, 1, 0.9) as int) from t1;

-- query 70
USE ${case_db};
select cast(percentile_approx_weighted(c3, 0, 0.9, 2048) as int) from t1;

-- query 71
USE ${case_db};
select cast(percentile_approx_weighted(c3, 10, 0.9, 5000) as int) from t1;

-- query 72
USE ${case_db};
select cast(percentile_approx_weighted(c3, c1, 0.9, 10000) as int) from t1;

-- query 73
USE ${case_db};
select cast(percentile_approx_weighted(c4, c1, 0.5) as int) from t1;

-- query 74
USE ${case_db};
select cast(percentile_approx_weighted(c4, 1, 0.9) as int) from t1;

-- query 75
USE ${case_db};
select cast(percentile_approx_weighted(c4, 0, 0.9, 2048) as int) from t1;

-- query 76
USE ${case_db};
select cast(percentile_approx_weighted(c4, 10, 0.9, 5000) as int) from t1;

-- query 77
USE ${case_db};
select cast(percentile_approx_weighted(c4, c1, 0.9, 10000) as int) from t1;

-- query 78
USE ${case_db};
select cast(percentile_approx_weighted(c5, c1, 0.5) as int) from t1;

-- query 79
USE ${case_db};
select cast(percentile_approx_weighted(c5, c1, 0.9, 10000) as int) from t1;

-- query 80
USE ${case_db};
select cast(percentile_approx_weighted(c5, 1, 0.9) as int) from t1;

-- query 81
USE ${case_db};
select cast(percentile_approx_weighted(c5, 0, 0.9, 2048) as int) from t1;

-- query 82
USE ${case_db};
select cast(percentile_approx_weighted(c5, 10, 0.9, 5000) as int) from t1;

-- query 83
USE ${case_db};
select cast(percentile_approx_weighted(c6, c1, 0.5) as int) from t1;

-- query 84
USE ${case_db};
select cast(percentile_approx_weighted(c6, c1, 0.9, 10000) as int) from t1;

-- query 85
USE ${case_db};
select cast(percentile_approx_weighted(c6, 1, 0.9) as int) from t1;

-- query 86
USE ${case_db};
select cast(percentile_approx_weighted(c6, 0, 0.9, 2048) as int) from t1;

-- query 87
USE ${case_db};
select cast(percentile_approx_weighted(c6, 10, 0.9, 5000) as int) from t1;

-- query 88
-- @expect_error=percentile_approx_weighted requires the first parameter (value) to be numeric type, but got: varchar(65533).
USE ${case_db};
select cast(percentile_approx_weighted(c7, c1, 0.5) as int) from t1;

-- query 89
-- @expect_error=percentile_approx_weighted requires the first parameter (value) to be numeric type, but got: date.
USE ${case_db};
select cast(percentile_approx_weighted(c9, c1, 0.5) as int) from t1;

-- query 90
-- @expect_error=percentile_approx_weighted requires the first parameter (value) to be numeric type, but got: datetime.
USE ${case_db};
select cast(percentile_approx_weighted(c10, c1, 0.5) as int) from t1;

-- query 91
USE ${case_db};
select cast(percentile_approx_weighted(c11[0], c1, 0.5) as int) from t1;

-- query 92
USE ${case_db};
select cast(percentile_approx_weighted(c11[0], 1, 0.5) as int) from t1;

-- query 93
USE ${case_db};
select cast(percentile_approx_weighted(c11[1], 1, 0.5) as int) from t1;

-- query 94
USE ${case_db};
select cast(percentile_approx_weighted(c11[1], 1, 0.5) as int) from t1;

-- query 95
USE ${case_db};
select cast(percentile_approx_weighted(c12[1], c1, 0.5) as int) from t1;

-- query 96
USE ${case_db};
select cast(percentile_approx_weighted(c12[2], c1, 0.5) as int) from t1;

-- query 97
USE ${case_db};
select cast(percentile_approx_weighted(c12[1], 1, 0.5) as int) from t1;

-- query 98
USE ${case_db};
select cast(percentile_approx_weighted(c13.a, c1, 0.5) as int) from t1;

-- query 99
USE ${case_db};
select cast(percentile_approx_weighted(c13.b, c1, 0.5) as int) from t1;

-- query 100
USE ${case_db};
select cast(percentile_approx_weighted(c13.a, 1, 0.5) as int) from t1;

-- query 101
USE ${case_db};
select cast(percentile_approx_weighted(c13.b, 1, 0.5) as int) from t1;

-- query 102
USE ${case_db};
with tt as (select @v1 as v1, c1, c2 from t1) select /*+ set_user_variable(@v1 = 0.5) */ cast(percentile_approx_weighted(c2, c1, v1) as int) from tt;

-- query 103
USE ${case_db};
with tt as (select @v1 as v1, @v2 as v2, c1, c2 from t1) select /*+ set_user_variable(@v1= 0.5, @v2 = 4096) */ cast(percentile_approx_weighted(c2, c1, v1, v2 + 1) as int) from tt;

-- query 104
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `t2` (
  `k` int(11) NULL COMMENT "",
  `v` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`k`, `v`)
PARTITION BY (`k`)
DISTRIBUTED BY RANDOM
PROPERTIES (
"replication_num" = "1"
);

-- query 105
-- @skip_result_check=true
USE ${case_db};
insert into t2 values(2,1),(3,2),(4,3);

-- query 106
USE ${case_db};
select /*+ SET_VAR (new_planner_agg_stage = '2') */ percentile_approx_weighted(k, v, 0.5, 10000) from t2;

-- query 107
-- @expect_error=Type check failed. percentile parameter must be between 0 and 1 in percentile_approx_weighted, but got: 1.5
USE ${case_db};
select percentile_approx_weighted(1, 1, 1.5) from t2;

-- query 108
-- @expect_error=Type check failed. percentile parameter must be between 0 and 1 in percentile_approx_weighted, but got: -0.1
USE ${case_db};
select percentile_approx_weighted(1, 1, -0.1) from t2;

-- query 109
USE ${case_db};
select percentile_approx_weighted(1, 1, 0.0) from t2;

-- query 110
USE ${case_db};
select percentile_approx_weighted(1, 1, 1.0) from t2;

-- query 111
USE ${case_db};
select percentile_approx_weighted(1, 1, 0.5) from t2;

-- query 112
-- @expect_error=Type check failed. percentile array element[1] must be between 0 and 1 in percentile_approx_weighted, but got: 1.5
USE ${case_db};
select percentile_approx_weighted(k, v, array<double>[0.5, 1.5]) from t2;

-- query 113
-- @expect_error=Type check failed. percentile array element[0] must be between 0 and 1 in percentile_approx_weighted, but got: -0.1
USE ${case_db};
select percentile_approx_weighted(k, v, [-0.1, 0.5]) from t2;

-- query 114
-- @expect_error=percentile_approx_weighted requires the third parameter (percentile) to be ARRAY<NUMERIC>, but got: ARRAY<NULL_TYPE>.
USE ${case_db};
select percentile_approx_weighted(k, v, []) from t2;

-- query 115
USE ${case_db};
select /*+ SET_VAR (new_planner_agg_stage = '2') */ percentile_approx_weighted(k, v, array<float>[0.0, 0.5, 1.0]) from t2;

-- query 116
USE ${case_db};
select percentile_approx_weighted(k, v, [0.25, 0.5, 0.75, 0.9]) from t2;

-- query 117
-- @expect_error=Type check failed. compression parameter must be positive in percentile_approx_weighted, but got: 0.0
USE ${case_db};
select percentile_approx_weighted(k, v, 0.5, 0) from t2;

-- query 118
-- @expect_error=Type check failed. compression parameter must be positive in percentile_approx_weighted, but got: -100.0
USE ${case_db};
select percentile_approx_weighted(k, v, 0.5, -100) from t2;

-- query 119
USE ${case_db};
select percentile_approx_weighted(k, v, 0.5, 1) from t2;

-- query 120
USE ${case_db};
select percentile_approx_weighted(k, v, 0.5, 100) from t2;

-- query 121
USE ${case_db};
select /*+ SET_VAR (new_planner_agg_stage = '2') */ percentile_approx_weighted(k, v, 0.5, 10000) from t2;

-- query 122
USE ${case_db};
select /*+ SET_VAR (new_planner_agg_stage = '2') */ percentile_approx_weighted(k, v, [0.25, 0.5, 0.75], 1000) from t2;

-- query 123
USE ${case_db};
select percentile_approx_weighted(k, v, array<double>[0.1, 0.5, 0.9], 2048) from t2;

-- query 124
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `test_sorted_streaming_agg_percentile_weighted`
(
    `id_int` int(11) NOT NULL COMMENT "",
    `value` double NOT NULL COMMENT ""
)
ENGINE=OLAP
DUPLICATE KEY(`id_int`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id_int`)
BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);

-- query 125
-- @skip_result_check=true
USE ${case_db};
insert into test_sorted_streaming_agg_percentile_weighted values(2,1),(2,6),(4,3),(4,8);

-- query 126
USE ${case_db};
select /*+ SET_VAR (enable_sort_aggregate = 'true') */ percentile_approx_weighted(value, 10, array<double>[0.5, 0.9], 5000) from test_sorted_streaming_agg_percentile_weighted group by id_int;

-- query 127
USE ${case_db};
select /*+ SET_VAR (enable_sort_aggregate = 'true') */ percentile_approx_weighted(value, 0, array<double>[0.5, 0.9], 5000) from test_sorted_streaming_agg_percentile_weighted group by id_int;

-- query 128
USE ${case_db};
select /*+ SET_VAR (enable_sort_aggregate = 'true') */ percentile_approx_weighted(value, value, array<double>[0.5, 0.9], 5000) from test_sorted_streaming_agg_percentile_weighted group by id_int;

-- query 129
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `t0_convert_to_serialize_format` (
  `v1` bigint(20) NULL COMMENT "",
  `v2` bigint(20) NULL COMMENT "",
  `v3` bigint(20) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`v1`, `v2`, `v3`)
DISTRIBUTED BY HASH(`v1`) BUCKETS 3
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);

-- query 130
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `t1_convert_to_serialize_format` (
  `v4` bigint(20) NULL COMMENT "",
  `v5` bigint(20) NULL COMMENT "",
  `v6` bigint(20) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`v4`, `v5`, `v6`)
DISTRIBUTED BY HASH(`v4`) BUCKETS 3
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);

-- query 131
-- @skip_result_check=true
USE ${case_db};
insert into t0_convert_to_serialize_format values(1,2,3),(4,5,6),(7,8,9);

-- query 132
-- @skip_result_check=true
USE ${case_db};
insert into t1_convert_to_serialize_format values(1,2,3),(4,5,6),(7,8,9);

-- query 133
USE ${case_db};
select /*+ SET_VAR (streaming_preaggregation_mode = 'force_streaming') */ percentile_approx_weighted(v2, 10, array<double>[0.1, 0.5, 0.9], 5000) from t0_convert_to_serialize_format join t1_convert_to_serialize_format group by 'a';

-- query 134
USE ${case_db};
select /*+ SET_VAR (streaming_preaggregation_mode = 'force_streaming') */ percentile_approx_weighted(v2, 0, array<double>[0.1, 0.5, 0.9], 5000) from t0_convert_to_serialize_format join t1_convert_to_serialize_format group by 'a';

-- query 135
USE ${case_db};
select /*+ SET_VAR (streaming_preaggregation_mode = 'force_streaming') */ percentile_approx_weighted(v2, v1, array<double>[0.1, 0.5, 0.9], 5000) from t0_convert_to_serialize_format join t1_convert_to_serialize_format group by 'a';
