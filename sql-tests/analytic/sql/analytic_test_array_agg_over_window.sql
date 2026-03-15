-- Migrated from dev/test/sql/test_array_agg_over_window/R/test_array_agg_over_window
-- Test Objective:
-- Preserve array test coverage migrated from dev/test.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_array_agg_over_window
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `t0` (
  `v1` int(11) NULL COMMENT "",
  `v2` int(11) NULL COMMENT "",
  `v3` bigint(20) NULL COMMENT "",
  `v4` varchar(50) NULL COMMENT "",
  `v5` decimal(10,2) NULL COMMENT "",
  `v6` float NULL COMMENT ""
) ENGINE=OLAP
PROPERTIES("replication_num"="1");

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into t0 select i/19 as v1, i/11 as v2, i/5 as v3, concat('item_', cast(i as varchar)) as v4, i/3.14 as v5, i/2.71 as v6 from table(generate_series(1,100)) t(i);

-- query 4
USE ${case_db};
select sum(
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0) +
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_distinct)), 0) +
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_order_by)), 0)
) as fingerprint from (
  select v1, v2, v3,
  array_agg(v1) over() as arr_basic,
  array_agg(distinct v2) over() as arr_distinct,
  array_agg(v3 order by v2) over() as arr_order_by
  from t0
) as t;

-- query 5
USE ${case_db};
select sum(
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0) +
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_distinct)), 0) +
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_order_by)), 0)
) as fingerprint from (
  select v1, v2, v3,
  array_agg(v1) over(partition by v1) as arr_basic,
  array_agg(distinct v2) over(partition by v1) as arr_distinct,
  array_agg(v3 order by v2) over(partition by v1) as arr_order_by
  from t0
) as t;

-- query 6
USE ${case_db};
select sum(
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0) +
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_distinct)), 0) +
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_order_by)), 0)
) as fingerprint from (
  select v1, v2, v3,
  array_agg(v1) over(partition by v1 order by v2) as arr_basic,
  array_agg(distinct v2) over(partition by v1 order by v2) as arr_distinct,
  array_agg(v3 order by v2) over(partition by v1 order by v2) as arr_order_by
  from t0
) as t;

-- query 7
USE ${case_db};
select sum(
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0) +
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_distinct)), 0) +
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_order_by)), 0)
) as fingerprint from (
  select v1, v2, v3,
  array_agg(v1) over(order by v2) as arr_basic,
  array_agg(distinct v2) over(order by v2) as arr_distinct,
  array_agg(v3 order by v2) over(order by v2) as arr_order_by
  from t0
) as t;

-- query 8
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(v1) over() as arr_basic
  from t0
) as t;

-- query 9
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_distinct)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(distinct v2) over() as arr_distinct
  from t0
) as t;

-- query 10
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_order_by)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(v3 order by v2) over() as arr_order_by
  from t0
) as t;

-- query 11
USE ${case_db};
select sum(
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0) +
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_distinct)), 0)
) as fingerprint from (
  select v1, v2, v3,
  array_agg(v1) over(partition by v1) as arr_basic,
  array_agg(distinct v2) over(partition by v1) as arr_distinct
  from t0
) as t;

-- query 12
USE ${case_db};
select sum(
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0) +
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_order_by)), 0)
) as fingerprint from (
  select v1, v2, v3,
  array_agg(v1) over(partition by v1 order by v2) as arr_basic,
  array_agg(v3 order by v2) over(partition by v1 order by v2) as arr_order_by
  from t0
) as t;

-- query 13
USE ${case_db};
select sum(
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_distinct)), 0) +
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_order_by)), 0)
) as fingerprint from (
  select v1, v2, v3,
  array_agg(distinct v2) over(partition by v1 order by v2) as arr_distinct,
  array_agg(v3 order by v2) over(partition by v1 order by v2) as arr_order_by
  from t0
) as t;

-- query 14
USE ${case_db};
select sum(
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0) +
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_distinct)), 0) +
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_order_by)), 0)
) as fingerprint from (
  select v1, v2, v3,
  array_agg(v1) over(partition by v1 order by v2) as arr_basic,
  array_agg(distinct v2) over(partition by v1 order by v2) as arr_distinct,
  array_agg(v3 order by v2) over(partition by v1 order by v2) as arr_order_by
  from t0
) as t;

-- query 15
USE ${case_db};
select sum(
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0) +
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_distinct)), 0) +
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_order_by)), 0)
) as fingerprint from (
  select v1, v2, v3,
  array_agg(v1) over(order by v2) as arr_basic,
  array_agg(distinct v2) over(order by v2) as arr_distinct,
  array_agg(v3 order by v2) over(order by v2) as arr_order_by
  from t0
) as t;

-- query 16
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_int)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(v1) over(partition by v1 order by v2) as arr_int
  from t0
) as t;

-- query 17
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, '')), arr_string)), 0)) as fingerprint from (
  select v1, v2, v4,
  array_agg(v4) over(partition by v1 order by v2) as arr_string
  from t0
) as t;

-- query 18
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_decimal)), 0)) as fingerprint from (
  select v1, v2, v5,
  array_agg(v5) over(partition by v1 order by v2) as arr_decimal
  from t0
) as t;

-- query 19
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_float)), 0)) as fingerprint from (
  select v1, v2, v6,
  array_agg(v6) over(partition by v1 order by v2) as arr_float
  from t0
) as t;

-- query 20
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_distinct_int)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(distinct v1) over(partition by v1 order by v2) as arr_distinct_int
  from t0
) as t;

-- query 21
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, '')), arr_distinct_string)), 0)) as fingerprint from (
  select v1, v2, v4,
  array_agg(distinct v4) over(partition by v1 order by v2) as arr_distinct_string
  from t0
) as t;

-- query 22
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_distinct_decimal)), 0)) as fingerprint from (
  select v1, v2, v5,
  array_agg(distinct v5) over(partition by v1 order by v2) as arr_distinct_decimal
  from t0
) as t;

-- query 23
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_distinct_float)), 0)) as fingerprint from (
  select v1, v2, v6,
  array_agg(distinct v6) over(partition by v1 order by v2) as arr_distinct_float
  from t0
) as t;

-- query 24
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_order_by_int)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(v1 order by v2) over(partition by v1 order by v2) as arr_order_by_int
  from t0
) as t;

-- query 25
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, '')), arr_order_by_string)), 0)) as fingerprint from (
  select v1, v2, v4,
  array_agg(v4 order by v2) over(partition by v1 order by v2) as arr_order_by_string
  from t0
) as t;

-- query 26
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_order_by_decimal)), 0)) as fingerprint from (
  select v1, v2, v5,
  array_agg(v5 order by v2) over(partition by v1 order by v2) as arr_order_by_decimal
  from t0
) as t;

-- query 27
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_order_by_float)), 0)) as fingerprint from (
  select v1, v2, v6,
  array_agg(v6 order by v2) over(partition by v1 order by v2) as arr_order_by_float
  from t0
) as t;

-- query 28
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(v1 + v2) over(partition by v1 order by v2) as arr_basic
  from t0
) as t;

-- query 29
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(v1 * v2) over(partition by v1 order by v2) as arr_basic
  from t0
) as t;

-- query 30
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(cast(v3 as decimal(10,2))) over(partition by v1 order by v2) as arr_basic
  from t0
) as t;

-- query 31
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, '')), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(upper(v4)) over(partition by v1 order by v2) as arr_basic
  from t0
) as t;

-- query 32
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(length(v4)) over(partition by v1 order by v2) as arr_basic
  from t0
) as t;

-- query 33
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(abs(v6)) over(partition by v1 order by v2) as arr_basic
  from t0
) as t;

-- query 34
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(round(v6, 2)) over(partition by v1 order by v2) as arr_basic
  from t0
) as t;

-- query 35
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, '')), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(concat('prefix_', v4)) over(partition by v1 order by v2) as arr_basic
  from t0
) as t;

-- query 36
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, '')), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(case when v1 > 2 then 'high' else 'low' end) over(partition by v1 order by v2) as arr_basic
  from t0
) as t;

-- query 37
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(coalesce(v5, 0.0)) over(partition by v1 order by v2) as arr_basic
  from t0
) as t;

-- query 38
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(sqrt(abs(v6))) over(partition by v1 order by v2) as arr_basic
  from t0
) as t;

-- query 39
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(pow(v6, 2)) over(partition by v1 order by v2) as arr_basic
  from t0
) as t;

-- query 40
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(year(days_add('2025-01-01', v3))) over(partition by v1 order by v2) as arr_basic
  from t0
) as t;

-- query 41
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(month(days_add('2025-01-01', v3))) over(partition by v1 order by v2) as arr_basic
  from t0
) as t;

-- query 42
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(nullif(v1, 1)) over(partition by v1 order by v2) as arr_basic
  from t0
) as t;

-- query 43
USE ${case_db};
select sum(
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0) +
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_complex)), 0) +
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_with_null)), 0)
) as fingerprint from (
  select v1, v2, v3,
  array_agg(distinct v2) over(partition by v1 order by v2) as arr_basic,
  array_agg(v1) over(partition by v1 order by v2 range between unbounded preceding and unbounded following) as arr_complex,
  array_agg(distinct nullif(v2, 2)) over(partition by v1 order by v2) as arr_with_null
  from t0
) as t;

-- query 44
USE ${case_db};
select sum(
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0) +
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_complex)), 0) +
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_with_null)), 0)
) as fingerprint from (
  select v1, v2, v3,
  array_agg(v3 order by v2) over(partition by v1 order by v2) as arr_basic,
  array_agg(v1) over(partition by v1 order by v2 range between current row and unbounded following) as arr_complex,
  array_agg(v3 order by v2) over(partition by v1 order by v2) as arr_with_null
  from t0
) as t;

-- query 45
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_large)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(v1) over(partition by v1 order by v2) as arr_large,
  array_agg(distinct v2) over(partition by v1 order by v2) as arr_large2,
  array_agg(v3 order by v2) over(partition by v1 order by v2) as arr_large3
  from t0
) as t;

-- query 46
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_large)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(v1 + v2) over(partition by v1 order by v2) as arr_large,
  array_agg(cast(v3 as decimal(19,2))) over(partition by v1 order by v2) as arr_large2,
  array_agg(length(v4)) over(partition by v1 order by v2) as arr_large3
  from t0
) as t;

-- query 47
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(0) over(partition by v1 order by v2) as arr_basic,
  array_agg('constant') over(partition by v1 order by v2) as arr_basic2,
  array_agg(3.14159) over(partition by v1 order by v2) as arr_basic3
  from t0
) as t;

-- query 48
USE ${case_db};
select sum(
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0) +
  murmur_hash3_32(coalesce(rn, 0)) +
  murmur_hash3_32(coalesce(rank, 0))
) as fingerprint from (
  select v1, v2, v3,
  array_agg(v1) over(partition by v1 order by v2) as arr_basic,
  row_number() over(partition by v1 order by v2) as rn,
  rank() over(partition by v1 order by v2) as rank
  from t0
) as t;

-- query 49
-- @expect_error=Getting syntax error
USE ${case_db};
select sum(
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0) +
  murmur_hash3_32(coalesce(d_rank, 0)) +
  murmur_hash3_32(coalesce(lag, 0))
) as fingerprint from (
  select v1, v2, v3,
  array_agg(distinct v2) over(partition by v1 order by v2) as arr_basic,
  dense_rank() over(partition by v1 order by v2) as d_rank,
  lag(v1, 1) over(partition by v1 order by v2) as lag
  from t0
) as t;

-- query 50
-- @expect_error=Getting syntax error
USE ${case_db};
select sum(
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0) +
  murmur_hash3_32(coalesce(lead, 0)) +
  murmur_hash3_32(coalesce(sum_window, 0))
) as fingerprint from (
  select v1, v2, v3,
  array_agg(v3 order by v2) over(partition by v1 order by v2) as arr_basic,
  lead(v1, 1) over(partition by v1 order by v2) as lead,
  sum(v1) over(partition by v1 order by v2) as sum_window
  from t0
) as t;

-- query 51
USE ${case_db};
select sum(
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0) +
  murmur_hash3_32(coalesce(cast(avg_window as bigint), 0)) +
  murmur_hash3_32(coalesce(count_window, 0))
) as fingerprint from (
  select v1, v2, v3,
  array_agg(v1) over(partition by v1 order by v2) as arr_basic,
  avg(v1) over(partition by v1 order by v2) as avg_window,
  count(v1) over(partition by v1 order by v2) as count_window
  from t0
) as t;

-- query 52
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(123456789012345678901234567890.123456789) over(partition by v1 order by v2) as arr_basic
  from t0
) as t;

-- query 53
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(-9876543210.987654321) over(partition by v1 order by v2) as arr_basic
  from t0
) as t;

-- query 54
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, '')), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg('this_is_a_very_long_string_with_many_characters_for_testing') over(partition by v1 order by v2) as arr_basic
  from t0
) as t;

-- query 55
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(v1) over(partition by v1, v2 order by v3) as arr_basic
  from t0
) as t;

-- query 56
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(distinct v1) over(partition by v1, v2 order by v3) as arr_basic
  from t0
) as t;

-- query 57
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(v1 order by v2, v3) over(partition by v1 order by v2) as arr_basic
  from t0
) as t;

-- query 58
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `t1` (
  `v1` int(11) NULL COMMENT "",
  `v2` int(11) NULL COMMENT "",
  `v3` bigint(20) NULL COMMENT "",
  `v4` varchar(50) NULL COMMENT "",
  `v5` decimal(10,2) NULL COMMENT "",
  `v6` float NULL COMMENT ""
) ENGINE=OLAP
PROPERTIES("replication_num"="1");

-- query 59
-- @skip_result_check=true
USE ${case_db};
insert into t1 values
  (1, 1, 100, 'a', 1.1, 1.1),
  (1, 2, NULL, 'b', 2.2, NULL),
  (1, NULL, 300, NULL, 3.3, 3.3),
  (NULL, 4, 400, 'd', NULL, 4.4),
  (2, 1, 500, 'e', 5.5, 5.5),
  (2, NULL, NULL, NULL, NULL, NULL),
  (2, 3, 700, 'g', 7.7, 7.7),
  (NULL, NULL, NULL, NULL, NULL, NULL),
  (3, 1, 900, 'i', 9.9, 9.9),
  (3, 2, 1000, 'j', 10.1, 10.1);

-- query 60
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(v1) over() as arr_basic
  from t1
) as t;

-- query 61
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_distinct)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(distinct v1) over() as arr_distinct
  from t1
) as t;

-- query 62
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_order_by)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(v3 order by v2) over() as arr_order_by
  from t1
) as t;

-- query 63
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, '')), arr_string)), 0)) as fingerprint from (
  select v1, v2, v4,
  array_agg(v4) over() as arr_string
  from t1
) as t;

-- query 64
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_decimal)), 0)) as fingerprint from (
  select v1, v2, v5,
  array_agg(v5) over() as arr_decimal
  from t1
) as t;

-- query 65
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_float)), 0)) as fingerprint from (
  select v1, v2, v6,
  array_agg(v6) over() as arr_float
  from t1
) as t;

-- query 66
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(v2) over(partition by v1) as arr_basic
  from t1
) as t;

-- query 67
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_distinct)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(distinct v2) over(partition by v1) as arr_distinct
  from t1
) as t;

-- query 68
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_order_by)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(v3 order by v2) over(partition by v1) as arr_order_by
  from t1
) as t;

-- query 69
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(v3) over(partition by v1 order by v2) as arr_basic
  from t1
) as t;

-- query 70
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_distinct)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(distinct v3) over(partition by v1 order by v2) as arr_distinct
  from t1
) as t;

-- query 71
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_order_by)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(v3 order by v2) over(partition by v1 order by v2) as arr_order_by
  from t1
) as t;

-- query 72
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_frame)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(v3) over(partition by v1 order by v2 range between unbounded preceding and unbounded following) as arr_frame
  from t1
) as t;

-- query 73
USE ${case_db};
select sum(
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_int)), 0) +
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, '')), arr_str)), 0) +
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_dec)), 0)
) as fingerprint from (
  select v1, v2, v3, v4, v5,
  array_agg(v2) over(partition by v1 order by v2) as arr_int,
  array_agg(v4) over(partition by v1 order by v2) as arr_str,
  array_agg(v5) over(partition by v1 order by v2) as arr_dec
  from t1
) as t;

-- query 74
USE ${case_db};
select sum(
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0) +
  murmur_hash3_32(coalesce(rn, 0)) +
  murmur_hash3_32(coalesce(sum_val, 0))
) as fingerprint from (
  select v1, v2, v3,
  array_agg(v3) over(partition by v1 order by v2) as arr_basic,
  row_number() over(partition by v1 order by v2) as rn,
  sum(v3) over(partition by v1 order by v2) as sum_val
  from t1
) as t;

-- query 75
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_expr)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(v1 + v2 + v3) over(partition by v1 order by v2) as arr_expr
  from t1
) as t;

-- query 76
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(v3) over(partition by v1, v2 order by v3) as arr_basic
  from t1
) as t;

-- query 77
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, '')), arr_distinct_str)), 0)) as fingerprint from (
  select v1, v2, v4,
  array_agg(distinct v4) over(partition by v1 order by v2) as arr_distinct_str
  from t1
) as t;

-- query 78
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(v6) over(partition by v1 order by v2) as arr_basic
  from t1
  where v1 = 2
) as t;

-- query 79
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(v3) over() as arr_basic
  from t1
  where v1 = 1 and v2 = 2
) as t;

-- query 80
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_coalesce)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(coalesce(v3, -1)) over(partition by v1 order by v2) as arr_coalesce
  from t1
) as t;

-- query 81
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(v2) over(order by v1, v2) as arr_basic
  from t1
) as t;

-- query 82
USE ${case_db};
select sum(
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_int)), 0) +
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_bigint)), 0) +
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, '')), arr_str)), 0)
) as fingerprint from (
  select v1, v2, v3, v4,
  array_agg(distinct v1) over() as arr_int,
  array_agg(distinct v3) over() as arr_bigint,
  array_agg(distinct v4) over() as arr_str
  from t1
) as t;

-- query 83
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_order_by)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(v3 order by v3) over(partition by v1) as arr_order_by
  from t1
) as t;

-- query 84
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_order_by)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(v3 order by v2, v3) over(partition by v1) as arr_order_by
  from t1
) as t;

-- query 85
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(v3) over(partition by v1 order by v2) as arr_basic
  from t1
  where v1 is null
) as t;

-- query 86
USE ${case_db};
select sum(coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_distinct)), 0)) as fingerprint from (
  select v1, v2, v3,
  array_agg(distinct v5) over(partition by v1) as arr_distinct
  from t1
  where v1 = 2
) as t;

-- query 87
USE ${case_db};
select sum(
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_basic)), 0) +
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_distinct)), 0) +
  coalesce(array_sum(array_map(x -> murmur_hash3_32(coalesce(x, 0)), arr_order_by)), 0)
) as fingerprint from (
  select v1, v2, v3,
  array_agg(v3) over(partition by v1 order by v2) as arr_basic,
  array_agg(distinct v2) over(partition by v1 order by v2) as arr_distinct,
  array_agg(v3 order by v2) over(partition by v1 order by v2) as arr_order_by
  from t1
) as t;

-- query 88
-- @skip_result_check=true
