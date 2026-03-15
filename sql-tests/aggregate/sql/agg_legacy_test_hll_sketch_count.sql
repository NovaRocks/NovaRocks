-- Migrated from dev/test/sql/test_agg_function/R/test_hll_sketch_count.sql
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_ds_hll_count_distinct
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1 (
  id BIGINT NOT NULL,
  province VARCHAR(64),
  age SMALLINT,
  dt VARCHAR(10) NOT NULL
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 4;

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into t1 select generate_series, generate_series, generate_series % 100, "2024-07-24" from table(generate_series(1, 100000));

-- query 4
USE ${case_db};
select
  ds_hll_count_distinct(id) between 95000 and 105000 as id_ok,
  ds_hll_count_distinct(province) between 95000 and 105000 as province_ok,
  ds_hll_count_distinct(age) between 90 and 110 as age_ok,
  ds_hll_count_distinct(dt) = 1 as dt_ok
from t1;

-- query 5
USE ${case_db};
select
  ds_hll_count_distinct(id, 4) between 70000 and 130000 as id_ok,
  ds_hll_count_distinct(province, 4) between 70000 and 130000 as province_ok,
  ds_hll_count_distinct(age, 4) between 90 and 110 as age_ok,
  ds_hll_count_distinct(dt, 4) = 1 as dt_ok
from t1;

-- query 6
USE ${case_db};
select
  ds_hll_count_distinct(id, 10) between 90000 and 110000 as id_ok,
  ds_hll_count_distinct(province, 10) between 90000 and 110000 as province_ok,
  ds_hll_count_distinct(age, 10) between 90 and 110 as age_ok,
  ds_hll_count_distinct(dt, 10) = 1 as dt_ok
from t1;

-- query 7
USE ${case_db};
select
  ds_hll_count_distinct(id, 21) between 95000 and 110000 as id_ok,
  ds_hll_count_distinct(province, 21) between 95000 and 110000 as province_ok,
  ds_hll_count_distinct(age, 21) between 90 and 110 as age_ok,
  ds_hll_count_distinct(dt, 21) = 1 as dt_ok
from t1;

-- query 8
USE ${case_db};
select
  ds_hll_count_distinct(id, 10, "HLL_4") between 90000 and 110000 as id_ok,
  ds_hll_count_distinct(province, 10, "HLL_4") between 90000 and 110000 as province_ok,
  ds_hll_count_distinct(age, 10, "HLL_4") between 90 and 110 as age_ok,
  ds_hll_count_distinct(dt, 10, "HLL_4") = 1 as dt_ok
from t1;

-- query 9
USE ${case_db};
select
  ds_hll_count_distinct(id, 10, "HLL_6") between 90000 and 110000 as id_ok,
  ds_hll_count_distinct(province, 10, "HLL_6") between 90000 and 110000 as province_ok,
  ds_hll_count_distinct(age, 10, "HLL_6") between 90 and 110 as age_ok,
  ds_hll_count_distinct(dt, 10, "HLL_6") = 1 as dt_ok
from t1;

-- query 10
USE ${case_db};
select
  ds_hll_count_distinct(id, 10, "HLL_8") between 90000 and 110000 as id_ok,
  ds_hll_count_distinct(province, 10, "HLL_8") between 90000 and 110000 as province_ok,
  ds_hll_count_distinct(age, 10, "HLL_8") between 90 and 110 as age_ok,
  ds_hll_count_distinct(dt, 10, "HLL_8") = 1 as dt_ok
from t1;

-- query 11
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 values (1, 'a', 1, '2024-07-22'), (2, 'b', 1, '2024-07-23'), (3, NULL, NULL, '2024-07-24');

-- query 12
USE ${case_db};
select
  id,
  province,
  ds_hll_count_distinct(id) = 1 as id_ok,
  ds_hll_count_distinct(province) = 1 as province_ok,
  ds_hll_count_distinct(age) = 1 as age_ok,
  ds_hll_count_distinct(dt) = 1 as dt_ok
from t1
group by 1, 2
order by 1, 2
limit 3;

-- query 13
USE ${case_db};
select
  id,
  province,
  ds_hll_count_distinct(id, 10) = 1 as id_ok,
  ds_hll_count_distinct(province, 10) = 1 as province_ok,
  ds_hll_count_distinct(age, 10) = 1 as age_ok,
  ds_hll_count_distinct(dt, 10) = 1 as dt_ok
from t1
group by 1, 2
order by 1, 2
limit 3;

-- query 14
USE ${case_db};
select
  id,
  province,
  ds_hll_count_distinct(id, 10, "HLL_4") = 1 as id_ok,
  ds_hll_count_distinct(province, 10, "HLL_4") = 1 as province_ok,
  ds_hll_count_distinct(age, 10, "HLL_4") = 1 as age_ok,
  ds_hll_count_distinct(dt, 10, "HLL_4") = 1 as dt_ok
from t1
group by 1, 2
order by 1, 2
limit 3;

-- query 15
-- @expect_error=ds_hll_count_distinct second parameter'value should be between 4 and 21.
USE ${case_db};
select ds_hll_count_distinct(id, 1)  from t1 order by 1, 2;

-- query 16
-- @expect_error=ds_hll_count_distinct second parameter'value should be between 4 and 21.
USE ${case_db};
select ds_hll_count_distinct(id, 100)  from t1 order by 1, 2;

-- query 17
-- @expect_error=ds_hll_count_distinct third  parameter'value should be in HLL_4/HLL_6/HLL_8.
USE ${case_db};
select ds_hll_count_distinct(id, 10, "INVALID") from t1 order by 1, 2;
