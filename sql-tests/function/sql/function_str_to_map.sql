-- Migrated from dev/test/sql/test_function/T/test_str_to_map
-- Test Objective:
-- 1. Validate str_to_map() correctly parses string values into maps.
-- 2. Validate cardinality of resulting maps over a large dataset (10000 rows).
-- 3. Ensure sum of cardinalities matches expected total.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1(c1 INT, c2 STRING)
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- query 2
-- @skip_result_check=true
USE ${case_db};
insert into t1 select generate_series, generate_series from TABLE(generate_series(1, 10000));

-- query 3
USE ${case_db};
select sum(cardinality(str_to_map(c2, ",", ":"))) from t1;
