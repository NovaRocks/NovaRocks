-- @order_sensitive=true
-- Test Objective:
-- 1. Validate that first-load statistics collection works with expression-partitioned tables.
-- 2. Verify partition-level stats (column_name, partition_name, row_count, max, min) are correct.
-- Note: This case relies on enable_statistic_collect_on_first_load=true (default).

-- query 1
-- @skip_result_check=true
admin set frontend config('enable_statistic_collect_on_first_load'='true');
CREATE TABLE ${case_db}.test_first_load (
    event_day datetime,
    k1 int
) PARTITION BY date_trunc('day', event_day)
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO test_first_load SELECT '2020-01-01', generate_series FROM TABLE(generate_series(1,3000000));

-- query 2
select column_name, partition_name, row_count, max, min from _statistics_.column_statistics where table_name = '${case_db}.test_first_load' order by column_name;

-- query 3
-- @skip_result_check=true
drop stats test_first_load;
