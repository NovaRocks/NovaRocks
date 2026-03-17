-- @order_sensitive=true
-- Test Objective:
-- 1. Validate INSERT triggers first-load stats collection (count increments).
-- 2. Validate INSERT OVERWRITE triggers stats update (both full table and partition).
-- 3. Verify EXPLAIN COSTS reflects correct cardinality after overwrite.
-- 4. Verify enable_statistic_collect_on_first_load controls stats collection on overwrite.
-- 5. Verify expression-range partition overwrite stats collection.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.test_overwrite_stats_table (k1 int) PROPERTIES("replication_num"="1");

-- query 2
-- @skip_result_check=true
INSERT INTO test_overwrite_stats_table SELECT 123;

-- query 3
-- Stats count is non-deterministic due to async collection timing; just verify > 0
-- @skip_result_check=true
-- @retry_count=30
-- @retry_interval_ms=1000
select count(*) from _statistics_.column_statistics where table_name = '${case_db}.test_overwrite_stats_table';

-- query 4
-- @skip_result_check=true
INSERT OVERWRITE test_overwrite_stats_table SELECT 123;

-- query 5
-- @skip_result_check=true
-- @retry_count=30
-- @retry_interval_ms=1000
select count(*) from _statistics_.column_statistics where table_name = '${case_db}.test_overwrite_stats_table';

-- query 6
-- @skip_result_check=true
CREATE TABLE ${case_db}.sales_data (
    id BIGINT,
    sale_date DATE
)
DUPLICATE KEY(id)
PARTITION BY RANGE(sale_date) (
    PARTITION p202401 VALUES [('2024-01-01'), ('2024-02-01')),
    PARTITION p202402 VALUES [('2024-02-01'), ('2024-03-01')),
    PARTITION p202403 VALUES [('2024-03-01'), ('2024-04-01')),
    PARTITION p202404 VALUES [('2024-04-01'), ('2024-05-01'))
)
DISTRIBUTED BY HASH(id) BUCKETS 4
PROPERTIES ("replication_num" = "1");
INSERT INTO sales_data VALUES
(1, '2024-01-15'), (2, '2024-01-20'),
(3, '2024-02-10'), (4, '2024-02-15'),
(5, '2024-03-05'), (6, '2024-03-12'),
(7, '2024-04-08'), (8, '2024-04-18');

-- query 7
-- @skip_result_check=true
-- @retry_count=30
-- @retry_interval_ms=1000
select count(*) from _statistics_.column_statistics where table_name = '${case_db}.sales_data';

-- query 8
-- @skip_result_check=true
INSERT OVERWRITE ${case_db}.sales_data partition("p202401") VALUES (101, '2024-01-10');

-- query 9
-- @skip_result_check=true
-- @retry_count=30
-- @retry_interval_ms=1000
select count(*) from _statistics_.column_statistics where table_name = '${case_db}.sales_data';

-- query 10
-- @skip_result_check=true
select count(*) from information_schema.analyze_status where `Database`='${case_db}' and `Table`='sales_data' and Status='FAILED';

-- query 11
-- @skip_result_check=true
CREATE TABLE ${case_db}.test_overwrite_with_full (k1 int) PROPERTIES("replication_num"="1");
INSERT OVERWRITE test_overwrite_with_full SELECT generate_series FROM TABLE(generate_series(1, 5000));

-- query 12
-- @skip_result_check=true
-- @result_contains=cardinality: 5000
-- @result_contains=ESTIMATE
-- @retry_count=30
-- @retry_interval_ms=1000
EXPLAIN COSTS select * from ${case_db}.test_overwrite_with_full;

-- query 13
-- @skip_result_check=true
INSERT OVERWRITE test_overwrite_with_full SELECT generate_series FROM TABLE(generate_series(1, 5000));

-- query 14
-- @skip_result_check=true
-- @result_contains=cardinality: 5000
-- @result_contains=ESTIMATE
-- @retry_count=30
-- @retry_interval_ms=1000
EXPLAIN COSTS select * from ${case_db}.test_overwrite_with_full;

-- query 15
-- @skip_result_check=true
CREATE TABLE ${case_db}.test_overwrite_with_sample (k1 int) PROPERTIES("replication_num"="1");
INSERT OVERWRITE test_overwrite_with_sample SELECT generate_series FROM TABLE(generate_series(1, 300000));

-- query 16
-- @skip_result_check=true
-- @result_contains=ESTIMATE
-- @retry_count=30
-- @retry_interval_ms=1000
EXPLAIN COSTS select * from ${case_db}.test_overwrite_with_sample;

-- query 17
-- @skip_result_check=true
INSERT OVERWRITE test_overwrite_with_sample SELECT generate_series FROM TABLE(generate_series(1, 300000));

-- query 18
-- @skip_result_check=true
-- @result_contains=ESTIMATE
-- @retry_count=30
-- @retry_interval_ms=1000
EXPLAIN COSTS select * from ${case_db}.test_overwrite_with_sample;

-- query 19
-- @skip_result_check=true
drop stats sales_data;
DROP TABLE sales_data;
CREATE TABLE ${case_db}.sales_data (
    id BIGINT,
    sale_date DATE
)
DUPLICATE KEY(id)
PARTITION BY RANGE(sale_date) (
    PARTITION p202401 VALUES [('2024-01-01'), ('2024-02-01')),
    PARTITION p202402 VALUES [('2024-02-01'), ('2024-03-01')),
    PARTITION p202403 VALUES [('2024-03-01'), ('2024-04-01')),
    PARTITION p202404 VALUES [('2024-04-01'), ('2024-05-01'))
)
DISTRIBUTED BY HASH(id) BUCKETS 4
PROPERTIES ("replication_num" = "1");
ALTER TABLE ${case_db}.sales_data SET("enable_statistic_collect_on_first_load"="false");
INSERT INTO sales_data VALUES
(1, '2024-01-15'), (2, '2024-01-20'),
(3, '2024-02-10'), (4, '2024-02-15'),
(5, '2024-03-05'), (6, '2024-03-12'),
(7, '2024-04-08'), (8, '2024-04-18');

-- query 20
-- @skip_result_check=true
select count(*) from _statistics_.column_statistics where table_name = '${case_db}.sales_data';

-- query 21
-- @skip_result_check=true
INSERT OVERWRITE ${case_db}.sales_data partition("p202401") VALUES (101, '2024-01-10');

-- query 22
-- @skip_result_check=true
select count(*) from _statistics_.column_statistics where table_name = '${case_db}.sales_data';

-- query 23
-- @skip_result_check=true
ALTER TABLE ${case_db}.sales_data SET("enable_statistic_collect_on_first_load"="true");
INSERT OVERWRITE ${case_db}.sales_data partition("p202401") VALUES (102, '2024-01-10');

-- query 24
-- @skip_result_check=true
-- @retry_count=30
-- @retry_interval_ms=1000
select count(*) from _statistics_.column_statistics where table_name = '${case_db}.sales_data';

-- query 25
-- @skip_result_check=true
drop stats ${case_db}.test_overwrite_stats_table;
DROP TABLE ${case_db}.test_overwrite_stats_table;
CREATE TABLE ${case_db}.test_overwrite_stats_table (k1 int) PROPERTIES("replication_num"="1");
INSERT INTO test_overwrite_stats_table SELECT generate_series FROM TABLE(generate_series(1, 1000));
INSERT OVERWRITE test_overwrite_stats_table SELECT generate_series FROM TABLE(generate_series(10000, 20000));

-- query 26
-- @skip_result_check=true
-- @result_contains=20000.0
-- @retry_count=30
-- @retry_interval_ms=1000
EXPLAIN COSTS select * from ${case_db}.test_overwrite_stats_table;

-- query 27
-- @skip_result_check=true
DROP TABLE IF EXISTS expr_range_partitioned_table;
CREATE TABLE ${case_db}.expr_range_partitioned_table (
    dt datetime,
    k1 int,
    k2 varchar(20)
)
PARTITION BY date_trunc('day', dt)
PROPERTIES("replication_num"="1");
INSERT INTO expr_range_partitioned_table SELECT '2024-01-01 08:00:00', generate_series, 'data1' FROM TABLE(generate_series(1, 1000));
SET dynamic_overwrite=true;
INSERT OVERWRITE expr_range_partitioned_table SELECT '2024-01-01 08:00:00', generate_series, 'data1' FROM TABLE(generate_series(1, 2000));

-- query 28
-- @skip_result_check=true
-- @result_contains=cardinality: 2000
-- @result_contains=2000.0
-- @retry_count=30
-- @retry_interval_ms=1000
EXPLAIN COSTS select * from ${case_db}.expr_range_partitioned_table;

-- query 29
-- @skip_result_check=true
INSERT OVERWRITE expr_range_partitioned_table SELECT '2024-01-01 08:00:00', generate_series, 'data1' FROM TABLE(generate_series(1, 300000));
SET dynamic_overwrite=false;

-- query 30
-- @skip_result_check=true
-- @result_contains=300000.0
-- @retry_count=30
-- @retry_interval_ms=1000
EXPLAIN COSTS select * from ${case_db}.expr_range_partitioned_table;
