-- Test Objective:
-- 1. Validate DROP PARTITIONS WHERE with complex expressions (date arithmetic, last_day).
-- 2. Verify partition count decreases correctly after conditional drops.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.tbl_ttl_expr (
    dt datetime,
    province string,
    num int
)
DUPLICATE KEY(dt, province)
PARTITION BY date_trunc('day', dt), province
PROPERTIES ("replication_num" = "1");
INSERT INTO tbl_ttl_expr (dt, province, num)
SELECT minutes_add(hours_add(date_add('2025-01-01', x), x%24), x%60), concat('x-', x%3), x
FROM TABLE(generate_series(0, 200-1)) as t(x);

-- query 2
select COUNT(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name = 'tbl_ttl_expr' and partition_name != '$shadow_automatic_partition';

-- query 3
-- @skip_result_check=true
ALTER TABLE tbl_ttl_expr DROP PARTITIONS WHERE date_trunc('day', dt) < '2025-05-30' - INTERVAL 3 MONTH;

-- query 4
select COUNT(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name = 'tbl_ttl_expr' and partition_name != '$shadow_automatic_partition';

-- query 5
-- @skip_result_check=true
ALTER TABLE tbl_ttl_expr DROP PARTITIONS WHERE last_day(date_trunc('day', dt)) != date_trunc('day', dt);

-- query 6
select COUNT(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name = 'tbl_ttl_expr' and partition_name != '$shadow_automatic_partition';

-- query 7
-- @skip_result_check=true
ALTER TABLE tbl_ttl_expr DROP PARTITIONS WHERE date_trunc('day', dt) < '2025-05-30' - INTERVAL 2 MONTH AND last_day(date_trunc('day', dt)) != date_trunc('day', dt);

-- query 8
select COUNT(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name = 'tbl_ttl_expr' and partition_name != '$shadow_automatic_partition';
