-- Test Objective:
-- 1. Verify shadow partition ($shadow_automatic_partition) exists on auto-partitioned table creation.
-- 2. Verify DROP PARTITIONS WHERE does not remove the shadow partition.
-- 3. Verify new data can still be inserted after dropping all user partitions.

-- query 1
CREATE TABLE ${case_db}.t1 (
    k1 date,
    k2 string,
    v1 int
)
PARTITION BY date_trunc('day', k1)
DISTRIBUTED BY HASH(k2) BUCKETS 3
PROPERTIES('replication_num' = '1');
select count(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%';

-- query 2
select count(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name = '$shadow_automatic_partition';

-- query 3
-- @skip_result_check=true
INSERT INTO t1 VALUES('2020-01-01','2020-02-02', 1);

-- query 4
select count(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%';

-- query 5
-- @skip_result_check=true
ALTER TABLE t1 DROP PARTITIONS WHERE k1 >= '2020-01-01';

-- query 6
select count(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%';

-- query 7
select count(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name = '$shadow_automatic_partition';

-- query 8
-- @skip_result_check=true
INSERT INTO t1 VALUES('2020-01-01','2020-02-02', 1);

-- query 9
select count(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%';
