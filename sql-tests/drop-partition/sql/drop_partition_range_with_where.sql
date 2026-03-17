-- Test Objective:
-- 1. Validate DROP PARTITIONS WHERE on range partitioned table.
-- 2. Verify error when using non-partition column in WHERE.
-- 3. Verify WHERE with str2date, range comparison, IS NULL.

-- query 1
CREATE TABLE ${case_db}.t1 (
    dt date,
    province string,
    v1 int
)
PARTITION BY RANGE(dt)
(
    PARTITION p0 values [('2020-07-01'),('2020-07-02')),
    PARTITION p1 values [('2020-07-02'),('2020-07-03')),
    PARTITION p2 values [('2020-07-03'),('2020-07-04')),
    PARTITION p3 values [('2020-07-04'),('2020-07-05'))
)
DISTRIBUTED BY HASH(dt) BUCKETS 3
PROPERTIES('replication_num' = '1');
INSERT INTO t1 VALUES
    ("2020-07-01", "beijing", 1), ("2020-07-01", "chengdu", 2),
    ("2020-07-02", "beijing", 3), ("2020-07-02", "hangzhou", 4),
    ("2020-07-03", "chengdu", 1), ("2020-07-04", "hangzhou", 1);
select COUNT(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name != '$shadow_automatic_partition';

-- query 2
-- @expect_error=is not a table's partition expression
ALTER TABLE t1 DROP PARTITIONS WHERE province like '%a%';

-- query 3
select COUNT(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name != '$shadow_automatic_partition';

-- query 4
-- @skip_result_check=true
ALTER TABLE t1 DROP PARTITIONS WHERE str2date(dt, '%Y-%m-%d') = '2020-07-07';

-- query 5
select COUNT(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name != '$shadow_automatic_partition';

-- query 6
ALTER TABLE t1 DROP PARTITIONS WHERE dt >= '2020-07-03';
select COUNT(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name != '$shadow_automatic_partition';

-- query 7
ALTER TABLE t1 DROP PARTITIONS WHERE dt is null;
select COUNT(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name != '$shadow_automatic_partition';

-- query 8
ALTER TABLE t1 DROP PARTITIONS WHERE dt >= '2020-07-01';
select COUNT(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name != '$shadow_automatic_partition';
