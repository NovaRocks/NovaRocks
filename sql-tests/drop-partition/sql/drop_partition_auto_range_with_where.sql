-- Test Objective:
-- 1. Validate DROP PARTITIONS WHERE on auto-partitioned range table (date_trunc expression).
-- 2. Verify various WHERE conditions: range, date_trunc, IS NULL, current_date().
-- 3. Covers NULL partition value insertion into expression-based range partition.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
    k1 date,
    k2 string,
    v1 int
)
PARTITION BY date_trunc('day', k1)
DISTRIBUTED BY HASH(k2) BUCKETS 3
PROPERTIES('replication_num' = '1');
INSERT INTO t1 VALUES
  ('2020-01-01','2020-02-02', 1), ('2020-01-02','2020-02-02', 2),
  ('2020-01-03','2020-02-03', 3), ('2020-01-04','2020-02-02', 4),
  ('2020-01-05','2020-02-03', 5), ('2020-01-06','2020-02-03', 6),
  (NULL, NULL, 10);

-- query 2
select count(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name != '$shadow_automatic_partition';

-- query 3
ALTER TABLE t1 DROP PARTITIONS WHERE k1 >= '2020-01-01' and k1 <= '2020-01-02';
select count(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name != '$shadow_automatic_partition';

-- query 4
ALTER TABLE t1 DROP PARTITIONS WHERE date_trunc('month', k1) = '2020-02-01';
select count(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name != '$shadow_automatic_partition';

-- query 5
ALTER TABLE t1 DROP PARTITIONS WHERE k1 >= '2020-01-05';
select count(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name != '$shadow_automatic_partition';

-- query 6
ALTER TABLE t1 DROP PARTITIONS WHERE k1 is null;
select count(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name != '$shadow_automatic_partition';

-- query 7
ALTER TABLE t1 DROP PARTITIONS WHERE k1 <= current_date();
select count(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name != '$shadow_automatic_partition';
