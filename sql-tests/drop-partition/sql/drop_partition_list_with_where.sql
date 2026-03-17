-- Test Objective:
-- 1. Validate DROP PARTITIONS WHERE on list auto-partitioned table.
-- 2. Verify various WHERE conditions: LIKE, str2date, range, IS NULL.
-- 3. Validate DROP PARTITIONS WHERE on multi-column date_trunc partitioned table.

-- query 1
CREATE TABLE ${case_db}.t1 (
    dt varchar(20),
    province string,
    num int
)
PARTITION BY dt, province;
INSERT INTO t1 VALUES
    ("2020-07-01", "beijing", 1), ("2020-07-01", "chengdu", 2),
    ("2020-07-02", "beijing", 3), ("2020-07-02", "hangzhou", 4),
    ("2020-07-03", "chengdu", 1), ("2020-07-04", "hangzhou", 1),
    (NULL, NULL, 10);
select COUNT(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name != '$shadow_automatic_partition';

-- query 2
ALTER TABLE t1 DROP PARTITIONS WHERE province like '%chengdu%';
select COUNT(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name != '$shadow_automatic_partition';

-- query 3
ALTER TABLE t1 DROP PARTITIONS WHERE str2date(dt, '%Y-%m-%d') = '2020-07-01' AND province = 'beijing';
select COUNT(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name != '$shadow_automatic_partition';

-- query 4
ALTER TABLE t1 DROP PARTITIONS WHERE dt >= '2020-07-03';
select COUNT(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name != '$shadow_automatic_partition';

-- query 5
ALTER TABLE t1 DROP PARTITIONS WHERE dt is null;
select COUNT(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name != '$shadow_automatic_partition';

-- query 6
ALTER TABLE t1 DROP PARTITIONS WHERE dt >= '2020-07-01';
select COUNT(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name != '$shadow_automatic_partition';

-- query 7
-- @skip_result_check=true
drop table t1;
create table ${case_db}.t1(
    k1 datetime,
    k2 datetime,
    v int
) partition by date_trunc('day', k1), date_trunc('month', k2);
insert into t1 values
  ('2020-01-01','2020-02-02', 1), ('2020-01-02','2020-02-02', 2),
  ('2020-01-03','2020-02-03', 3), ('2020-01-04','2020-02-02', 4),
  ('2020-01-05','2020-02-03', 5), ('2020-01-06','2020-02-03', 6),
  (NULL, NULL, 10);

-- query 8
select COUNT(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name != '$shadow_automatic_partition';

-- query 9
ALTER TABLE t1 DROP PARTITIONS WHERE date_trunc('day', k1) = '2020-01-01';
select COUNT(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name != '$shadow_automatic_partition';

-- query 10
ALTER TABLE t1 DROP PARTITIONS WHERE date_trunc('month', k2) = '2020-01-01';
select COUNT(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name != '$shadow_automatic_partition';

-- query 11
ALTER TABLE t1 DROP PARTITIONS WHERE date_trunc('month', k2) = '2020-01-01' or date_trunc('day', k1) = '2020-01-02';
select COUNT(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name != '$shadow_automatic_partition';

-- query 12
ALTER TABLE t1 DROP PARTITIONS WHERE date_trunc('month', k2) = '2020-02-01';
select COUNT(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name != '$shadow_automatic_partition';

-- query 13
ALTER TABLE t1 DROP PARTITIONS WHERE date_trunc('day', k1) is null;
select COUNT(1) from INFORMATION_SCHEMA.PARTITIONS_META where db_name = '${case_db}' and table_name like '%t1%' and partition_name != '$shadow_automatic_partition';
