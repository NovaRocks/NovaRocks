-- Migrated from dev/test/sql/test_optimize_table/T/test_optimize_table (test_optimize_table_by_partition_name)
-- Test Objective:
-- 1. Verify ALTER TABLE PARTITIONS(p1,p2,p3) can optimize all named partitions.
-- @sequential=true

-- query 1
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t(
    k1 date,
    k2 datetime,
    k3 char(20),
    k4 varchar(20),
    k5 boolean,
    k6 tinyint,
    k7 smallint,
    k8 int,
    k9 bigint,
    k10 largeint,
    k11 float,
    k12 double,
    k13 decimal(27,9)
) DUPLICATE KEY(k1, k2, k3, k4, k5)
PARTITION BY RANGE(k1)(
    PARTITION p202006 VALUES LESS THAN ("2020-07-01"),
    PARTITION p202007 VALUES LESS THAN ("2020-08-01"),
    PARTITION p202008 VALUES LESS THAN ("2020-09-01")
) DISTRIBUTED BY HASH(k1, k2, k3, k4, k5) BUCKETS 3
    PROPERTIES("replication_num" = "1");

-- query 2
-- @skip_result_check=true
-- @wait_alter_optimize=t
USE ${case_db};
ALTER TABLE t PARTITIONS(p202006,p202007,p202008) DISTRIBUTED BY HASH(k1, k2, k3, k4, k5) BUCKETS 4;
