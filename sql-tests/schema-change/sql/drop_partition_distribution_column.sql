-- Test Objective:
-- 1. Validate that partition columns cannot be dropped (LIST partition).
-- 2. Validate that partition columns cannot be dropped (auto date_trunc partition).
-- 3. Validate that distribution columns cannot be dropped.
-- Migrated from: dev/test/sql/test_alter_table/T/test_alter_table_abnormal (test_drop_partition_or_distribution_column)

-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sc_drop_part_dist_${uuid0} FORCE;
CREATE DATABASE sc_drop_part_dist_${uuid0};
USE sc_drop_part_dist_${uuid0};
CREATE TABLE t9 (
    c0 int(11) NULL COMMENT "",
    c1 int(11) NOT NULL COMMENT ""
)
DUPLICATE KEY(c0)
PARTITION BY LIST(c1)(
    PARTITION p0 VALUES IN ('0'),
    PARTITION p1 VALUES IN ('1')
)
DISTRIBUTED BY HASH(c0) BUCKETS 1
PROPERTIES ("fast_schema_evolution" = "true", "replication_num"="1");

-- query 2
-- Cannot drop partition column (LIST partition, fast_schema_evolution=true).
-- @expect_error=Partition column
USE sc_drop_part_dist_${uuid0};
ALTER TABLE t9 DROP COLUMN c1;

-- query 3
-- @skip_result_check=true
USE sc_drop_part_dist_${uuid0};
CREATE TABLE t10 (
    c0 int(11) NULL COMMENT "",
    c1 int(11) NOT NULL COMMENT ""
)
DUPLICATE KEY(c0)
PARTITION BY LIST(c1)(
    PARTITION p0 VALUES IN ('0'),
    PARTITION p1 VALUES IN ('1')
)
DISTRIBUTED BY HASH(c0) BUCKETS 1
PROPERTIES ("fast_schema_evolution" = "false", "replication_num"="1");

-- query 4
-- Cannot drop partition column (LIST partition, fast_schema_evolution=false).
-- @expect_error=Partition column
USE sc_drop_part_dist_${uuid0};
ALTER TABLE t10 DROP COLUMN c1;

-- query 5
-- @skip_result_check=true
USE sc_drop_part_dist_${uuid0};
CREATE TABLE site_access1 (
    event_day DATETIME NOT NULL,
    site_id INT DEFAULT '10',
    city_code VARCHAR(100),
    user_name VARCHAR(32) DEFAULT '',
    pv BIGINT DEFAULT '0'
)
DUPLICATE KEY(event_day, site_id, city_code, user_name)
PARTITION BY date_trunc('day', event_day)
DISTRIBUTED BY HASH(event_day, site_id)
PROPERTIES('replication_num'='1');

-- query 6
-- Cannot drop partition column (date_trunc partition).
-- @expect_error=Partition column
USE sc_drop_part_dist_${uuid0};
ALTER TABLE site_access1 DROP COLUMN event_day;

-- query 7
-- Cannot drop distribution column.
-- @expect_error=Distribution column
USE sc_drop_part_dist_${uuid0};
ALTER TABLE site_access1 DROP COLUMN site_id;

-- query 8
-- @skip_result_check=true
DROP DATABASE IF EXISTS sc_drop_part_dist_${uuid0} FORCE;
