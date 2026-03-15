-- Test Objective:
-- 1. Validate ALTER TABLE MODIFY PARTITION (*) SET on a unique key table with storage_cooldown_ttl.
-- 2. Verify partition-level storage properties can be modified.
-- Migrated from: dev/test/sql/test_alter_table/T/test_alter_table_abnormal (test_alter_table_storage_ttl)

-- query 1
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `test` (
  `k1` date NULL COMMENT "",
  `k2` datetime NULL COMMENT "",
  `k3` varchar(65533) NULL COMMENT "",
  `k4` varchar(20) NULL COMMENT "",
  `k5` boolean NULL COMMENT "",
  `k6` tinyint(4) NULL COMMENT "",
  `k7` smallint(6) NULL COMMENT "",
  `k8` int(11) NULL COMMENT "",
  `k9` bigint(20) NULL COMMENT "",
  `k10` largeint(40) NULL COMMENT "",
  `k11` float NULL COMMENT "",
  `k12` double NULL COMMENT "",
  `k13` decimal(27, 9) NULL COMMENT ""
) ENGINE=OLAP
UNIQUE KEY(`k1`, `k2`, `k3`, `k4`, `k5`)
PARTITION BY time_slice(k2, 1, 'month', 'floor')
DISTRIBUTED BY HASH(`k1`, `k2`, `k3`)
PROPERTIES (
    "compression" = "LZ4",
    "fast_schema_evolution" = "true",
    "replicated_storage" = "true",
    "replication_num" = "1",
    "storage_cooldown_ttl" = "3 day",
    "storage_medium" = "SSD"
);
INSERT INTO test VALUES('2020-01-01', '2024-10-30 11:17:01', 'asfgrgte', 'wergergqer', 0, 11, 111, 1111, 11111, 111111, 11.11, 111.111, 1111.1111),('2020-01-01', '2024-10-30 10:17:01', 'asfgrgte', 'wergergqer', 0, 11, 111, 1111, 11111, 111111, 11.11, 111.111, 1111.1111),('2020-01-01', '2024-10-30 09:17:01', 'asfgrgte', 'wergergqer', 0, 11, 111, 1111, 11111, 111111, 11.11, 111.111, 1111.1111),('2020-01-01', '2024-10-30 12:17:01', 'asfgrgte', 'wergergqer', 0, 11, 111, 1111, 11111, 111111, 11.11, 111.111, 1111.1111);
ALTER TABLE test MODIFY PARTITION (*) SET ("storage_cooldown_ttl" = "1 year", "storage_medium" = "SSD");

-- query 2
-- @skip_result_check=true
