-- Migrated from: dev/test/sql/test_fast_schema_evolution/T/test_fast_schema_evolution (test_meta_scan)
-- Test Objective:
-- 1. Validate ALTER TABLE ADD COLUMN with default value remains visible to SHOW CREATE TABLE.
-- 2. Preserve meta-scan dict_merge behavior after adding a defaulted column.
-- 3. Validate analyze + grouped query still work for both legacy fast_schema_evolution declarations.
-- Notes:
-- - The legacy dev/test helper `manual_compact(...)` is replaced with SQL `ALTER TABLE ... COMPACT`.
-- - Current NovaRocks SHOW CREATE TABLE normalizes cloud-native properties, so we assert only stable fragments.

-- query 1
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS reproducex4;
CREATE TABLE `reproducex4` (
  `id_int` int(11) NULL COMMENT "",
  `v1` varchar(255) NULL COMMENT ""
)
DUPLICATE KEY(`id_int`, `v1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id_int`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "storage_format" = "DEFAULT",
  "enable_persistent_index" = "true",
  "fast_schema_evolution" = "true"
);
INSERT INTO reproducex4 VALUES (1,2),(3,4),(5,6);
ALTER TABLE reproducex4 ADD COLUMN v2 varchar(256) DEFAULT "-1000" AFTER v1;

-- query 2
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=FINISHED
-- @skip_result_check=true
USE ${case_db};
SHOW ALTER TABLE COLUMN FROM ${case_db} WHERE TableName = 'reproducex4' ORDER BY CreateTime DESC LIMIT 1;

-- query 3
-- @result_contains=`v2` varchar(256) NULL DEFAULT "-1000" COMMENT ""
-- @result_contains="cloud_native_fast_schema_evolution_v2" = "true"
-- @result_contains="replication_num" = "1"
-- @skip_result_check=true
USE ${case_db};
SHOW CREATE TABLE reproducex4;

-- query 4
-- @skip_result_check=true
USE ${case_db};
INSERT INTO reproducex4 VALUES (7,8,9);
ALTER TABLE reproducex4 COMPACT;

-- query 5
USE ${case_db};
SELECT dict_merge(v2, 255) FROM reproducex4 [_META_];

-- query 6
USE ${case_db};
ANALYZE FULL TABLE reproducex4;

-- query 7
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=SUCCESS
-- @skip_result_check=true
USE ${case_db};
SHOW ANALYZE STATUS WHERE `Database` = 'default_catalog.${case_db}' AND `Table` = 'reproducex4' ORDER BY StartTime DESC LIMIT 1;

-- query 8
-- @order_sensitive=true
USE ${case_db};
SELECT v2, count(*) FROM reproducex4 GROUP BY v2 ORDER BY v2;

-- query 9
-- @skip_result_check=true
USE ${case_db};
DROP TABLE reproducex4;
CREATE TABLE `reproducex4` (
  `id_int` int(11) NULL COMMENT "",
  `v1` varchar(255) NULL COMMENT ""
)
DUPLICATE KEY(`id_int`, `v1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id_int`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "storage_format" = "DEFAULT",
  "enable_persistent_index" = "true",
  "fast_schema_evolution" = "false"
);
INSERT INTO reproducex4 VALUES (1,2),(3,4),(5,6);
ALTER TABLE reproducex4 ADD COLUMN v2 varchar(256) DEFAULT "-1000" AFTER v1;

-- query 10
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=FINISHED
-- @skip_result_check=true
USE ${case_db};
SHOW ALTER TABLE COLUMN FROM ${case_db} WHERE TableName = 'reproducex4' ORDER BY CreateTime DESC LIMIT 1;

-- query 11
-- @result_contains=`v2` varchar(256) NULL DEFAULT "-1000" COMMENT ""
-- @result_contains="replication_num" = "1"
-- @skip_result_check=true
USE ${case_db};
SHOW CREATE TABLE reproducex4;

-- query 12
-- @skip_result_check=true
USE ${case_db};
INSERT INTO reproducex4 VALUES (7,8,9);
ALTER TABLE reproducex4 COMPACT;

-- query 13
USE ${case_db};
SELECT dict_merge(v2, 255) FROM reproducex4 [_META_];

-- query 14
USE ${case_db};
ANALYZE FULL TABLE reproducex4;

-- query 15
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=SUCCESS
-- @skip_result_check=true
USE ${case_db};
SHOW ANALYZE STATUS WHERE `Database` = 'default_catalog.${case_db}' AND `Table` = 'reproducex4' ORDER BY StartTime DESC LIMIT 1;

-- query 16
-- @order_sensitive=true
USE ${case_db};
SELECT v2, count(*) FROM reproducex4 GROUP BY v2 ORDER BY v2;
