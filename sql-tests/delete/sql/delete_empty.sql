-- Test Objective:
-- 1. Validate DELETE on empty partitioned table does not error.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.test_delete (
    `stat_date` varchar(65533) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`stat_date`)
PARTITION BY (`stat_date`)
DISTRIBUTED BY HASH(`stat_date`)
PROPERTIES (
    "replication_num" = "1",
    "enable_persistent_index" = "true",
    "replicated_storage" = "true",
    "compression" = "LZ4"
);
DELETE FROM test_delete WHERE stat_date = '2023-12-19';
