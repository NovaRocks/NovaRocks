-- Migrated from dev/test/sql/test_ddl/T/test_alter_pk_reorder
-- Test Objective:
-- 1. Validate ALTER TABLE ORDER BY on PRIMARY KEY table changes the sort key order.
-- 2. Validate that queries by k2 (new sort key) work correctly after reorder.
-- 3. Validate error cases: DOUBLE and FLOAT columns cannot be sort keys in PRIMARY KEY table.
-- 4. Validate ALTER TABLE ORDER BY with a valid VARCHAR column succeeds.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.tab1;
CREATE TABLE ${case_db}.tab1 (
    k1 INTEGER,
    k2 INTEGER,
    v1 INTEGER,
    v2 INTEGER,
    v3 INTEGER
) ENGINE=OLAP
PRIMARY KEY(`k1`, `k2`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES ("replication_num" = "1");
INSERT INTO ${case_db}.tab1 VALUES (100,100,100,100,100);
INSERT INTO ${case_db}.tab1 VALUES (200,200,200,200,200);
INSERT INTO ${case_db}.tab1 VALUES (300,300,300,300,300);
INSERT INTO ${case_db}.tab1 VALUES (400,400,400,400,400);
INSERT INTO ${case_db}.tab1 VALUES (500,500,500,500,500);
INSERT INTO ${case_db}.tab1 VALUES (600,600,600,600,600);

-- query 2
-- @order_sensitive=true
SELECT * FROM ${case_db}.tab1 WHERE k1 = 100;

-- query 3
-- @skip_result_check=true
ALTER TABLE ${case_db}.tab1 ORDER BY (k2, k1);

-- query 4
-- Wait for ORDER BY schema change to finish
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=FINISHED
-- @skip_result_check=true
SHOW ALTER TABLE COLUMN FROM ${case_db} ORDER BY CreateTime DESC LIMIT 1;

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.tab1 VALUES (700,700,700,700,700);

-- query 6
-- @order_sensitive=true
-- @retry_count=10
-- @retry_interval_ms=500
SELECT * FROM ${case_db}.tab1 ORDER BY k1;

-- query 7
-- @order_sensitive=true
SELECT * FROM ${case_db}.tab1 WHERE k2 = 100;

-- query 8
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.test_alter_pk_reorder_column_type;
CREATE TABLE ${case_db}.test_alter_pk_reorder_column_type (
  `k1` date NOT NULL COMMENT "",
  `v1` datetime NOT NULL COMMENT "",
  `v2` varchar(20) NOT NULL COMMENT "",
  `v3` varchar(20) NOT NULL COMMENT "",
  `v4` double NOT NULL COMMENT "",
  `v5` float NOT NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(`k1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`k1`) BUCKETS 3
ORDER BY(`v2`)
PROPERTIES ("replication_num" = "1");

-- query 9
-- DOUBLE column is not allowed as sort key in PRIMARY KEY table
-- @expect_error=type not supported
ALTER TABLE ${case_db}.test_alter_pk_reorder_column_type ORDER BY (v3, v4);

-- query 10
-- FLOAT column is not allowed as sort key in PRIMARY KEY table
-- @expect_error=type not supported
ALTER TABLE ${case_db}.test_alter_pk_reorder_column_type ORDER BY (v3, v5);

-- query 11
-- @skip_result_check=true
-- VARCHAR column is allowed as sort key
ALTER TABLE ${case_db}.test_alter_pk_reorder_column_type ORDER BY (v3);
