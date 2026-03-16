-- Migrated from dev/test/sql/test_condition_update/T/test_condition_update
-- Test Objective:
-- 1. Validate merge_condition rejects primary key column (Fail, error message contains "should not be primary key").
-- 2. Validate merge_condition rejects non-existent column (Fail, error message contains "does not exist").
-- 3. Validate merge_condition:v1 stream_load updates row only when new v1 > existing v1.
-- 4. Validate partial_update + merge_condition with column not in partial columns → Fail.
-- 5. Validate partial_update + merge_condition:v1 updates row only when new v1 > existing v1.
-- 6. Validate INSERT PROPERTIES("merge_condition"="v1"): no update when new value < existing.
-- 7. Validate INSERT PROPERTIES("merge_condition"="v1"): update when new value > existing.
-- 8. Validate merge_condition with expr_partition table: only highest modified_at row wins.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.tab1;
CREATE TABLE ${case_db}.tab1 (
      k1 INTEGER,
      k2 VARCHAR(50),
      v1 INTEGER,
      v2 INTEGER,
      v3 INTEGER,
      v4 varchar(50),
      v5 varchar(50)
)
ENGINE=OLAP
PRIMARY KEY(`k1`,`k2`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO ${case_db}.tab1 VALUES (100, "k2_100", 100, 100, 100, "v4_100", "v5_100");
INSERT INTO ${case_db}.tab1 VALUES (200, "k2_200", 200, 200, 200, "v4_200", "v5_200");
INSERT INTO ${case_db}.tab1 VALUES (300, "k3_300", 300, 300, 300, "v4_300", "v5_300");

-- query 2
-- @order_sensitive=true
SELECT * FROM ${case_db}.tab1 ORDER BY k1;

-- query 3
-- merge_condition:k1 must fail because k1 is a primary key column
-- @result_contains=should not be primary key
shell: NO_PROXY=127.0.0.1,localhost curl -s --location-trusted -u ${cluster.user}: --data-binary "100,k2_100,111,100,100,v4_100,v5_100" -XPUT -H "Expect:100-continue" -H "label:cond_update_1_${uuid0}" -H "column_separator:," -H "merge_condition:k1" ${url}/api/${case_db}/tab1/_stream_load

-- query 4
-- @order_sensitive=true
-- Row 100 should be unchanged (stream_load was rejected)
-- @retry_count=5
-- @retry_interval_ms=200
SELECT * FROM ${case_db}.tab1 ORDER BY k1;

-- query 5
-- merge_condition:v6 must fail because v6 does not exist in the table
-- @result_contains=does not exist
shell: NO_PROXY=127.0.0.1,localhost curl -s --location-trusted -u ${cluster.user}: --data-binary "100,k2_100,111,100,100,v4_100,v5_100" -XPUT -H "Expect:100-continue" -H "label:cond_update_2_${uuid1}" -H "column_separator:," -H "merge_condition:v6" ${url}/api/${case_db}/tab1/_stream_load

-- query 6
-- @order_sensitive=true
-- Still unchanged after failed merge_condition:v6
-- @retry_count=5
-- @retry_interval_ms=200
SELECT * FROM ${case_db}.tab1 ORDER BY k1;

-- query 7
-- merge_condition:v1, new v1=111 > old v1=100 → row 100 should be updated
-- @result_contains="Status":"Success"
shell: NO_PROXY=127.0.0.1,localhost curl -s --location-trusted -u ${cluster.user}: --data-binary "100,k2_100,111,100,100,v4_100,v5_100" -XPUT -H "Expect:100-continue" -H "label:cond_update_3_${uuid2}" -H "column_separator:," -H "merge_condition:v1" ${url}/api/${case_db}/tab1/_stream_load

-- query 8
-- @order_sensitive=true
-- Row 100: v1 updated to 111; rows 200, 300 unchanged
-- @retry_count=10
-- @retry_interval_ms=300
SELECT * FROM ${case_db}.tab1 ORDER BY k1;

-- query 9
-- partial_update with columns:k1,k2,v1 and merge_condition:v2 (v2 not in partial columns) → Fail
-- @result_contains=does not exist
shell: NO_PROXY=127.0.0.1,localhost curl -s --location-trusted -u ${cluster.user}: --data-binary "$(printf '100,k2_100,100\n200,k2_200,222')" -XPUT -H "Expect:100-continue" -H "label:cond_update_4_${uuid3}" -H "column_separator:," -H "merge_condition:v2" -H "partial_update:true" -H "columns:k1,k2,v1" ${url}/api/${case_db}/tab1/_stream_load

-- query 10
-- @order_sensitive=true
-- No change: partial update was rejected (merge_condition column v2 not in columns list)
-- @retry_count=5
-- @retry_interval_ms=200
SELECT * FROM ${case_db}.tab1 ORDER BY k1;

-- query 11
-- partial_update with columns:k1,k2,v1 and merge_condition:v1 → should succeed
-- Row 100: new v1=100 < old v1=111 → NO update
-- Row 200: new v1=222 > old v1=200 → UPDATE
-- @result_contains="Status":"Success"
shell: NO_PROXY=127.0.0.1,localhost curl -s --location-trusted -u ${cluster.user}: --data-binary "$(printf '100,k2_100,100\n200,k2_200,222')" -XPUT -H "Expect:100-continue" -H "label:cond_update_5_${uuid4}" -H "column_separator:," -H "merge_condition:v1" -H "partial_update:true" -H "columns:k1,k2,v1" ${url}/api/${case_db}/tab1/_stream_load

-- query 12
-- @order_sensitive=true
-- Row 100: v1=111 (100 < 111, no update); Row 200: v1=222 (222 > 200, updated)
-- @retry_count=10
-- @retry_interval_ms=300
SELECT * FROM ${case_db}.tab1 ORDER BY k1;

-- query 13
-- @skip_result_check=true
-- INSERT with merge_condition: new v1=200 < existing v1=300 → row 300 should NOT be updated
INSERT INTO ${case_db}.tab1 PROPERTIES("merge_condition" = "v1") VALUES (300, "k3_300", 200, 400, 400, "v4_400", "v5_400");

-- query 14
-- @order_sensitive=true
-- Row 300 retains v1=300 (insert rejected because 200 < 300)
SELECT * FROM ${case_db}.tab1 ORDER BY k1, k2;

-- query 15
-- @skip_result_check=true
-- INSERT with merge_condition: new v1=400 > existing v1=300 → row 300 SHOULD be updated
INSERT INTO ${case_db}.tab1 PROPERTIES("merge_condition" = "v1") VALUES (300, "k3_300", 400, 400, 400, "v4_400", "v5_400");

-- query 16
-- @order_sensitive=true
-- Row 300 updated to v1=400 (400 > 300)
SELECT * FROM ${case_db}.tab1 ORDER BY k1, k2;

-- query 17
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.expr_partition_tbl;
CREATE TABLE ${case_db}.expr_partition_tbl (
      org_id INT,
      id INT,
      v1 INT,
      modified_at INT
)
ENGINE=OLAP
PRIMARY KEY(`org_id`,`id`)
PARTITION BY (`org_id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

-- query 18
-- Two rows with same key (1,2): modified_at=1 and modified_at=0.
-- merge_condition:modified_at means only the row with modified_at=1 persists.
-- @result_contains="Status":"Success"
shell: NO_PROXY=127.0.0.1,localhost curl -s --location-trusted -u ${cluster.user}: -H "Expect:100-continue" -H "label:cond_update_expr_part_${uuid5}" -H "column_separator:," -H "row_delimiter:|" -H "merge_condition:modified_at" --data-binary "1,2,1,1|1,2,0,0" -XPUT ${url}/api/${case_db}/expr_partition_tbl/_stream_load

-- query 19
-- @order_sensitive=true
-- Only (1, 2, 1, 1) should exist (modified_at=1 > 0)
-- @retry_count=10
-- @retry_interval_ms=300
SELECT * FROM ${case_db}.expr_partition_tbl ORDER BY org_id, id;
