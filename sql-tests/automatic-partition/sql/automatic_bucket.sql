-- @order_sensitive=true
-- Test Objective:
-- 1. Validate bucket_size property default, ALTER, and error handling.
-- 2. Validate mutable_bucket_num property and error handling.
-- 3. Validate write path with automatic bucket on expression/range/list partitions.
-- 4. Validate delete from automatic bucket table.
-- Migrated from: dev/test/sql/test_automatic_bucket/T/test_automatic_partition

-- query 1
-- Verify default bucket_size property
-- @skip_result_check=true
-- @result_contains="bucket_size" = "1073741824"
CREATE TABLE ${case_db}.t_bucket_default(k int) PROPERTIES("replication_num" = "1");
SHOW CREATE TABLE ${case_db}.t_bucket_default;

-- query 2
-- ALTER bucket_size to 1024 and verify
-- @skip_result_check=true
-- @result_contains="bucket_size" = "1024"
ALTER TABLE ${case_db}.t_bucket_default SET('bucket_size'='1024');
SHOW CREATE TABLE ${case_db}.t_bucket_default;

-- query 3
-- bucket_size=0 is allowed (reset to default)
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_bucket_zero(k int) PROPERTIES('bucket_size'='0', "replication_num" = "1");

-- query 4
-- bucket_size=-1 should error
-- @expect_error=Illegal bucket size
CREATE TABLE ${case_db}.t_bucket_neg(k int) PROPERTIES('bucket_size'='-1');

-- query 5
-- bucket_size=1024 valid, ALTER to 0 allowed
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_bucket_1024(k int) PROPERTIES('bucket_size'='1024', "replication_num" = "1");
ALTER TABLE ${case_db}.t_bucket_1024 SET('bucket_size'='0');

-- query 6
-- ALTER bucket_size=-1 should error
-- @expect_error=Illegal bucket size
ALTER TABLE ${case_db}.t_bucket_1024 SET('bucket_size'='-1');

-- query 7
-- ALTER bucket_size=2048 valid
-- @skip_result_check=true
ALTER TABLE ${case_db}.t_bucket_1024 SET('bucket_size'='2048');

-- query 8
-- mutable_bucket_num=2 property
-- @skip_result_check=true
-- @result_contains="mutable_bucket_num" = "2"
CREATE TABLE ${case_db}.t_mutable(k date, v int) PARTITION BY DATE_TRUNC('DAY', k)
PROPERTIES ("replication_num" = "1", "bucket_size" = "1", "mutable_bucket_num" = "2");
SHOW CREATE TABLE ${case_db}.t_mutable;

-- query 9
-- ALTER mutable_bucket_num to 3
-- @skip_result_check=true
-- @result_contains="mutable_bucket_num" = "3"
ALTER TABLE ${case_db}.t_mutable SET('mutable_bucket_num'='3');
SHOW CREATE TABLE ${case_db}.t_mutable;

-- query 10
-- @expect_error=Illegal mutable bucket num
ALTER TABLE ${case_db}.t_mutable SET('mutable_bucket_num'='-1');

-- query 11
-- @expect_error=Mutable bucket num
ALTER TABLE ${case_db}.t_mutable SET('mutable_bucket_num'='a');

-- query 12
-- Write and verify mutable bucket table data
INSERT INTO ${case_db}.t_mutable VALUES('2021-01-01', 1);
INSERT INTO ${case_db}.t_mutable VALUES('2021-01-01', 1);
SELECT * FROM ${case_db}.t_mutable ORDER BY k, v;

-- query 13
-- Expression partition with bucket_size=1
CREATE TABLE ${case_db}.t_expr(k date, v int) PARTITION BY DATE_TRUNC('DAY', k)
PROPERTIES ("replication_num" = "1", "bucket_size" = "1");
INSERT INTO ${case_db}.t_expr VALUES('2021-01-01', 1);
INSERT INTO ${case_db}.t_expr VALUES('2021-01-03', 1);
INSERT INTO ${case_db}.t_expr VALUES('2021-01-05', 1);
INSERT INTO ${case_db}.t_expr VALUES('2021-01-01', 1);
SELECT * FROM ${case_db}.t_expr ORDER BY k, v;

-- query 14
-- Range partition with bucket_size=1
CREATE TABLE ${case_db}.t_range(k date, v int) PARTITION BY RANGE(k)
(PARTITION p20210101 VALUES [("2021-01-01"), ("2021-01-02")),
PARTITION p20210102 VALUES [("2021-01-02"), ("2021-01-03")),
PARTITION p20210103 VALUES [("2021-01-03"), ("2021-01-04")),
PARTITION p20210104 VALUES [("2021-01-04"), ("2021-01-05")),
PARTITION p20210105 VALUES [("2021-01-05"), ("2021-01-06")))
PROPERTIES ("replication_num" = "1", "bucket_size" = "1");
INSERT INTO ${case_db}.t_range VALUES('2021-01-01', 1);
INSERT INTO ${case_db}.t_range VALUES('2021-01-03', 1);
INSERT INTO ${case_db}.t_range VALUES('2021-01-05', 1);
INSERT INTO ${case_db}.t_range VALUES('2021-01-01', 1);
SELECT * FROM ${case_db}.t_range ORDER BY k, v;

-- query 15
-- List partition with bucket_size=1
CREATE TABLE ${case_db}.t_list(k date not null, v int) PARTITION BY LIST(k)
(PARTITION p20210101 VALUES IN ("2021-01-01"),
PARTITION p20210102 VALUES IN ("2021-01-02"),
PARTITION p20210103 VALUES IN ("2021-01-03"),
PARTITION p20210104 VALUES IN ("2021-01-04"),
PARTITION p20210105 VALUES IN ("2021-01-05"))
PROPERTIES ("replication_num" = "1", "bucket_size" = "1");
INSERT INTO ${case_db}.t_list VALUES('2021-01-01', 1);
INSERT INTO ${case_db}.t_list VALUES('2021-01-03', 1);
INSERT INTO ${case_db}.t_list VALUES('2021-01-05', 1);
INSERT INTO ${case_db}.t_list VALUES('2021-01-01', 1);
SELECT * FROM ${case_db}.t_list ORDER BY k, v;

-- query 16
-- Delete all rows from auto-bucket table
CREATE TABLE ${case_db}.t_del(k int, v int)
PROPERTIES ("replication_num" = "1", "bucket_size" = "1");
INSERT INTO ${case_db}.t_del VALUES(1,1);
INSERT INTO ${case_db}.t_del VALUES(1,2);
INSERT INTO ${case_db}.t_del VALUES(1,3);
INSERT INTO ${case_db}.t_del VALUES(1,4);
INSERT INTO ${case_db}.t_del VALUES(1,5);
SELECT * FROM ${case_db}.t_del ORDER BY k, v;

-- query 17
-- After delete, table should be empty
DELETE FROM ${case_db}.t_del WHERE k = 1;
SELECT COUNT(*) FROM ${case_db}.t_del;
