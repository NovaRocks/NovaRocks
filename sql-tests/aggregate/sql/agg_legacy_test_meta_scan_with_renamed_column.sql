-- @order_sensitive=true
-- @tags=aggregate,legacy-migration
-- Migrated from dev/test/sql/test_agg/R/test_meta_scan_agg
-- query 1
DROP DATABASE IF EXISTS sql_tests_test_meta_scan_with_renamed_column FORCE;
CREATE DATABASE sql_tests_test_meta_scan_with_renamed_column;
USE sql_tests_test_meta_scan_with_renamed_column;
create table t2 (
    c0 INT
) DUPLICATE key (c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');

-- query 2
USE sql_tests_test_meta_scan_with_renamed_column;
insert into t2 values (1), (2), (3);

-- query 3
USE sql_tests_test_meta_scan_with_renamed_column;
select count(*) from t2[_META_];

-- query 4
USE sql_tests_test_meta_scan_with_renamed_column;
select count(*) from t2;

-- query 5
USE sql_tests_test_meta_scan_with_renamed_column;
alter table t2 rename column c0 to c1;

-- query 6
USE sql_tests_test_meta_scan_with_renamed_column;
select count(*) from t2[_META_];

-- query 7
USE sql_tests_test_meta_scan_with_renamed_column;
select count(*) from t2;

-- query 8
USE sql_tests_test_meta_scan_with_renamed_column;
alter table t2 rename column c1 to c2;

-- query 9
USE sql_tests_test_meta_scan_with_renamed_column;
select count(*) from t2[_META_];

-- query 10
USE sql_tests_test_meta_scan_with_renamed_column;
select count(*) from t2;
