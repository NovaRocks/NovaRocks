-- Test Objective:
-- 1. Validate analyze full/sample multiple columns creates multi_column_statistics.
-- 2. Verify drop multiple columns stats clears records.
-- 3. Verify drop stats also clears multi_column_statistics.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1(c1 int, c2 bigint, c3 string, c4 string)
PROPERTIES('replication_num'='1');
INSERT INTO t1 VALUES (1, 1, 's1', 's1');
INSERT INTO t1 VALUES (2, 2, 's2', 's2');
INSERT INTO t1 VALUES (3, 3, 's3', 's3');
INSERT INTO t1 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,1000));
INSERT INTO t1 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,1000));
INSERT INTO t1 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,1000));
INSERT INTO t1 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,1000));
INSERT INTO t1 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,1000));

-- query 2
-- @skip_result_check=true
analyze full table t1 multiple columns (c1, c2);

-- query 3
select count(1) from _statistics_.multi_column_statistics where table_name = '${case_db}.t1' and column_names = 'c1,c2';

-- query 4
-- @skip_result_check=true
drop multiple columns stats t1;

-- query 5
select count(1) from _statistics_.multi_column_statistics where table_name = '${case_db}.t1' and column_names = 'c1,c2';

-- query 6
-- @skip_result_check=true
analyze sample table t1 multiple columns (c1, c2);

-- query 7
select count(1) from _statistics_.multi_column_statistics where table_name = '${case_db}.t1' and column_names = 'c1,c2';

-- query 8
-- @skip_result_check=true
drop stats t1;

-- query 9
select count(1) from _statistics_.multi_column_statistics where table_name = '${case_db}.t1' and column_names = 'c1,c2';

-- query 10
-- @skip_result_check=true
drop stats t1;
