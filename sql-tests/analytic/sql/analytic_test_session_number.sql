-- Migrated from: dev/test/sql/test_window_function/T/test_session_number_window_function
-- Test Objective:
-- 1. Validate session_number() window function with various gap sizes and partition/order combinations.
-- 2. Cover null-valued gap argument, column-valued gap, and null input rows.
-- 3. Validate session_number on empty table.
-- 4. Validate aggregate (sum/count) of session numbers.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t0 (
    c0 INT,
    c1 BIGINT
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 PROPERTIES('replication_num' = '1');
INSERT INTO ${case_db}.t0 SELECT generate_series, 4096 - generate_series FROM TABLE(generate_series(1, 4096, 3));
INSERT INTO ${case_db}.t0 SELECT generate_series, generate_series FROM TABLE(generate_series(1, 4096, 2));

-- query 2
-- @order_sensitive=true
SELECT c0, c1, session_number(c0, 4) OVER (PARTITION BY c1 ORDER BY c0) FROM ${case_db}.t0 ORDER BY c1, c0 LIMIT 10;

-- query 3
-- @order_sensitive=true
SELECT c0, c1, session_number(c0, 2) OVER (PARTITION BY c1 ORDER BY c0) FROM ${case_db}.t0 ORDER BY c1, c0 LIMIT 10;

-- query 4
-- @order_sensitive=true
SELECT c0, c1, session_number(c0, 1) OVER (PARTITION BY c1 ORDER BY c0) FROM ${case_db}.t0 ORDER BY c1, c0 LIMIT 10;

-- query 5
-- @order_sensitive=true
SELECT c0, c1, session_number(null, null) OVER (PARTITION BY c1 ORDER BY c0) FROM ${case_db}.t0 ORDER BY c1, c0 LIMIT 10;

-- query 6
-- @order_sensitive=true
SELECT c0, c1, session_number(c0, 2) OVER (PARTITION BY c1 ORDER BY c0) FROM ${case_db}.t0 ORDER BY c1, c0 LIMIT 10;

-- query 7
-- session_number with column delta (non-constant) not tested: NovaRocks requires constant delta parameter.
SELECT sum(session), count(session) FROM (SELECT c0, c1, session_number(c0, 2) OVER (PARTITION BY c1 ORDER BY c0) AS session FROM ${case_db}.t0) tb;

-- query 8
-- @skip_result_check=true
CREATE TABLE ${case_db}.empty_tbl LIKE ${case_db}.t0;

-- query 9
-- @order_sensitive=true
SELECT c0, c1, session_number(c0, 2) OVER (PARTITION BY c1 ORDER BY c0) FROM ${case_db}.empty_tbl;

-- query 10
-- @order_sensitive=true
SELECT c0, c1, session_number(c0, null) OVER (PARTITION BY c1 ORDER BY c0) FROM ${case_db}.empty_tbl;

-- query 11
-- @skip_result_check=true
INSERT INTO ${case_db}.t0 SELECT generate_series, null FROM TABLE(generate_series(1, 4096, 2));

-- query 12
-- @order_sensitive=true
SELECT c0, c1, session_number(c0, 2) OVER (PARTITION BY c1 ORDER BY c0) AS session FROM ${case_db}.t0 ORDER BY c1, c0 LIMIT 10;

-- query 13
SELECT sum(session), count(session) FROM (SELECT c0, c1, session_number(c0, 2) OVER (PARTITION BY c1 ORDER BY c0) AS session FROM ${case_db}.t0) tb;
