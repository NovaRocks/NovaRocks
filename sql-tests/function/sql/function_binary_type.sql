-- Migrated from dev/test/sql/test_binary_type
-- Test Objective:
-- 1. Validate BINARY(n) column type: padded storage, hex output.
-- 2. Validate BINARY (no length), VARBINARY, VARCHAR(n) in same table.
-- 3. Validate INSERT and SELECT hex() across binary column types.
-- 4. Validate INSERT INTO ... SELECT * across tables with matching binary schema.
-- 5. Validate INSERT of hex literal into varbinary column (equivalent
--    to the previous stream_load JSON path, which is not supported in
--    standalone-server).

-- query 1
-- Create table with BINARY(16), BINARY, VARBINARY, VARCHAR columns
-- @skip_result_check=true
CREATE TABLE ${case_db}.t0(c0 INT, c1 binary(16), c2 binary, c3 varbinary, c4 varchar(16))
        DUPLICATE KEY(c0)
        DISTRIBUTED BY HASH(c0)
        BUCKETS 1
        PROPERTIES('replication_num'='1');

-- query 2
-- Insert one row using hex literals
-- @skip_result_check=true
INSERT INTO ${case_db}.t0 VALUES (1, x'ab01', x'abab', x'abac', 'ab01');

-- query 3
-- BINARY(16) pads to 16 bytes; BINARY pads to 1 byte; VARBINARY stores exact bytes
SELECT hex(c1), hex(c2), hex(c3), hex(c4) FROM ${case_db}.t0 ORDER BY c0;

-- query 4
-- Insert second row; both rows should appear
-- @skip_result_check=true
INSERT INTO ${case_db}.t0 VALUES (2, x'ab01', x'abab', x'abac', 'ab01');

-- query 5
SELECT hex(c1), hex(c2), hex(c3), hex(c4) FROM ${case_db}.t0 ORDER BY c0;

-- query 6
-- Create t1 with the same schema; insert via SELECT *
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1(c0 INT, c1 binary(16), c2 binary, c3 varbinary, c4 varchar(16))
        DUPLICATE KEY(c0)
        DISTRIBUTED BY HASH(c0)
        BUCKETS 1
        PROPERTIES('replication_num'='1');

-- query 7
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 SELECT * FROM ${case_db}.t0;

-- query 8
-- t1 should have same rows as t0
SELECT hex(c1), hex(c2), hex(c3), hex(c4) FROM ${case_db}.t1 ORDER BY c0;

-- query 9
-- Create t2 for binary-ingest test
-- @skip_result_check=true
CREATE TABLE ${case_db}.t2(c1 int, c2 varbinary) DUPLICATE KEY(c1) PROPERTIES("replication_num"="1");

-- query 10
-- Insert (1, '1234') into the varbinary column via the hex literal
-- x'31323334' (the four ASCII bytes of "1234"). Equivalent to the
-- stream_load JSON path the original StarRocks test used; both
-- routes deposit the same four raw bytes in c2.
-- @skip_result_check=true
INSERT INTO ${case_db}.t2 VALUES (1, x'31323334');

-- query 11
SELECT * FROM ${case_db}.t2;
