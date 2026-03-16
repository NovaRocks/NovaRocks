-- Migrated from dev/test/sql/test_binary_functions
-- Test Objective:
-- 1. Validate to_binary() with HEX/ENCODE64/UTF8 format (case-insensitive).
-- 2. Validate from_binary() roundtrip conversions.
-- 3. Cover bad input cases (odd-length hex, invalid base64, empty strings).
-- 4. Validate to_binary/from_binary on varbinary column data.

-- query 1
-- to_binary with default (no explicit format); result format may vary, skip check
-- @skip_result_check=true
select to_binary('ab');

-- query 2
-- HEX format (uppercase)
select to_binary('ab', 'HEX');

-- query 3
-- HEX format (lowercase)
select to_binary('ab', 'hex');

-- query 4
-- ENCODE64 format (uppercase)
select to_binary('qw==', 'ENCODE64');

-- query 5
-- ENCODE64 format (lowercase)
select to_binary('qw==', 'encode64');

-- query 6
-- UTF8 format (lowercase)
select to_binary('ab', 'utf8');

-- query 7
-- UTF8 format (uppercase)
select to_binary('ab', 'UTF8');

-- query 8
-- from_binary roundtrip: utf8 -> utf8
select from_binary(to_binary('ab', 'utf8'), 'utf8');

-- query 9
-- from_binary roundtrip: hex -> hex (result is uppercase)
select from_binary(to_binary('ab', 'hex'), 'hex');

-- query 10
-- from_binary: decode hex-encoded binary to base64
select from_binary(to_binary('ab', 'hex'), 'encode64');

-- query 11
-- Bad input: empty string with hex returns empty binary (non-NULL, but value can't be snapshot)
-- @skip_result_check=true
select to_binary('', 'hex');

-- query 12
-- Bad input: empty string with utf8 returns empty binary (non-NULL, but value can't be snapshot)
-- @skip_result_check=true
select to_binary('', 'utf8');

-- query 13
-- Bad input: empty string with encode64 (invalid base64 -> NULL)
select to_binary('', 'encode64');

-- query 14
-- Bad input: odd-length hex string
select to_binary('1', 'hex');

-- query 15
-- Bad input: invalid hex characters XY
select to_binary('XY', 'hex');

-- query 16
-- Bad input: invalid hex characters XYZ0
select to_binary('XYZ0', 'hex');

-- query 17
-- Bad input: invalid base64 padding
select to_binary('1', 'encode64');

-- query 18
-- Bad input: from_binary on null-producing hex
select from_binary(to_binary('X', 'hex'), 'hex');

-- query 19
select from_binary(to_binary('0X', 'hex'), 'hex');

-- query 20
select from_binary(to_binary('1', 'encode64'), 'hex');

-- query 21
-- Create table with varbinary column
-- @skip_result_check=true
CREATE TABLE ${case_db}.t0(c0 INT, c1 varbinary(16), c2 varchar(16))
        DUPLICATE KEY(c0)
        DISTRIBUTED BY HASH(c0)
        BUCKETS 1
        PROPERTIES('replication_num'='1');

-- query 22
-- Insert one row (x'ab01' is binary literal)
-- @skip_result_check=true
INSERT INTO ${case_db}.t0 VALUES (1, x'ab01', 'ab01');

-- query 23
-- from_binary on varbinary column vs reconstructed binary
SELECT from_binary(c1, 'hex'), from_binary(to_binary(c2, 'hex'), 'hex') FROM ${case_db}.t0;

-- query 24
SELECT hex(from_binary(c1, 'utf8')), from_binary(to_binary(c2, 'encode64'), 'hex') FROM ${case_db}.t0;

-- query 25
SELECT from_binary(c1, 'encode64'), from_binary(to_binary(c2, 'utf8'), 'hex') FROM ${case_db}.t0;

-- query 26
-- Insert second row
-- @skip_result_check=true
INSERT INTO ${case_db}.t0 VALUES (2, x'abab', 'abab');

-- query 27
SELECT from_binary(c1, 'hex'), from_binary(to_binary(c2, 'hex'), 'hex') FROM ${case_db}.t0 ORDER BY c0;

-- query 28
SELECT hex(from_binary(c1, 'utf8')), from_binary(to_binary(c2, 'encode64'), 'hex') FROM ${case_db}.t0 ORDER BY c0;

-- query 29
SELECT from_binary(c1, 'encode64'), from_binary(to_binary(c2, 'utf8'), 'hex') FROM ${case_db}.t0 ORDER BY c0;
