-- Migrated from dev/test/sql/test_function/T/test_encode_row_id
-- Test Objective:
-- 1. Validate ENCODE_ROW_ID() with various NULL and boundary inputs, verifying hex output via FROM_BINARY.
-- 2. Validate ENCODE_FINGERPRINT_SHA256() with NULL and boundary inputs.
-- 3. Validate ENCODE_SORT_KEY() with NULL and boundary inputs.
-- 4. Validate all three encoding functions on table columns (bigint, string, array<int>).
-- 5. Cover NULL placement at different argument positions.

-- query 1
-- Setup: create table for column-based tests
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1 (id int, v1 bigint, v2 string, v3 array<int>)
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES("replication_num" = "1");

-- query 2
-- ENCODE_ROW_ID with constant NULL args
USE ${case_db};
SELECT FROM_BINARY(ENCODE_ROW_ID(NULL), 'hex');

-- query 3
USE ${case_db};
SELECT FROM_BINARY(ENCODE_ROW_ID(NULL, NULL), 'hex');

-- query 4
USE ${case_db};
SELECT FROM_BINARY(ENCODE_ROW_ID(NULL, NULL, NULL), 'hex');

-- query 5
USE ${case_db};
SELECT FROM_BINARY(ENCODE_ROW_ID(NULL, NULL, NULL, NULL), 'hex');

-- query 6
USE ${case_db};
SELECT FROM_BINARY(ENCODE_ROW_ID('', 9223372036854775807), 'hex');

-- query 7
USE ${case_db};
SELECT FROM_BINARY(ENCODE_ROW_ID('', 9223372036854775807, NULL), 'hex');

-- query 8
USE ${case_db};
SELECT FROM_BINARY(ENCODE_ROW_ID('', -9223372036854775807, 465254298), 'hex');

-- query 9
USE ${case_db};
SELECT FROM_BINARY(ENCODE_ROW_ID('', 9223372036854775806, 465254298, NULL), 'hex');

-- query 10
USE ${case_db};
SELECT FROM_BINARY(ENCODE_ROW_ID('', 9223372036854775807, NULL), 'hex');

-- query 11
USE ${case_db};
SELECT FROM_BINARY(ENCODE_ROW_ID('', 9223372036854775807, 465254298), 'hex');

-- query 12
USE ${case_db};
SELECT FROM_BINARY(ENCODE_ROW_ID('', -9223372036854775807, 465254298), 'hex');

-- query 13
USE ${case_db};
SELECT FROM_BINARY(ENCODE_ROW_ID('', 9223372036854775806, 465254298, NULL), 'hex');

-- query 14
-- ENCODE_FINGERPRINT_SHA256 with constant NULL args
USE ${case_db};
SELECT FROM_BINARY(ENCODE_FINGERPRINT_SHA256(NULL), 'hex');

-- query 15
USE ${case_db};
SELECT FROM_BINARY(ENCODE_FINGERPRINT_SHA256(NULL, NULL), 'hex');

-- query 16
USE ${case_db};
SELECT FROM_BINARY(ENCODE_FINGERPRINT_SHA256(NULL, NULL, NULL), 'hex');

-- query 17
USE ${case_db};
SELECT FROM_BINARY(ENCODE_FINGERPRINT_SHA256(NULL, NULL, NULL, NULL), 'hex');

-- query 18
USE ${case_db};
SELECT FROM_BINARY(ENCODE_FINGERPRINT_SHA256('', 9223372036854775807), 'hex');

-- query 19
USE ${case_db};
SELECT FROM_BINARY(ENCODE_FINGERPRINT_SHA256('', 9223372036854775807, NULL), 'hex');

-- query 20
USE ${case_db};
SELECT FROM_BINARY(ENCODE_FINGERPRINT_SHA256('', -9223372036854775807, 465254298), 'hex');

-- query 21
USE ${case_db};
SELECT FROM_BINARY(ENCODE_FINGERPRINT_SHA256('', 9223372036854775806, 465254298, NULL), 'hex');

-- query 22
-- ENCODE_SORT_KEY with constant NULL args
USE ${case_db};
SELECT FROM_BINARY(ENCODE_SORT_KEY(NULL), 'hex');

-- query 23
USE ${case_db};
SELECT FROM_BINARY(ENCODE_SORT_KEY(NULL, NULL), 'hex');

-- query 24
USE ${case_db};
SELECT FROM_BINARY(ENCODE_SORT_KEY(NULL, NULL, NULL), 'hex');

-- query 25
USE ${case_db};
SELECT FROM_BINARY(ENCODE_SORT_KEY(NULL, NULL, NULL, NULL), 'hex');

-- query 26
USE ${case_db};
SELECT FROM_BINARY(ENCODE_SORT_KEY('', 9223372036854775807), 'hex');

-- query 27
USE ${case_db};
SELECT FROM_BINARY(ENCODE_SORT_KEY('', 9223372036854775807, NULL), 'hex');

-- query 28
USE ${case_db};
SELECT FROM_BINARY(ENCODE_SORT_KEY('', -9223372036854775807, 465254298), 'hex');

-- query 29
USE ${case_db};
SELECT FROM_BINARY(ENCODE_SORT_KEY('', 9223372036854775806, 465254298, NULL), 'hex');

-- query 30
-- Insert data for column-based tests
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 VALUES(1, 9223372036854775807, 'STARROCKS', [1, 2, 3]), (2, -9223372036854775807, 'STARROCKS', [1, 2, 3]), (3, 9223372036854775806, 'STARROCKS', [1, 2, 3]);

-- query 31
-- ENCODE_ROW_ID on table columns
USE ${case_db};
SELECT FROM_BINARY(ENCODE_ROW_ID(v1), 'hex') FROM t1;

-- query 32
USE ${case_db};
SELECT FROM_BINARY(ENCODE_ROW_ID(v1, v2), 'hex') FROM t1;

-- query 33
USE ${case_db};
SELECT FROM_BINARY(ENCODE_ROW_ID(v1, v2, v3), 'hex') FROM t1;

-- query 34
USE ${case_db};
SELECT FROM_BINARY(ENCODE_ROW_ID(NULL, v1, v2), 'hex') FROM t1;

-- query 35
USE ${case_db};
SELECT FROM_BINARY(ENCODE_ROW_ID(NULL, v1, v2, v3), 'hex') FROM t1;

-- query 36
USE ${case_db};
SELECT FROM_BINARY(ENCODE_ROW_ID(v1, NULL, v2, v3), 'hex') FROM t1;

-- query 37
USE ${case_db};
SELECT FROM_BINARY(ENCODE_ROW_ID(v1, v2, NULL, v3), 'hex') FROM t1;

-- query 38
USE ${case_db};
SELECT FROM_BINARY(ENCODE_ROW_ID(v1, v2, v3, NULL), 'hex') FROM t1;

-- query 39
-- ENCODE_FINGERPRINT_SHA256 on table columns
USE ${case_db};
SELECT FROM_BINARY(ENCODE_FINGERPRINT_SHA256(v1), 'hex') FROM t1;

-- query 40
USE ${case_db};
SELECT FROM_BINARY(ENCODE_FINGERPRINT_SHA256(v1, v2), 'hex') FROM t1;

-- query 41
USE ${case_db};
SELECT FROM_BINARY(ENCODE_FINGERPRINT_SHA256(v1, v2, v3), 'hex') FROM t1;

-- query 42
USE ${case_db};
SELECT FROM_BINARY(ENCODE_FINGERPRINT_SHA256(NULL, v1, v2), 'hex') FROM t1;

-- query 43
USE ${case_db};
SELECT FROM_BINARY(ENCODE_FINGERPRINT_SHA256(NULL, v1, v2, v3), 'hex') FROM t1;

-- query 44
USE ${case_db};
SELECT FROM_BINARY(ENCODE_FINGERPRINT_SHA256(v1, NULL, v2, v3), 'hex') FROM t1;

-- query 45
USE ${case_db};
SELECT FROM_BINARY(ENCODE_FINGERPRINT_SHA256(v1, v2, NULL, v3), 'hex') FROM t1;

-- query 46
USE ${case_db};
SELECT FROM_BINARY(ENCODE_FINGERPRINT_SHA256(v1, v2, v3, NULL), 'hex') FROM t1;

-- query 47
-- ENCODE_SORT_KEY on table columns
USE ${case_db};
SELECT FROM_BINARY(ENCODE_SORT_KEY(v1), 'hex') FROM t1;

-- query 48
USE ${case_db};
SELECT FROM_BINARY(ENCODE_SORT_KEY(v1, v2), 'hex') FROM t1;

-- query 49
USE ${case_db};
SELECT FROM_BINARY(ENCODE_SORT_KEY(NULL, v1, v2), 'hex') FROM t1;

-- query 50
-- ENCODE_SORT_KEY with array column (unsupported, expects error)
-- @expect_error=unsupported argument type
USE ${case_db};
SELECT FROM_BINARY(ENCODE_SORT_KEY(v1, v2, v3), 'hex') FROM t1;

-- query 51
-- @expect_error=unsupported argument type
USE ${case_db};
SELECT FROM_BINARY(ENCODE_SORT_KEY(NULL, v1, v2, v3), 'hex') FROM t1;

-- query 52
-- @expect_error=unsupported argument type
USE ${case_db};
SELECT FROM_BINARY(ENCODE_SORT_KEY(v1, NULL, v2, v3), 'hex') FROM t1;

-- query 53
-- @expect_error=unsupported argument type
USE ${case_db};
SELECT FROM_BINARY(ENCODE_SORT_KEY(v1, v2, NULL, v3), 'hex') FROM t1;

-- query 54
-- @expect_error=unsupported argument type
USE ${case_db};
SELECT FROM_BINARY(ENCODE_SORT_KEY(v1, v2, v3, NULL), 'hex') FROM t1;
