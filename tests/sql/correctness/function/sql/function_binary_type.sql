-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- Migrated from dev/test/sql/test_binary_type
-- Preserve exact VARBINARY bytes and VARCHAR encoding through inserts and table copies.
-- Integer encodings verify the fixed eight-byte representation and hexadecimal display.

-- query 1
-- Create a table with binary and character columns.
-- @skip_result_check=true
CREATE TABLE ${case_db}.t0(c0 INT, c1 VARBINARY, c2 VARBINARY, c3 VARBINARY, c4 varchar(16)) WITH (format_version = 3);

-- query 2
-- Insert one row using hex literals
-- @skip_result_check=true
INSERT INTO ${case_db}.t0 VALUES ROW(1, X'ab01', X'abab', X'abac', 'ab01');

-- query 3
-- VARBINARY stores the exact inserted bytes and lengths.
SELECT to_hex(c1), to_hex(c2), to_hex(c3), to_hex(to_utf8(c4)),
       length(c1) AS c1_bytes, length(c2) AS c2_bytes, length(c3) AS c3_bytes
FROM ${case_db}.t0 ORDER BY c0;

-- query 4
-- Insert second row; both rows should appear
-- @skip_result_check=true
INSERT INTO ${case_db}.t0 VALUES ROW(2, X'ab01', X'abab', X'abac', 'ab01');

-- query 5
SELECT to_hex(c1), to_hex(c2), to_hex(c3), to_hex(to_utf8(c4)) FROM ${case_db}.t0 ORDER BY c0;

-- query 6
-- Create t1 with the same schema; insert via SELECT *
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1(c0 INT, c1 VARBINARY, c2 VARBINARY, c3 VARBINARY, c4 varchar(16)) WITH (format_version = 3);

-- query 7
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 SELECT * FROM ${case_db}.t0;

-- query 8
-- t1 should have same rows as t0
SELECT to_hex(c1), to_hex(c2), to_hex(c3), to_hex(to_utf8(c4)) FROM ${case_db}.t1 ORDER BY c0;

-- query 9
-- Create t2 for binary-ingest test
-- @skip_result_check=true
CREATE TABLE ${case_db}.t2(c1 int, c2 varbinary) WITH (format_version = 3);

-- query 10
-- Insert (1, '1234') into the varbinary column via the hex literal
-- x'31323334' (the four ASCII bytes of "1234"). Equivalent to the
-- stream_load JSON path the original StarRocks test used; both
-- routes deposit the same four raw bytes in c2.
-- @skip_result_check=true
INSERT INTO ${case_db}.t2 VALUES ROW(1, X'31323334');

-- query 11
SELECT * FROM ${case_db}.t2;

-- query 12
SELECT typeof(to_big_endian_64(BIGINT '26')) AS bytes_type,
       length(to_big_endian_64(BIGINT '26')) AS byte_count,
       to_hex(to_big_endian_64(BIGINT '26')) AS padded_hex;

-- query 13
SELECT ordinal, n, to_hex(to_big_endian_64(n)) AS padded_hex,
       CASE WHEN n = 0 THEN '0'
            ELSE regexp_replace(to_hex(to_big_endian_64(n)), '^0+', '')
       END AS hex_display
FROM (VALUES
    (1, BIGINT '0'),
    (2, BIGINT '26'),
    (3, BIGINT '-1'),
    (4, BIGINT '-9223372036854775808'),
    (5, BIGINT '9223372036854775807'),
    (6, CAST(NULL AS BIGINT))
) AS t(ordinal, n)
ORDER BY ordinal;
