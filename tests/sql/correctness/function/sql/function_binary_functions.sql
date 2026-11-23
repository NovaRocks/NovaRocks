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

-- Hex, Base64, UTF-8 and digest operations consume the original byte carrier.

-- query 1
SELECT to_hex(from_hex('aB')) AS hex_bytes,
       to_hex(from_base64('qw==')) AS base64_bytes,
       to_hex(to_utf8('ab')) AS utf8_bytes,
       to_hex(to_utf8('é')) AS multibyte_utf8;

-- query 2
SELECT from_utf8(to_utf8('ab')) AS utf8_roundtrip,
       to_base64(from_hex('ab')) AS base64_roundtrip,
       to_hex(from_base64('/wA=')) AS raw_bytes;

-- query 3
SELECT length(from_hex('')) AS hex_bytes,
       length(from_base64('')) AS base64_bytes,
       length(to_utf8('')) AS utf8_bytes,
       length(to_base64(X'')) AS base64_chars,
       crc32(X'') AS empty_crc32,
       crc32(CAST(NULL AS VARBINARY)) AS null_crc32;

-- query 4
-- Omitted Base64 padding and noncanonical unused bits are accepted by Trino.
SELECT to_hex(from_base64('/w')) AS short_base64,
       to_hex(from_base64('/x==')) AS unused_bits;

-- query 5
SELECT to_hex(md5(X'FF00')) AS md5_hex,
       to_hex(sha256(X'FF00')) AS sha256_hex,
       to_hex(sha512(X'FF00')) AS sha512_hex,
       typeof(md5(X'FF00')) AS md5_type,
       typeof(sha256(X'FF00')) AS sha256_type,
       typeof(sha512(X'FF00')) AS sha512_type,
       crc32(X'FF00') AS crc;

-- query 6
SELECT from_utf8(X'41C0AFEDA080FF42') AS default_replacement,
       from_utf8(X'41C0AFEDA080FF42', '') AS deleted,
       from_utf8(X'41C0AFEDA080FF42', '中') AS text_replacement,
       from_utf8(X'41C0AFEDA080FF42', BIGINT '128512') AS codepoint_replacement,
       to_hex(to_utf8(CAST('é中' AS CHAR(4)))) AS padded_utf8;

-- query 7
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT from_hex('1');

-- query 8
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT from_hex('XY');

-- query 9
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT from_base64('AA=');

-- query 10
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT from_utf8(X'FF', BIGINT '-1');

-- query 11
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT crc32('é');

-- query 12
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT md5('é');

-- query 13
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT to_base64('é');

-- query 14
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT to_binary('ab', 'hex');

-- query 15
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT from_binary(X'AB', 'hex');

-- query 16
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT HEX(BIGINT '26');

-- query 17
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT UNHEX('ab');

-- query 18
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT sha2('ab', 256);

-- query 19
-- @skip_result_check=true
CREATE TABLE ${case_db}.t0(c0 INT, c1 VARBINARY, c2 VARCHAR(16)) WITH (format_version = 3);

-- query 20
-- @skip_result_check=true
INSERT INTO ${case_db}.t0 VALUES
    ROW(1, X'AB01', 'ab01'),
    ROW(2, X'ABAB', 'abab'),
    ROW(3, X'FF00', '/wA='),
    ROW(4, X'', ''),
    ROW(5, NULL, NULL);

-- query 21
SELECT c0, length(c1) AS byte_count, to_hex(c1) AS raw_hex,
       to_hex(to_utf8(from_utf8(c1))) AS decoded_hex,
       to_base64(c1) AS base64, to_hex(from_base64(c2)) AS decoded_base64,
       crc32(c1) AS crc
FROM ${case_db}.t0
ORDER BY c0;

-- query 22
WITH prepared AS (
    SELECT c0, from_base64(c2) AS decoded, c1 AS raw FROM ${case_db}.t0 WHERE c0 >= 3
)
SELECT c0, decoded = raw AS exact_bytes, crc32(decoded) AS crc,
       to_hex(md5(decoded)) AS digest_hex
FROM prepared
ORDER BY c0;
