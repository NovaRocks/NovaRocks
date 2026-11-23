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

-- @order_sensitive=true
-- @tags=project,string,regexp
-- query 1
SELECT regexp_extract('foo=123', 'bar=([0-9]+)', 1) AS re_no_match,
       regexp_extract(NULL, 'x', 1) AS re_null_input;

-- query 2
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT from_hex('ZZ');

-- query 3
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT from_hex('F');

-- query 4
SELECT to_hex(to_utf8(NORMALIZE(U&'e\0301'))) AS nfc_bytes,
       typeof(chr(128512)) AS chr_type,
       length(CAST('a' AS CHAR(3))) AS char_length_value,
       to_hex(to_utf8(CAST('a' AS CHAR(3)))) AS char_bytes;

-- query 5
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT NORMALIZE(U&'e\0301', nfd);

-- query 6
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT codepoint(CAST('a' AS VARCHAR));
