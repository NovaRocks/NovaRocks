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
-- @sequential=true
-- @order_sensitive=true

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.dictionary_output (label STRING) WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.dictionary_output VALUES 'blue','red','blue','red','red',NULL;

-- query 3
-- @be_log_contains=NOVAROCKS_CONNECTOR_SCAN_FIRST_CHUNK utf8_offsets=1 utf8_view=0 utf8_dictionary=0
SELECT label, count(*) AS n FROM ${case_db}.dictionary_output GROUP BY label ORDER BY label NULLS LAST;

-- query 4
-- @iceberg_scan_layout=view
-- @be_log_contains=NOVAROCKS_CONNECTOR_SCAN_FIRST_CHUNK utf8_offsets=0 utf8_view=1 utf8_dictionary=0
SELECT label, count(*) AS n FROM ${case_db}.dictionary_output GROUP BY label ORDER BY label NULLS LAST;

-- query 5
-- @iceberg_dictionary_output=true
-- @be_log_contains=NOVAROCKS_CONNECTOR_SCAN_FIRST_CHUNK utf8_offsets=0 utf8_view=0 utf8_dictionary=1
SELECT label, count(*) AS n FROM ${case_db}.dictionary_output GROUP BY label ORDER BY label NULLS LAST;

-- query 6
-- @iceberg_scan_layout=view
-- @iceberg_dictionary_output=true
-- @be_log_contains=NOVAROCKS_CONNECTOR_SCAN_FIRST_CHUNK utf8_offsets=0 utf8_view=0 utf8_dictionary=1
SELECT label, count(*) AS n FROM ${case_db}.dictionary_output GROUP BY label ORDER BY label NULLS LAST;

-- query 7
-- @skip_result_check=true
CREATE TABLE ${case_db}.dictionary_plain_fallback (label STRING) WITH (format_version = 3);

-- query 8
-- @skip_result_check=true
INSERT INTO ${case_db}.dictionary_plain_fallback SELECT rpad(CAST(generate_series AS VARCHAR), 16384, 'x') FROM TABLE(generate_series(1, 512));

-- query 9
-- @iceberg_scan_layout=view
-- @iceberg_dictionary_output=true
-- @be_log_contains=NOVAROCKS_CONNECTOR_SCAN_FIRST_CHUNK utf8_offsets=0 utf8_view=1 utf8_dictionary=0
SELECT count(label) AS n, sum(length(label)) AS total FROM ${case_db}.dictionary_plain_fallback;
