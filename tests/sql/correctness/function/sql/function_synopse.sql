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

-- Migrated from dev/test/sql/test_function/T/test_synopse
-- Test Objective:
-- 1. Validate bar() function renders a horizontal bar chart string for a value within a range.
-- 2. Validate width_bucket() assigns values into equal-width histogram buckets.
-- 3. Both functions tested over a generate_series range from 0 to 10.

-- query 1
select r, bar(CAST(r AS DOUBLE) / 10e0, BIGINT '20') as x from table(sequence(0, 10)) as s(r);

-- query 2
select r, width_bucket(CAST(r AS DOUBLE), 0e0, 10e0, BIGINT '20') as x from table(sequence(0, 10)) as s(r);

-- query 3
SELECT typeof(url_extract_host('http://a.test')) AS host_type,
       typeof(url_extract_parameter('http://a.test?q=x', 'q')) AS parameter_type,
       typeof(url_encode('中')) AS encoded_type,
       typeof(url_decode('%41')) AS decoded_type,
       typeof(url_extract_port('http://a.test:80')) AS port_type;

-- query 4
SELECT url_extract_parameter('/p?%71=first+name&q=second%26name', 'q') AS raw_q,
       url_extract_parameter('/p?%71=first+name&q=second%26name', '%71') AS raw_percent_key,
       url_extract_host('HTTP://ExAmPle.test:80/a/../b') AS host,
       url_extract_port('HTTP://ExAmPle.test:80/a/../b') AS port,
       url_extract_path('HTTP://ExAmPle.test:80/a/../b') AS path;

-- query 5
WITH inputs(uri) AS (VALUES CAST('http://a.test?q=x' AS VARCHAR(32)))
SELECT typeof(url_extract_host(uri)) AS host_type,
       typeof(url_extract_parameter(uri, 'q')) AS parameter_type,
       typeof(url_encode(uri)) AS encoded_type,
       typeof(url_decode(uri)) AS decoded_type,
       url_extract_host(uri) AS host,
       url_extract_parameter(uri, 'q') AS parameter
FROM inputs;
