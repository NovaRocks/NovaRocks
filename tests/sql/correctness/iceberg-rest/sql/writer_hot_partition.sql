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
-- One partition, about 469 MiB of input values, within the default 1 GB
-- target. More than one file proves that more than one writer received it.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_rest_${suite_uuid0}.rwm13_hot_${uuid0};
CREATE TABLE iceberg_rest_${suite_uuid0}.rwm13_hot_${uuid0}.t (p BIGINT, id BIGINT, payload VARCHAR)
PARTITION BY (p);
INSERT INTO iceberg_rest_${suite_uuid0}.rwm13_hot_${uuid0}.t
SELECT CAST(11 AS BIGINT), generate_series, repeat('x', 4096)
FROM TABLE(generate_series(1, 120000));

-- query 2
SELECT count(*) > 1 AS spread, sum(record_count) AS rows_in_files
FROM iceberg_rest_${suite_uuid0}.rwm13_hot_${uuid0}.t$files WHERE content = 0;

-- query 3
SELECT count(*) AS n, sum(id) AS id_sum, sum(length(payload)) AS payload_chars
FROM iceberg_rest_${suite_uuid0}.rwm13_hot_${uuid0}.t;

-- query 4
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS iceberg_rest_${suite_uuid0}.rwm13_hot_${uuid0}.t FORCE;
DROP DATABASE IF EXISTS iceberg_rest_${suite_uuid0}.rwm13_hot_${uuid0};
