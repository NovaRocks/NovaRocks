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
-- Each source file contains all three days. INSERT and CTAS must co-locate
-- each small partition through the exchange while preserving the row bag.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA iceberg_rest_${suite_uuid0}.rwm13_${uuid0};
CREATE TABLE iceberg_rest_${suite_uuid0}.rwm13_${uuid0}.src (ts TIMESTAMP, id BIGINT);
INSERT INTO iceberg_rest_${suite_uuid0}.rwm13_${uuid0}.src VALUES
  ROW(TIMESTAMP '2024-06-01 00:00:00', 1), ROW(TIMESTAMP '2024-06-02 00:00:00', 2), ROW(TIMESTAMP '2024-06-03 00:00:00', 3);
INSERT INTO iceberg_rest_${suite_uuid0}.rwm13_${uuid0}.src VALUES
  ROW(TIMESTAMP '2024-06-01 06:00:00', 4), ROW(TIMESTAMP '2024-06-02 06:00:00', 5), ROW(TIMESTAMP '2024-06-03 06:00:00', 6);
INSERT INTO iceberg_rest_${suite_uuid0}.rwm13_${uuid0}.src VALUES
  ROW(TIMESTAMP '2024-06-01 12:00:00', 7), ROW(TIMESTAMP '2024-06-02 12:00:00', 8), ROW(TIMESTAMP '2024-06-03 12:00:00', 9);
INSERT INTO iceberg_rest_${suite_uuid0}.rwm13_${uuid0}.src VALUES
  ROW(TIMESTAMP '2024-06-01 18:00:00', 10), ROW(TIMESTAMP '2024-06-02 18:00:00', 11), ROW(TIMESTAMP '2024-06-03 18:00:00', 12);
CREATE TABLE iceberg_rest_${suite_uuid0}.rwm13_${uuid0}.t (ts TIMESTAMP, id BIGINT) WITH (partitioning = ARRAY['day(ts)']);
INSERT INTO iceberg_rest_${suite_uuid0}.rwm13_${uuid0}.t
SELECT ts, id FROM iceberg_rest_${suite_uuid0}.rwm13_${uuid0}.src;
CREATE TABLE iceberg_rest_${suite_uuid0}.rwm13_${uuid0}.derived WITH (partitioning = ARRAY['day(ts)']) AS SELECT ts, id FROM iceberg_rest_${suite_uuid0}.rwm13_${uuid0}.src;

-- query 2
SELECT count(*) AS data_files
FROM iceberg_rest_${suite_uuid0}.rwm13_${uuid0}."t$files" WHERE content = 0;

-- query 3
SELECT count(*) AS partitions FROM iceberg_rest_${suite_uuid0}.rwm13_${uuid0}."t$partitions";

-- query 4
SELECT count(*) AS n, sum(id) AS id_sum FROM iceberg_rest_${suite_uuid0}.rwm13_${uuid0}.t;

-- query 5
SELECT count(*) AS data_files
FROM iceberg_rest_${suite_uuid0}.rwm13_${uuid0}."derived$files" WHERE content = 0;

-- query 6
SELECT count(*) AS partitions FROM iceberg_rest_${suite_uuid0}.rwm13_${uuid0}."derived$partitions";

-- query 7
SELECT count(*) AS n, sum(id) AS id_sum FROM iceberg_rest_${suite_uuid0}.rwm13_${uuid0}.derived;

-- query 8
SELECT count(*) AS n, sum(id) AS id_sum FROM iceberg_rest_${suite_uuid0}.rwm13_${uuid0}.src;

-- query 9
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS iceberg_rest_${suite_uuid0}.rwm13_${uuid0}.derived ;
DROP TABLE IF EXISTS iceberg_rest_${suite_uuid0}.rwm13_${uuid0}.t ;
DROP TABLE IF EXISTS iceberg_rest_${suite_uuid0}.rwm13_${uuid0}.src ;
DROP SCHEMA IF EXISTS iceberg_rest_${suite_uuid0}.rwm13_${uuid0};
