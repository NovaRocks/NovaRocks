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
-- Validate the declaration and values of Iceberg REST's nanosecond timestamp.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA iceberg_rest_${suite_uuid0}.iceberg_rest_tsns_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_tsns_db_${uuid0}.t_tsns_${uuid0} (
  id BIGINT,
  ts TIMESTAMP(9)
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_tsns_db_${uuid0}.t_tsns_${uuid0}
VALUES
  ROW(1, TIMESTAMP '2024-01-02 03:04:05.123456789'),
  ROW(2, TIMESTAMP '2024-01-02 03:04:05.000000001'),
  ROW(3, TIMESTAMP '1970-01-01 00:00:00.000000000');

-- query 4
SELECT id, ts
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_tsns_db_${uuid0}.t_tsns_${uuid0}
  ORDER BY id;

-- query 5
-- Nanosecond range predicate pushdown: count rows where ts > '2024-01-02 03:04:05.000000001'.
-- Row 1 has ts = .123456789 (passes), row 2 has ts = .000000001 (boundary, excluded by >),
-- row 3 is epoch (excluded). Correct nanosecond semantics yield COUNT=1.
-- If the predicate bound were rounded to microseconds (.000000), rows 1 and 2 would
-- both pass (COUNT=2), proving the nanosecond path is exercised.
SELECT COUNT(*) AS cnt
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_tsns_db_${uuid0}.t_tsns_${uuid0}
  WHERE ts > TIMESTAMP '2024-01-02 03:04:05.000000001';

-- query 6
-- Narrowing to TIMESTAMP(6) rounds once at the requested precision.
SELECT id, CAST(ts AS TIMESTAMP(6)) AS micros
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_tsns_db_${uuid0}.t_tsns_${uuid0}
  ORDER BY id;

-- query 7
-- Fail-fast guard: a time-based partition transform on a nanosecond timestamp
-- column must error rather than silently mis-derive partitions (IV3-7.1).
-- @expect_error=nanosecond
CREATE TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_tsns_db_${uuid0}.t_tsns_part_${uuid0} (
  id BIGINT,
  ts TIMESTAMP(9)
) WITH (partitioning = ARRAY['day(ts)'], format_version = 3);

-- query 8
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_tsns_db_${uuid0}.t_tsns_${uuid0};

-- query 9
-- @skip_result_check=true
DROP SCHEMA iceberg_rest_${suite_uuid0}.iceberg_rest_tsns_db_${uuid0};
