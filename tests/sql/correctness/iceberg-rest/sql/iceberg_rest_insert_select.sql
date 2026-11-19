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
-- Validate REST appendData / overwriteFiles commit round-trips:
-- - INSERT INTO ... VALUES (append)
-- - INSERT INTO ... SELECT (append from query)
-- - INSERT OVERWRITE (full overwrite)
-- Positive assertion after each commit: SELECT count + rows ordered by pk.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_rest_${suite_uuid0}.iceberg_rest_io_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_io_db_${uuid0}.t_io_${uuid0} (
  id BIGINT,
  region STRING,
  amount DOUBLE
)
PARTITION BY (region);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_io_db_${uuid0}.t_io_${uuid0}
VALUES (1, 'us', 10.5), (2, 'us', 20.0), (3, 'eu', 30.25);

-- query 4
-- After 3 rows inserted: count = 3.
SELECT COUNT() AS n FROM iceberg_rest_${suite_uuid0}.iceberg_rest_io_db_${uuid0}.t_io_${uuid0};

-- query 5
-- Rows ordered by id.
SELECT id, region, amount
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_io_db_${uuid0}.t_io_${uuid0}
  ORDER BY id;

-- query 6
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_io_db_${uuid0}.t_io_${uuid0}
SELECT id + 100 AS id, region, amount * 2 AS amount
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_io_db_${uuid0}.t_io_${uuid0}
  WHERE id <= 2;

-- query 7
-- After INSERT...SELECT (2 more rows): count = 5.
SELECT COUNT() AS n FROM iceberg_rest_${suite_uuid0}.iceberg_rest_io_db_${uuid0}.t_io_${uuid0};

-- query 8
-- @skip_result_check=true
INSERT OVERWRITE iceberg_rest_${suite_uuid0}.iceberg_rest_io_db_${uuid0}.t_io_${uuid0}
VALUES (999, 'ap', 0.0), (998, 'ap', 1.0);

-- query 9
-- After INSERT OVERWRITE: count = 2.
SELECT COUNT() AS n FROM iceberg_rest_${suite_uuid0}.iceberg_rest_io_db_${uuid0}.t_io_${uuid0};

-- query 10
-- INSERT OVERWRITE replaced everything; only the two new rows remain.
SELECT id, region, amount
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_io_db_${uuid0}.t_io_${uuid0}
  ORDER BY id;

-- query 11
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_io_db_${uuid0}.t_io_${uuid0};

-- query 12
-- @skip_result_check=true
DROP DATABASE iceberg_rest_${suite_uuid0}.iceberg_rest_io_db_${uuid0};
