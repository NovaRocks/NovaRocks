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
-- Actual persisted column types depend on the format version.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA iceberg_ddl_cat_${suite_uuid0}.trc6c_types_${uuid0};
USE iceberg_ddl_cat_${suite_uuid0}.trc6c_types_${uuid0};
CREATE TABLE temporal_v1 (event TIMESTAMP) WITH (format_version = 1);
INSERT INTO temporal_v1 VALUES ROW(TIMESTAMP '1969-12-31 23:59:59.999999');

-- query 2
SELECT event FROM temporal_v1;

-- query 3
-- @skip_result_check=true
CREATE TABLE temporal_v2 (event TIMESTAMP(9), clock TIME(4)) WITH (format_version = 2);
ALTER TABLE temporal_v2 ADD COLUMN zoned TIMESTAMP(12) WITH TIME ZONE;
INSERT INTO temporal_v2 VALUES ROW(
    TIMESTAMP '2024-01-01 00:00:00.123456789',
    TIME '12:34:56.1234',
    TIMESTAMP '2024-01-01 08:00:00.1234567894 Asia/Shanghai'
);

-- query 4
SELECT event, clock, zoned FROM temporal_v2;

-- query 5
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE TABLE temporal_zone_rejected (clock TIME(4) WITH TIME ZONE);

-- query 6
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ALTER TABLE temporal_v2 ADD COLUMN elapsed INTERVAL DAY TO SECOND;

-- query 7
-- @skip_result_check=true
CREATE TABLE temporal_defaults (id INTEGER) WITH (format_version = 3);
INSERT INTO temporal_defaults VALUES ROW(1);

-- query 8
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ALTER TABLE temporal_defaults ADD COLUMN zoned TIMESTAMP(12) WITH TIME ZONE
    DEFAULT '1969-12-31 23:59:59.9999999987 UTC';

-- query 9
-- @skip_result_check=true
ALTER TABLE temporal_defaults ADD COLUMN zoned TIMESTAMP(12) WITH TIME ZONE
    DEFAULT '2024-01-01 00:00:00.1234567897 UTC';
INSERT INTO temporal_defaults (id) VALUES ROW(2);

-- query 10
SELECT id, zoned FROM temporal_defaults ORDER BY id;

-- query 11
-- @skip_result_check=true
DROP TABLE temporal_v1;
DROP TABLE temporal_v2;
DROP TABLE temporal_defaults;
DROP SCHEMA iceberg_ddl_cat_${suite_uuid0}.trc6c_types_${uuid0};
