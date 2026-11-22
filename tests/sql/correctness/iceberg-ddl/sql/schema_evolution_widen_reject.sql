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
-- Negative widening matrix.

-- query 1
CREATE SCHEMA iceberg_ddl_cat_${suite_uuid0}.schema_reject_${uuid0};
USE iceberg_ddl_cat_${suite_uuid0}.schema_reject_${uuid0};
DROP TABLE IF EXISTS bad;
CREATE TABLE bad (
  i BIGINT,
  d DOUBLE,
  s VARCHAR,
  ts TIMESTAMP(6)
) WITH (format_version = 2);

-- query 2
-- @expect_error=unsupported Iceberg type evolution
ALTER TABLE bad ALTER COLUMN i SET DATA TYPE INT;

-- query 3
-- @expect_error=unsupported Iceberg type evolution
ALTER TABLE bad ALTER COLUMN d SET DATA TYPE REAL;

-- query 4
-- @expect_error=unsupported Iceberg type evolution
ALTER TABLE bad ALTER COLUMN s SET DATA TYPE VARBINARY;

-- query 5
-- @expect_error=unsupported Iceberg type evolution
ALTER TABLE bad ALTER COLUMN ts SET DATA TYPE DATE;

-- query 6
DROP TABLE bad;
DROP SCHEMA iceberg_ddl_cat_${suite_uuid0}.schema_reject_${uuid0};
