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
CREATE DATABASE iceberg_ddl_cat_${suite_uuid0}.schema_reject_${uuid0};
USE iceberg_ddl_cat_${suite_uuid0}.schema_reject_${uuid0};
DROP TABLE IF EXISTS bad;
CREATE TABLE bad (
  i BIGINT,
  d DOUBLE,
  s STRING,
  ts DATETIME
) TBLPROPERTIES ("format-version" = "2");

-- query 2
-- @expect_error=unsupported Iceberg type evolution
ALTER TABLE bad MODIFY COLUMN i INT;

-- query 3
-- @expect_error=unsupported Iceberg type evolution
ALTER TABLE bad MODIFY COLUMN d FLOAT;

-- query 4
-- @expect_error=unsupported Iceberg type evolution
ALTER TABLE bad MODIFY COLUMN s VARBINARY;

-- query 5
-- @expect_error=unsupported Iceberg type evolution
ALTER TABLE bad MODIFY COLUMN ts DATE;

-- query 6
DROP TABLE bad;
DROP DATABASE iceberg_ddl_cat_${suite_uuid0}.schema_reject_${uuid0};
