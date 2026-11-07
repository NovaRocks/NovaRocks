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
-- Nested STRUCT add / drop / rename / widen DDL end-to-end.
-- Note: NovaRocks's standalone INSERT path doesn't yet support STRUCT
-- column writes, so this test exercises the DDL pipeline only and
-- confirms that nested-path SELECT projection still resolves after
-- each ALTER. Data-level coverage of the nested schema is provided by
-- unit tests in `connector::iceberg::catalog::schema_update`.

-- query 1
CREATE DATABASE iceberg_ddl_cat_${suite_uuid0}.schema_nested_${uuid0};
USE iceberg_ddl_cat_${suite_uuid0}.schema_nested_${uuid0};
DROP TABLE IF EXISTS people;
CREATE TABLE people (
  id INT,
  address STRUCT<street STRING, city STRING>
) TBLPROPERTIES (
  "format-version" = "2"
);

-- query 2
ALTER TABLE people ADD COLUMN address.zip INT;

-- query 3
ALTER TABLE people RENAME COLUMN address.zip TO postal_code;

-- query 4
ALTER TABLE people MODIFY COLUMN address.postal_code BIGINT;

-- query 5
ALTER TABLE people DROP COLUMN address.city;

-- query 6
-- @expect_error=Iceberg column `city` does not exist
ALTER TABLE people DROP COLUMN address.city;

-- query 7
-- @expect_error=Iceberg column `bogus` does not exist
ALTER TABLE people DROP COLUMN address.bogus;

-- query 8
DROP TABLE people;
DROP DATABASE iceberg_ddl_cat_${suite_uuid0}.schema_nested_${uuid0};
