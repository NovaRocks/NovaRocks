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
-- ARRAY element + MAP value widen DDL end-to-end.
-- Note: standalone INSERT does not yet support ARRAY/MAP column writes,
-- so this test exercises the DDL pipeline only. Data-level coverage of
-- element/value widen is provided by unit tests in
-- `connector::iceberg::catalog::schema_update`.

-- query 1
CREATE DATABASE iceberg_ddl_cat_${suite_uuid0}.schema_arrmap_${uuid0};
USE iceberg_ddl_cat_${suite_uuid0}.schema_arrmap_${uuid0};
DROP TABLE IF EXISTS samples;
CREATE TABLE samples (
  id INT,
  scores ARRAY<INT>,
  attrs MAP<STRING, INT>
) TBLPROPERTIES (
  "format-version" = "2"
);

-- query 2
ALTER TABLE samples MODIFY COLUMN scores.element BIGINT;

-- query 3
ALTER TABLE samples MODIFY COLUMN attrs.value BIGINT;

-- query 4
-- @expect_error=unsupported Iceberg type evolution
ALTER TABLE samples MODIFY COLUMN scores.element INT;

-- query 5
DROP TABLE samples;
DROP DATABASE iceberg_ddl_cat_${suite_uuid0}.schema_arrmap_${uuid0};
