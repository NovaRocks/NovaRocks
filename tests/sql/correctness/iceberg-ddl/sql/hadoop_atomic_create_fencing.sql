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
-- Validate SQL policy projection for the Hadoop v1 create fence. Cross-process
-- ownership is covered by the provider integration test; this case freezes the
-- user-visible strict/no-op behavior and confirms that the winner remains usable.

-- query 1
CREATE DATABASE iceberg_ddl_cat_${suite_uuid0}.hadoop_fence_${uuid0};
USE iceberg_ddl_cat_${suite_uuid0}.hadoop_fence_${uuid0};
CREATE TABLE events (
  id BIGINT,
  payload STRING
) TBLPROPERTIES (
  "format-version" = "2"
);

-- query 2
CREATE TABLE IF NOT EXISTS events (
  id BIGINT,
  payload STRING
) TBLPROPERTIES (
  "format-version" = "2"
);

-- query 3
-- @expect_error=AlreadyExists
CREATE TABLE events (
  id BIGINT,
  payload STRING
) TBLPROPERTIES (
  "format-version" = "2"
);

-- query 4
INSERT INTO events VALUES (1, 'winner');
SELECT * FROM events ORDER BY id;

-- query 5
DROP TABLE iceberg_ddl_cat_${suite_uuid0}.hadoop_fence_${uuid0}.events FORCE;
DROP DATABASE iceberg_ddl_cat_${suite_uuid0}.hadoop_fence_${uuid0};
