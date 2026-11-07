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
-- DATE -> TIMESTAMP widen.

-- query 1
CREATE DATABASE iceberg_ddl_cat_${suite_uuid0}.schema_date_${uuid0};
USE iceberg_ddl_cat_${suite_uuid0}.schema_date_${uuid0};
DROP TABLE IF EXISTS events;
CREATE TABLE events (
  id INT,
  occurred_on DATE
) TBLPROPERTIES (
  "format-version" = "2"
);
INSERT INTO events VALUES (1, '2026-01-15');

-- query 2
SELECT id, occurred_on FROM events ORDER BY id;

-- query 3
ALTER TABLE events MODIFY COLUMN occurred_on DATETIME;
INSERT INTO events VALUES (2, '2026-02-20 11:22:33');

-- query 4
SELECT id, occurred_on FROM events ORDER BY id;

-- query 5
DROP TABLE events;
DROP DATABASE iceberg_ddl_cat_${suite_uuid0}.schema_date_${uuid0};
