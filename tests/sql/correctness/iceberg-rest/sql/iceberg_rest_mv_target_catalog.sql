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
-- Validate Iceberg MV target catalog semantics against REST catalog:
-- - target is created as a normal Iceberg table in an explicitly selected namespace
-- - CREATE does not write data; REFRESH populates the target
-- - refresh resolves an unqualified raw source with the frozen creation database,
--   not the target namespace
-- - SHOW/REFRESH/DROP use NovaRocks relationship metadata
-- - existing target and non-Iceberg current catalog fail fast

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_rest_${suite_uuid0}.analytics_${uuid0};
CREATE DATABASE iceberg_rest_${suite_uuid0}.sales_${uuid0};
CREATE TABLE iceberg_rest_${suite_uuid0}.sales_${uuid0}.orders_${uuid0} (
  id INT,
  name STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO iceberg_rest_${suite_uuid0}.sales_${uuid0}.orders_${uuid0}
VALUES (1, 'a'), (2, 'b');
USE iceberg_rest_${suite_uuid0}.sales_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE MATERIALIZED VIEW analytics_${uuid0}.mv_orders_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES('storage_engine' = 'iceberg')
AS SELECT /* effective raw source */ id, name
FROM orders_${uuid0};

-- query 3
SELECT COUNT(*) AS n FROM analytics_${uuid0}.mv_orders_${uuid0};

-- query 4
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW analytics_${uuid0}.mv_orders_${uuid0};

-- query 5
SELECT id, name FROM analytics_${uuid0}.mv_orders_${uuid0} ORDER BY id;

-- query 6
-- @result_contains=mv_orders_
SELECT * FROM system.runtime.materialized_view_states WHERE catalog_name = 'iceberg_rest_${suite_uuid0}';

-- query 7
-- @expect_error=Iceberg MV target table iceberg_rest_${suite_uuid0}.analytics_${uuid0}.mv_conflict_${uuid0} already exists
CREATE TABLE analytics_${uuid0}.mv_conflict_${uuid0} (id INT);
CREATE MATERIALIZED VIEW analytics_${uuid0}.mv_conflict_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES('storage_engine' = 'iceberg')
AS SELECT id FROM orders_${uuid0};

-- query 8
-- @skip_result_check=true
DROP TABLE analytics_${uuid0}.mv_conflict_${uuid0};

-- query 9
-- @expect_error=requires current Iceberg catalog context
USE default_catalog.default;
CREATE MATERIALIZED VIEW mv_bad_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES('storage_engine' = 'iceberg')
AS SELECT id
FROM iceberg_rest_${suite_uuid0}.sales_${uuid0}.orders_${uuid0};

-- query 10
-- @skip_result_check=true
USE iceberg_rest_${suite_uuid0}.sales_${uuid0};
DROP MATERIALIZED VIEW analytics_${uuid0}.mv_orders_${uuid0};

-- query 11
-- @expect_error=unknown table
SELECT COUNT(*) FROM analytics_${uuid0}.mv_orders_${uuid0};

-- query 12
-- @cleanup=true
-- @skip_result_check=true
USE iceberg_rest_${suite_uuid0}.analytics_${uuid0};
DROP MATERIALIZED VIEW IF EXISTS analytics_${uuid0}.mv_orders_${uuid0};
DROP MATERIALIZED VIEW IF EXISTS analytics_${uuid0}.mv_conflict_${uuid0};
DROP TABLE IF EXISTS iceberg_rest_${suite_uuid0}.analytics_${uuid0}.mv_conflict_${uuid0};
DROP TABLE IF EXISTS iceberg_rest_${suite_uuid0}.sales_${uuid0}.orders_${uuid0};
DROP DATABASE IF EXISTS iceberg_rest_${suite_uuid0}.analytics_${uuid0};
DROP DATABASE IF EXISTS iceberg_rest_${suite_uuid0}.sales_${uuid0};
