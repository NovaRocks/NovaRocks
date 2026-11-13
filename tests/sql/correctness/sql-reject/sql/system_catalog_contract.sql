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

-- @sequential=true
-- @order_sensitive=true
-- @tags=system_catalog,read_only,reserved_names
-- Correctness owner: sql-reject. Trino B14/B15/B17/B20/E03/E13/E17
-- freeze the lookup and read-only errors. The accepted spec additionally
-- rejects INSERT as NOT_SUPPORTED and reserves system for catalog DDL.
-- Rejection happens before catalog construction; no external fixture or
-- cleanup resource is needed for these statements.

-- query 1
-- @expect_trino_error=TABLE_NOT_FOUND,USER_ERROR
SELECT * FROM system.metadata.trc_no_such_table;

-- query 2
-- @expect_trino_error=SCHEMA_NOT_FOUND,USER_ERROR
SELECT * FROM system.trc_no_such_schema.t;

-- query 3
-- @expect_trino_error=CATALOG_NOT_FOUND,USER_ERROR
SELECT * FROM trc_no_such_catalog.information_schema.tables;

-- query 4
-- @expect_trino_error=TABLE_NOT_FOUND,USER_ERROR
SELECT * FROM default_catalog.information_schema.materialized_views;

-- query 5
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
DELETE FROM system.metadata.materialized_views;

-- query 6
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE TABLE system.metadata.t (a bigint);

-- query 7
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
INSERT INTO system.metadata.materialized_views SELECT * FROM system.metadata.materialized_views;

-- query 8
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
DROP CATALOG system;

-- query 9
-- @expect_trino_error=ALREADY_EXISTS,USER_ERROR
CREATE EXTERNAL CATALOG system
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "hadoop",
    "iceberg.catalog.warehouse" = "file:///trc_reserved_system"
);

-- query 10
SELECT name FROM system.metadata.materialized_views WHERE name = 'trc_no_such_view';

-- query 11
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ANALYZE TABLE system.metadata.materialized_views;

-- query 12
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
ANALYZE TABLE trc_no_such_catalog.information_schema.tables;
