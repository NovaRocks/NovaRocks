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
-- @tags=system_catalog,retired_syntax
-- Correctness owner: sql-reject. Trino B05-B08 reject these complete
-- statements with SYNTAX_ERROR/USER_ERROR. No external fixture is required.

-- query 1
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SHOW VIEWS;

-- query 2
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SHOW VIEWS FROM x;

-- query 3
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SHOW BASIC STATS META;

-- query 4
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SHOW HISTOGRAM STATS META;

-- query 5
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SHOW CREATE SCHEMA reject_ice.reject_ns;

-- query 6
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SHOW CREATE FUNCTION reject_ice.reject_ns.f;

-- query 7
-- @expect_trino_error=CATALOG_NOT_FOUND,USER_ERROR
SHOW TABLES FROM trc7_missing_catalog.reject_ns;

-- query 8
-- @expect_trino_error=CATALOG_NOT_FOUND,USER_ERROR
SHOW SCHEMAS FROM trc7_missing_catalog;

-- query 10
-- @expect_trino_error=CATALOG_NOT_FOUND,USER_ERROR
SHOW COLUMNS FROM trc7_missing_catalog.reject_ns.t;

-- query 11
-- @expect_trino_error=CATALOG_NOT_FOUND,USER_ERROR
DESCRIBE trc7_missing_catalog.reject_ns.t;

-- query 12
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SHOW STATS FOR (SELECT 1 AS id);

-- query 13
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SHOW CURRENT ROLES FROM reject_ice;

-- query 14
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SHOW ROLE GRANTS FROM reject_ice;

-- query 17
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SHOW PROCESSLIST;
