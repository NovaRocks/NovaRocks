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
-- @tags=session,protocol
-- Test Objective:
-- Retired MySQL session spellings fail with the approved Trino error names.
-- Each rejected statement leaves the session usable and emits no mutation.
-- Valid USE and typed session assignments keep their original behavior.

-- query 1
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
-- @skip_result_check=true
SET NAMES utf8mb4;

-- query 2
SELECT 1;

-- query 3
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
-- @skip_result_check=true
SET autocommit = 1;

-- query 4
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
-- @skip_result_check=true
SET character_set_results = NULL;

-- query 5
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
-- @skip_result_check=true
SET @@autocommit = 1;

-- query 6
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
-- @skip_result_check=true
SET NAMES 'utf8mb4' COLLATE 'utf8mb4_general_ci';

-- query 7
-- The rejected spellings leave the session usable.
SELECT 1;

-- query 8
-- @skip_result_check=true
USE default;

-- query 9
SELECT 1;

-- query 10
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
-- The retired @@ target rejects the complete statement before assignment.
-- @skip_result_check=true
SET SESSION query_timeout = 60, LOCAL pipeline_dop = 1, @@session.query_timeout = 0;

-- query 11
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
-- @skip_result_check=true
SET CATALOG default_catalog;

-- query 12
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
-- @skip_result_check=true
SET catalog = default_catalog;

-- query 13
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
-- The retired transaction-setting grammar is rejected.
-- @skip_result_check=true
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;

-- query 14
-- `USE` remains usable after the rejected catalog pseudo-properties.
-- @skip_result_check=true
USE default;

-- query 15
SELECT 1;

-- query 16
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
-- GLOBAL is refused for unknown and recognized property names alike.
-- @skip_result_check=true
SET GLOBAL sqlp9_unknown_scope_probe=1, query_timeout=1;

-- query 17
-- @skip_result_check=true
SET query_timeout = 0;

-- query 18
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
-- ON is a reserved expression token; the parser rejects it before lookup.
-- @skip_result_check=true
SET autocommit = ON;

-- query 19
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
-- @skip_result_check=true
SET @@session.autocommit = TRUE;
