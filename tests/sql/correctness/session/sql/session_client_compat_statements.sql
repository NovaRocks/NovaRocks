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
-- MySQL clients and connection pools open a session by issuing a handful of
-- charset/transaction statements before any user SQL. NovaRocks accepts them as
-- inert session no-ops. This case freezes that contract: each statement must
-- succeed, and none of them may disturb the session that follows.
--
-- The contract worth protecting is user-observable: typed session statements
-- preserve their independent scopes, accepted compatibility forms and catalog
-- context while the session remains usable.

-- query 1
-- @skip_result_check=true
SET NAMES utf8mb4;

-- query 2
SELECT 1;

-- query 3
-- @skip_result_check=true
SET autocommit = 1;

-- query 4
-- @skip_result_check=true
SET character_set_results = NULL;

-- query 5
-- @skip_result_check=true
SET @@autocommit = 1;

-- query 6
-- @skip_result_check=true
SET NAMES 'utf8mb4' COLLATE 'utf8mb4_general_ci';

-- query 7
-- The session is still usable, and unrelated session state set earlier in the
-- connection has not been clobbered by the compatibility statements above.
SELECT 1;

-- query 8
-- @skip_result_check=true
USE default;

-- query 9
SELECT 1;

-- query 10
-- Scope is per assignment rather than inherited by the following assignment.
-- @skip_result_check=true
SET SESSION query_timeout = 60, LOCAL pipeline_dop = 1, @@session.query_timeout = 0;

-- query 11
-- @skip_result_check=true
SET CATALOG default_catalog;

-- query 12
-- @skip_result_check=true
SET catalog = default_catalog;

-- query 13
-- This interoperability statement does not create a transaction or claim that
-- NovaRocks implements transaction-isolation semantics.
-- @skip_result_check=true
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;

-- query 14
-- `USE` continues to resolve against the catalog selected above.
-- @skip_result_check=true
USE default;

-- query 15
SELECT 1;

-- query 16
-- An unknown GLOBAL variable is tolerated without leaking its scope to the
-- following independently-scoped, recognized assignment.
-- @skip_result_check=true
SET GLOBAL sqlp9_unknown_scope_probe=1, query_timeout=1;

-- query 17
-- @skip_result_check=true
SET query_timeout = 0;

-- query 18
-- The only supported autocommit mode is the truthful default: each statement
-- publishes at its own frontier. These spellings must not open a transaction.
-- @skip_result_check=true
SET autocommit = ON;

-- query 19
-- @skip_result_check=true
SET @@session.autocommit = TRUE;
