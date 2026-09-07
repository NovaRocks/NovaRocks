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

-- SQLP-9 session statements retain typed parser and frontend capability
-- contracts through the MySQL boundary.

-- @expect_error_tier=target
-- @expect_sql_code=sql.parse.unexpected_token
-- @expect_sql_phase=Parse
-- @expect_error_at=1:4
SET;

-- @expect_error_tier=target
-- @expect_sql_code=sql.parse.unexpected_token
-- @expect_sql_phase=Parse
-- @expect_error_at=1:5
SET = 1;

-- @expect_error_tier=target
-- @expect_sql_code=sql.parse.unexpected_token
-- @expect_sql_phase=Parse
-- @expect_error_at=1:14
KILL QUERY 1 extra;

-- @expect_error_tier=target
-- @expect_sql_code=sql.parse.unexpected_token
-- @expect_sql_phase=Parse
-- @expect_error_at=1:8
USE db extra;

-- @expect_error_tier=target
-- @expect_sql_code=sql.admit.session_global_scope_unsupported
-- @expect_sql_phase=Admit
-- @expect_error_at=1:5
SET GLOBAL query_timeout = 1;

-- Every external publication is one statement-level frontier. Transaction
-- controls are therefore admitted as a typed unsupported session family before
-- a catalog or data mutation can start.
-- @expect_error_tier=target
-- @expect_sql_code=sql.admit.session_transaction_unsupported
-- @expect_sql_phase=Admit
-- @expect_error_at=1:1
BEGIN;

-- @expect_error_tier=target
-- @expect_sql_code=sql.admit.session_transaction_unsupported
-- @expect_sql_phase=Admit
-- @expect_error_at=1:1
START TRANSACTION;

-- @expect_error_tier=target
-- @expect_sql_code=sql.admit.session_transaction_unsupported
-- @expect_sql_phase=Admit
-- @expect_error_at=1:1
COMMIT;

-- @expect_error_tier=target
-- @expect_sql_code=sql.admit.session_transaction_unsupported
-- @expect_sql_phase=Admit
-- @expect_error_at=1:1
ROLLBACK;

-- @expect_error_tier=target
-- @expect_sql_code=sql.admit.session_transaction_unsupported
-- @expect_sql_phase=Admit
-- @expect_error_at=1:1
SAVEPOINT before_publish;

-- Rejecting autocommit-off is an admission decision and the preceding SET
-- assignment in the same command must not be applied first. The location is
-- the offending assignment, not the statement head, so it stays on
-- `autocommit` even when an accepted assignment precedes it.
-- @expect_error_tier=target
-- @expect_sql_code=sql.admit.session_transaction_unsupported
-- @expect_sql_phase=Admit
-- @expect_error_at=1:24
SET query_timeout = 1, autocommit = OFF;

-- @expect_error_tier=target
-- @expect_sql_code=sql.admit.session_transaction_unsupported
-- @expect_sql_phase=Admit
-- @expect_error_at=1:5
SET autocommit = FALSE;

-- @expect_error_tier=target
-- @expect_sql_code=sql.admit.session_transaction_unsupported
-- @expect_sql_phase=Admit
-- @expect_error_at=1:5
SET @@session.autocommit = 0;
