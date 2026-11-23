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

-- SQLP-9 separates SQL grammar, transaction state, and session property admission.

-- query 1
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SET;

-- query 2
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SET = 1;

-- query 3
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
KILL QUERY 1 extra;

-- query 4
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
USE db extra;

-- query 5
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SET GLOBAL query_timeout = 1;

-- query 6
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
BEGIN;

-- query 7
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
START TRANSACTION;

-- query 8
-- @expect_trino_error=NOT_IN_TRANSACTION,USER_ERROR
COMMIT;

-- query 9
-- @expect_trino_error=NOT_IN_TRANSACTION,USER_ERROR
ROLLBACK;

-- query 10
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SAVEPOINT before_publish;

-- query 11
-- The retired comma assignment is rejected before any statement change.
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SET query_timeout = 1, autocommit = OFF;

-- query 12
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION autocommit = FALSE;

-- query 13
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SET @@session.autocommit = 0;

-- query 14
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SET ROLE ALL IN reject_ice;

-- query 15
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SET SESSION AUTHORIZATION 'example-user';

-- query 16
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
RESET SESSION AUTHORIZATION;

-- query 17
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
PREPARE rejected FROM SELECT 1;

-- query 18
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
DEALLOCATE PREPARE rejected;

-- query 19
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
EXECUTE rejected USING 1;

-- query 20
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
EXECUTE IMMEDIATE 'SELECT 1';

-- query 21
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
DESCRIBE INPUT rejected;

-- query 22
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
DESCRIBE OUTPUT rejected;

-- query 23
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
DESCRIBE OUTPUT (SELECT 1 AS id);

-- query 24
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SET PATH reject_ice.reject_ns;

-- query 25
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION no_such_property = 1;

-- query 26
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
WITH SESSION no_such_property = 1 SELECT CAST(1 AS integer);

-- query 27
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SET LOCAL pipeline_dop = 1;

-- query 28
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SET pipeline_dop = 1;

-- query 29
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
START TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY;

-- query 30
-- @expect_trino_error=NOT_IN_TRANSACTION,USER_ERROR
COMMIT WORK;

-- query 31
-- @expect_trino_error=NOT_IN_TRANSACTION,USER_ERROR
ROLLBACK WORK;

-- query 32
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
RESET SESSION no_such_property;
