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
-- @tags=session,trc5c,session_registry
-- Required typed session values, bounded evaluation and retired syntax.
-- Expected rules come from fixed Trino 484 source and approved NovaRocks differences.

-- query 1
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SELECT @x;

-- query 2
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SELECT @@time_zone;

-- query 3
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SET @x = 1;

-- query 4
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SET NAMES utf8;

-- query 5
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SET CATALOG default_catalog;

-- query 6
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

-- query 7
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SELECT * FROM t1 JOIN [broadcast] t2 ON t1.a = t2.a;

-- query 8
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SELECT count(*) FROM t [_META_];

-- query 9
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SELECT /*+ set_user_variable(@v = 1) */ 1;

-- query 10
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET CATALOG = default_catalog;
