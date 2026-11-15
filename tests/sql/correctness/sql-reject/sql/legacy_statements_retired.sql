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
-- Trino 484 R01-R17/R21/R22 and plan2.7 require SYNTAX_ERROR.
-- No catalog or fixture side effects; each typed outcome belongs to one SQL.

-- query 1
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SHOW ALTER TABLE OPTIMIZE;

-- query 2
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SHOW ANALYZE;

-- query 3
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SHOW ANALYZE JOBS;

-- query 4
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
CANCEL ANALYZE 1;

-- query 5
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
KILL ANALYZE 1;

-- query 6
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
ANALYZE FULL TABLE t;

-- query 7
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
ANALYZE SAMPLE TABLE t;

-- query 8
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
ANALYZE TABLE t WITH SYNC MODE;

-- query 9
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
DROP STATS t;

-- query 10
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
DROP HISTOGRAM t (k);

-- query 11
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
DROP MULTIPLE COLUMNS STATS t (k);

-- query 12
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SHOW TABLE STATS t;

-- query 13
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SHOW MATERIALIZED VIEWS;

-- query 14
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SHOW MATERIALIZED VIEWS FROM db;

-- query 15
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SHOW MATERIALIZED VIEWS LIKE 'mv%';

-- query 16
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SHOW BACKENDS;

-- query 17
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
ADMIN RAISE ENGINE ERROR 'UnsupportedDistributedDmlShape';

-- query 18
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SHOW ALTER TABLE COLUMN;

-- query 19
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SHOW ALTER TABLE ROLLUP;
