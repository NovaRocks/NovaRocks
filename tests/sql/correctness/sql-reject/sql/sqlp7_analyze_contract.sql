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

-- SQL analyze errors must retain their registered code and the AST location
-- until the MySQL client boundary.
-- @expect_error_tier=target
-- @expect_sql_code=sql.analyze.unknown_table
-- @expect_sql_phase=Analyze
-- @expect_error_at=1:15
SELECT * FROM sqlp7_missing_table;

-- @expect_error_tier=target
-- @expect_sql_code=sql.analyze.unknown_column
-- @expect_sql_phase=Analyze
-- @expect_error_at=1:34
WITH t AS (SELECT 1 AS c) SELECT missing FROM t;

-- @expect_error_tier=target
-- @expect_sql_code=sql.analyze.unknown_function
-- @expect_sql_phase=Analyze
-- @expect_error_at=1:8
SELECT sqlp7_not_a_function(1);

-- @expect_error_tier=target
-- @expect_sql_code=sql.analyze.type_mismatch
-- @expect_sql_phase=Analyze
-- @expect_error_at=1:8
SELECT substring('x', 1, 2, 3);

-- @expect_error_tier=target
-- @expect_sql_code=sql.parse.unexpected_token
-- @expect_sql_phase=Parse
-- @expect_error_at=1:19
SELECT 1 GROUP BY 9999999999999999999999999999999999999999;

-- @expect_error_tier=target
-- @expect_sql_code=sql.analyze.invalid_argument
-- @expect_sql_phase=Analyze
-- @expect_error_at=1:8
SELECT ds_hll_count_distinct(1, 1);

-- @expect_error_tier=target
-- @expect_sql_code=sql.analyze.invalid_query_shape
-- @expect_sql_phase=Analyze
-- @expect_error_at=1:8
SELECT count_if(DISTINCT true);

-- @expect_error_tier=target
-- @expect_sql_code=sql.analyze.unsupported_expression
-- @expect_sql_phase=Analyze
-- @expect_error_at=1:8
SELECT (x) -> x;

-- @expect_error_tier=target
-- @expect_sql_code=sql.analyze.unsupported_query_shape
-- @expect_sql_phase=Analyze
-- @expect_error_at=1:65
WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS x) SELECT * FROM a NATURAL JOIN b;
