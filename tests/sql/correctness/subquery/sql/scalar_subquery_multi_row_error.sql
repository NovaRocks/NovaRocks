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

-- @tags=subquery,oq13,assert_one_row
-- Test Objective: A scalar subquery that returns more than one row must raise a
-- runtime AssertOneRow error.  This is the base guarantee that
-- RankingWindowPredicatePushdown must not change: returning >1 row in a scalar
-- position must always error, regardless of whether the rule fires or not.
CREATE SCHEMA IF NOT EXISTS ${case_db};
USE ${case_db};
CREATE TABLE smr (k INT, v INT) WITH (format_version = 3);
INSERT INTO smr VALUES ROW(1, 10), ROW(1, 20);
-- @expect_error=[query.execution.invalid_data] Data failure (reason 7:316)
SELECT (SELECT v FROM smr WHERE k = 1);
