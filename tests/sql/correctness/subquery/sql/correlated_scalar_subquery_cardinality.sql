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


-- Test the one-row guard generated when decorrelating a scalar subquery.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.outer_rows (id INTEGER, k INTEGER) WITH (format_version = 3);
CREATE TABLE ${case_db}.inner_rows (k INTEGER, v INTEGER) WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.outer_rows VALUES ROW(1, 10), ROW(2, 20);
INSERT INTO ${case_db}.inner_rows VALUES ROW(10, 100), ROW(20, 200), ROW(20, 201);

-- query 3
SELECT o.id, (SELECT i.v FROM ${case_db}.inner_rows i WHERE i.k = o.k) AS v
FROM ${case_db}.outer_rows o WHERE o.id = 1;

-- query 4
-- @expect_error=[query.execution.invalid_data] Data failure (reason 1:422)
SELECT o.id, (SELECT i.v FROM ${case_db}.inner_rows i WHERE i.k = o.k) AS v
FROM ${case_db}.outer_rows o WHERE o.id = 2;
