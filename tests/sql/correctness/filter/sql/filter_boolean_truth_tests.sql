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


-- Test Boolean truth predicates on nullable scan columns in projections and filters.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.truth_values (id INTEGER, p BOOLEAN) WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.truth_values VALUES ROW(1, TRUE), ROW(2, FALSE), ROW(3, NULL);

-- query 3
SELECT id, p IS TRUE AS is_true, p IS NOT TRUE AS is_not_true,
    p IS FALSE AS is_false, p IS NOT FALSE AS is_not_false,
    p IS UNKNOWN AS is_unknown, p IS NOT UNKNOWN AS is_not_unknown
FROM ${case_db}.truth_values ORDER BY id;

-- query 4
SELECT id FROM ${case_db}.truth_values WHERE p IS TRUE ORDER BY id;

-- query 5
SELECT id FROM ${case_db}.truth_values WHERE p IS NOT TRUE ORDER BY id;

-- query 6
SELECT id FROM ${case_db}.truth_values WHERE p IS FALSE ORDER BY id;

-- query 7
SELECT id FROM ${case_db}.truth_values WHERE p IS NOT FALSE ORDER BY id;

-- query 8
SELECT id FROM ${case_db}.truth_values WHERE p IS UNKNOWN ORDER BY id;

-- query 9
SELECT id FROM ${case_db}.truth_values WHERE p IS NOT UNKNOWN ORDER BY id;
