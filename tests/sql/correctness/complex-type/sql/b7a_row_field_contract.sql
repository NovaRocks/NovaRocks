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

-- @order_sensitive=true
-- Build a named ROW over stored input with both a NULL child and a NULL parent.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.b7a_row_input (
    id INTEGER, payload ROW(inner_value INTEGER)
) WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.b7a_row_input VALUES
    ROW(1, ROW(7)),
    ROW(2, ROW(CAST(NULL AS INTEGER))),
    ROW(3, NULL);

-- query 3
WITH constructed AS (
    SELECT ROW(id AS "Identifier", payload AS nested, id + 10) AS r
    FROM ${case_db}.b7a_row_input
)
SELECT r."Identifier" AS outer_id, r.nested.inner_value AS inner_value,
       r.nested IS NULL AS parent_null, r[3] AS positional
FROM constructed
ORDER BY outer_id;

-- query 4
-- @expect_trino_error=AMBIGUOUS_NAME,USER_ERROR
SELECT r.id
FROM (SELECT ROW(id AS id, id AS ID) AS r FROM ${case_db}.b7a_row_input);

-- query 5
-- @expect_trino_error=EXPRESSION_NOT_CONSTANT,USER_ERROR
SELECT r[1 + 1]
FROM (SELECT ROW(id, payload) AS r FROM ${case_db}.b7a_row_input);
