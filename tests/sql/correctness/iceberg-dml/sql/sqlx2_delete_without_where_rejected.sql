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
-- SQLX-2 owner coverage: DELETE admission rejects an unqualified full-table
-- delete before it reaches connector write planning.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.sqlx2_delete_without_where ;
CREATE TABLE ${case_db}.sqlx2_delete_without_where (
  id BIGINT,
  v VARCHAR
) WITH (format_version = 3);
INSERT INTO ${case_db}.sqlx2_delete_without_where VALUES ROW(1, 'a');

-- query 2
-- @expect_error=DELETE requires a WHERE clause
DELETE FROM ${case_db}.sqlx2_delete_without_where;

-- query 3
SELECT id, v
FROM ${case_db}.sqlx2_delete_without_where
ORDER BY id;

-- query 4
-- @skip_result_check=true
DROP TABLE ${case_db}.sqlx2_delete_without_where ;
