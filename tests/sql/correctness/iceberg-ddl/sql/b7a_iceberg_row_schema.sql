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

-- query 1
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE TABLE ${case_db}.b7a_anon_row AS
SELECT ROW(1) AS payload;

-- query 2
-- @expect_trino_error=DUPLICATE_COLUMN_NAME,USER_ERROR
CREATE TABLE ${case_db}.b7a_duplicate_row AS
SELECT ROW(1 AS id, 2 AS ID) AS payload;

-- query 3
SELECT count(*) AS rejected_tables
FROM information_schema.tables
WHERE table_schema = '${case_db}'
  AND table_name IN ('b7a_anon_row', 'b7a_duplicate_row');

-- query 4
-- @skip_result_check=true
CREATE TABLE ${case_db}.b7a_named_row (
    payload ROW(id INTEGER)
) WITH (format_version = 3);

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.b7a_named_row VALUES ROW(ROW(7));

-- query 6
SELECT payload.id AS id_value FROM ${case_db}.b7a_named_row;
