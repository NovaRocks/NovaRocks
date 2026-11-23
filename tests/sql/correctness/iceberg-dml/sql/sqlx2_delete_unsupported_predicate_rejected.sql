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
-- SQLX-2 owner coverage: unsupported DELETE predicates fail in SQL admission
-- and leave the frozen target unchanged.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.sqlx2_delete_unsupported_predicate ;
CREATE TABLE ${case_db}.sqlx2_delete_unsupported_predicate (
  id BIGINT,
  v VARCHAR
) WITH (format_version = 3);
INSERT INTO ${case_db}.sqlx2_delete_unsupported_predicate VALUES ROW(1, 'alpha');

-- query 2
-- @expect_error=phase 1 DELETE WHERE
DELETE FROM ${case_db}.sqlx2_delete_unsupported_predicate WHERE v LIKE 'a%';

-- query 3
SELECT id, v
FROM ${case_db}.sqlx2_delete_unsupported_predicate
ORDER BY id;

-- query 4
-- @skip_result_check=true
DROP TABLE ${case_db}.sqlx2_delete_unsupported_predicate ;
