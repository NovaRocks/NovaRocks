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
-- Validate ordinary REST object-store reads and writes with the admitted
-- role-local source. The runner owns this case's isolated database.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.object_store_rows (
    id INT,
    `value` STRING
) TBLPROPERTIES ("format-version" = "2");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.object_store_rows VALUES
    (1, 'alpha'), (2, 'beta'), (3, 'gamma');

-- query 3
SELECT id, `value` FROM ${case_db}.object_store_rows ORDER BY id;

-- query 4
-- @skip_result_check=true
DELETE FROM ${case_db}.object_store_rows WHERE id = 2;

-- query 5
SELECT id, `value` FROM ${case_db}.object_store_rows ORDER BY id;

-- query 6
-- @skip_result_check=true
DROP TABLE ${case_db}.object_store_rows;
