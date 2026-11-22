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
-- @skip_result_check=true
CREATE TABLE ${case_db}.bounded_defaults (
  id INTEGER,
  v VARCHAR(2) DEFAULT '𐐷é',
  c CHAR(1) DEFAULT '𐐷é ',
  empty_v VARCHAR(0) DEFAULT '',
  null_v VARCHAR(0) DEFAULT NULL
) WITH (format_version = 3);
INSERT INTO ${case_db}.bounded_defaults (id) VALUES ROW(1);
ALTER TABLE ${case_db}.bounded_defaults ADD COLUMN added_v VARCHAR(2) DEFAULT 'é中';
ALTER TABLE ${case_db}.bounded_defaults ADD COLUMN added_c CHAR(1) DEFAULT '𐐷é ';

-- query 2
-- @expect_trino_error=INVALID_DEFAULT_COLUMN_VALUE,USER_ERROR
CREATE TABLE ${case_db}.over_width (v VARCHAR(1) DEFAULT '𐐷é') WITH (format_version = 3);

-- query 3
-- @expect_trino_error=INVALID_DEFAULT_COLUMN_VALUE,USER_ERROR
ALTER TABLE ${case_db}.bounded_defaults ADD COLUMN rejected VARCHAR(1) DEFAULT 'x ';

-- query 4
-- @expect_trino_error=INVALID_DEFAULT_COLUMN_VALUE,USER_ERROR
CREATE TABLE ${case_db}.wrong_kind (v VARCHAR(4) DEFAULT 3) WITH (format_version = 3);

-- query 5
-- @expect_trino_error=INVALID_DEFAULT_COLUMN_VALUE,USER_ERROR
ALTER TABLE ${case_db}.bounded_defaults ADD COLUMN rejected VARCHAR(4) DEFAULT 3;

-- query 6
-- @expect_trino_error=COLUMN_NOT_FOUND,USER_ERROR
SELECT rejected FROM ${case_db}.bounded_defaults;

-- query 7
INSERT INTO ${case_db}.bounded_defaults (id) VALUES ROW(2);
SELECT id, v, concat('<', c, '>') AS c, empty_v, null_v,
       added_v, concat('<', added_c, '>') AS added_c
FROM ${case_db}.bounded_defaults ORDER BY id;

-- query 8
-- @skip_result_check=true
DROP TABLE ${case_db}.bounded_defaults;
