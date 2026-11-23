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
-- Validate REST view replacement: every CREATE OR REPLACE VIEW adds a new
-- current version. Unchanged columns reference the existing view schema
-- (the reference server refuses a version on schema -1 when the commit added
-- none), changed columns add a schema, and replacing with an identical
-- definition still succeeds.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA iceberg_rest_${suite_uuid0}.iceberg_rest_vr_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_vr_db_${uuid0}.t_vr_${uuid0} (id INT, name VARCHAR);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_vr_db_${uuid0}.t_vr_${uuid0} VALUES ROW(1, 'a'), ROW(2, 'b');

-- query 4
-- @skip_result_check=true
CREATE VIEW iceberg_rest_${suite_uuid0}.iceberg_rest_vr_db_${uuid0}.v_vr_${uuid0} AS
  SELECT id, name FROM iceberg_rest_${suite_uuid0}.iceberg_rest_vr_db_${uuid0}.t_vr_${uuid0} WHERE id = 1;

-- query 5
-- @skip_result_check=true
-- Only the SQL changes; the columns stay id, name.
CREATE OR REPLACE VIEW iceberg_rest_${suite_uuid0}.iceberg_rest_vr_db_${uuid0}.v_vr_${uuid0} AS
  SELECT id, name FROM iceberg_rest_${suite_uuid0}.iceberg_rest_vr_db_${uuid0}.t_vr_${uuid0} WHERE id = 2;

-- query 6
SELECT id, name FROM iceberg_rest_${suite_uuid0}.iceberg_rest_vr_db_${uuid0}.v_vr_${uuid0} ORDER BY id;

-- query 7
-- @skip_result_check=true
-- The columns change.
CREATE OR REPLACE VIEW iceberg_rest_${suite_uuid0}.iceberg_rest_vr_db_${uuid0}.v_vr_${uuid0} AS
  SELECT id FROM iceberg_rest_${suite_uuid0}.iceberg_rest_vr_db_${uuid0}.t_vr_${uuid0};

-- query 8
SELECT * FROM iceberg_rest_${suite_uuid0}.iceberg_rest_vr_db_${uuid0}.v_vr_${uuid0} ORDER BY id;

-- query 9
-- @skip_result_check=true
-- The identical definition again, twice.
CREATE OR REPLACE VIEW iceberg_rest_${suite_uuid0}.iceberg_rest_vr_db_${uuid0}.v_vr_${uuid0} AS
  SELECT id FROM iceberg_rest_${suite_uuid0}.iceberg_rest_vr_db_${uuid0}.t_vr_${uuid0};

-- query 10
-- @skip_result_check=true
CREATE OR REPLACE VIEW iceberg_rest_${suite_uuid0}.iceberg_rest_vr_db_${uuid0}.v_vr_${uuid0} AS
  SELECT id FROM iceberg_rest_${suite_uuid0}.iceberg_rest_vr_db_${uuid0}.t_vr_${uuid0};

-- query 11
SELECT * FROM iceberg_rest_${suite_uuid0}.iceberg_rest_vr_db_${uuid0}.v_vr_${uuid0} ORDER BY id;

-- query 12
-- @skip_result_check=true
DROP VIEW iceberg_rest_${suite_uuid0}.iceberg_rest_vr_db_${uuid0}.v_vr_${uuid0};

-- query 13
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_vr_db_${uuid0}.t_vr_${uuid0};

-- query 14
-- @skip_result_check=true
DROP SCHEMA iceberg_rest_${suite_uuid0}.iceberg_rest_vr_db_${uuid0};
