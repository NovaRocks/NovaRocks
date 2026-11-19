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
-- Validate SELECT * FROM <tbl>$refs surfaces branches and tags.
-- Tests Phase A (metadata table SQL routing) — refs flavour.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metaref_${uuid0} (id INT)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metaref_${uuid0} VALUES (1);

-- query 4
-- @skip_result_check=true
ALTER TABLE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metaref_${uuid0} CREATE BRANCH dev;

-- query 5
-- @skip_result_check=true
ALTER TABLE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metaref_${uuid0} CREATE TAG v1;

-- query 6
-- 3 refs total: dev (BRANCH), main (BRANCH), v1 (TAG). Names are tab-stable.
SELECT name, type
  FROM iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}."metaref_${uuid0}$refs"
  ORDER BY name;

-- query 7
-- @skip_result_check=true
ALTER TABLE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metaref_${uuid0} DROP TAG v1;

-- query 8
-- @skip_result_check=true
ALTER TABLE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metaref_${uuid0} DROP BRANCH dev;

-- query 9
-- @skip_result_check=true
DROP TABLE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metaref_${uuid0};

-- query 10
-- @skip_result_check=true
DROP DATABASE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0};
