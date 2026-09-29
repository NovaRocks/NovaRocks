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
-- Validate Iceberg complex-type readback and the frozen provider schema.
-- query 1
CREATE DATABASE iceberg_ddl_cat_${suite_uuid0}.iceberg_db_${uuid0};
CREATE TABLE iceberg_ddl_cat_${suite_uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} (
  name ARRAY<STRUCT<
    user STRING,
    family STRING,
    given ARRAY<STRING>,
    prefix ARRAY<STRING>,
    suffix ARRAY<STRING>
  >>
);
INSERT INTO iceberg_ddl_cat_${suite_uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} VALUES
([named_struct('user', 'official', 'family', 'Glover433', 'given', ['Kira861'], 'prefix', ['Ms.'], 'suffix', NULL)]);
SELECT array_filter(x -> x.`user` = 'official', name)[1].family AS family_name
FROM iceberg_ddl_cat_${suite_uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0};

-- query 2
-- The previous label listed all five fields, so it did not prove pruning.
-- Assert the admitted ARRAY/STRUCT schema in the level that exposes types.
-- @skip_result_check=true
-- @result_contains=relation-field[0] column=
-- @result_contains=provider-output[0] column=
-- @result_contains=type=List(Struct(
-- @result_contains="user": Utf8
-- @result_contains="family": Utf8
-- @result_contains="given": List(Utf8
-- @result_contains="prefix": List(Utf8
-- @result_contains="suffix": List(Utf8
EXPLAIN CONTRACT
SELECT array_filter(x -> x.`user` = 'official', name)[1].family AS family_name
FROM iceberg_ddl_cat_${suite_uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0};
SET catalog default_catalog;
DROP TABLE iceberg_ddl_cat_${suite_uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} FORCE;
DROP DATABASE iceberg_ddl_cat_${suite_uuid0}.iceberg_db_${uuid0};
