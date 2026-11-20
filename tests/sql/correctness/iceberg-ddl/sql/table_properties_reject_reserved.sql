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
-- Denylist coverage: each reserved category errors clearly.

-- query 1
CREATE SCHEMA iceberg_ddl_cat_${suite_uuid0}.tblprops_reject_${uuid0};
USE iceberg_ddl_cat_${suite_uuid0}.tblprops_reject_${uuid0};
DROP TABLE IF EXISTS p;
CREATE TABLE p (id INT) WITH (format_version = 2);

-- query 2
-- @expect_error=Iceberg table property `format-version` is reserved
ALTER TABLE p SET PROPERTIES extra_properties = MAP(ARRAY['format-version'], ARRAY['3']);

-- query 3
-- @expect_error=Iceberg internal metadata key
ALTER TABLE p SET PROPERTIES extra_properties = MAP(ARRAY['identifier-field-ids'], ARRAY['[1]']);

-- query 4
-- @expect_error=Iceberg internal metadata key
ALTER TABLE p SET PROPERTIES extra_properties = MAP(ARRAY['current-schema-id'], ARRAY['5']);

-- query 5
-- ADR-0182: this ordinary property does not author any type domain.
ALTER TABLE p SET PROPERTIES extra_properties = MAP(ARRAY['novarocks.logical_type.foo'], ARRAY['TINYINT']);
ALTER TABLE p EXECUTE unset_extra_properties(keys => ARRAY['novarocks.logical_type.foo'], if_exists => false);

-- query 6
-- @expect_error=novarocks.* namespace is reserved
ALTER TABLE p SET PROPERTIES extra_properties = MAP(ARRAY['novarocks.future'], ARRAY['whatever']);

-- query 7
-- UNSET path covered too.
-- @expect_error=Iceberg internal metadata key
ALTER TABLE p EXECUTE unset_extra_properties(keys => ARRAY['last-column-id'], if_exists => false);

-- query 8
-- @expect_error_tier=drift
-- @expect_error=write.parquet.variant-shredding.
ALTER TABLE p SET PROPERTIES extra_properties = MAP(ARRAY['write.parquet.variant-shredding.payload'], ARRAY['a:int']);

-- query 9
-- @expect_error_tier=drift
-- @expect_error=write.parquet.variant-shredding.
CREATE TABLE shredding_create (id INT) WITH (extra_properties = MAP(ARRAY['write.parquet.variant-shredding.payload'], ARRAY['a:int']));

-- query 10
-- Only the current nominal document namespace authors these type facts.
-- @expect_error=Iceberg nominal declarations are owned by schema mutations
ALTER TABLE p SET PROPERTIES extra_properties = MAP(ARRAY['novarocks.nominal-types.v1'], ARRAY['{"version":1,"fields":[]}']);

-- query 11
DROP TABLE p;
DROP SCHEMA iceberg_ddl_cat_${suite_uuid0}.tblprops_reject_${uuid0};
