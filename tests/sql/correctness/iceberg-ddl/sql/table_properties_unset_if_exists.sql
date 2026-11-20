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
-- UNSET TBLPROPERTIES strict vs IF EXISTS.

-- query 1
CREATE SCHEMA iceberg_ddl_cat_${suite_uuid0}.tblprops_ifexists_${uuid0};
USE iceberg_ddl_cat_${suite_uuid0}.tblprops_ifexists_${uuid0};
DROP TABLE IF EXISTS p;
CREATE TABLE p (id INT) WITH (format_version = 2);
ALTER TABLE p SET PROPERTIES extra_properties = MAP(ARRAY['a', 'b'], ARRAY['1', '2']);

-- query 2
SELECT count() FROM p;

-- query 3
-- Strict: missing key fails.
-- @expect_error=Iceberg table property `c` does not exist
ALTER TABLE p EXECUTE unset_extra_properties(keys => ARRAY['a', 'c'], if_exists => false);

-- query 4
-- Existing keys unchanged after the failed strict UNSET; table still queryable.
SELECT count() FROM p;

-- query 5
-- IF EXISTS: missing keys silently skipped, present keys still removed.
ALTER TABLE p EXECUTE unset_extra_properties(keys => ARRAY['a', 'c'], if_exists => true);

-- query 6
SELECT count() FROM p;

-- query 7
DROP TABLE p;
DROP SCHEMA iceberg_ddl_cat_${suite_uuid0}.tblprops_ifexists_${uuid0};
