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

-- @tags=iceberg_ddl,struct
-- Test Objective:
-- 1. Dropping the final remaining field of a STRUCT is rejected on Iceberg.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.tab1;
CREATE TABLE ${case_db}.tab1 (
  c0 INT,
  c1 ROW(v1 INT)
);

-- query 2
-- @expect_error=cannot drop last field of STRUCT
ALTER TABLE ${case_db}.tab1 DROP COLUMN c1.v1;
