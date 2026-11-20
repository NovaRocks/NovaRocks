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
-- Test Point: INSERT INTO t (a) SELECT ... materializes write-default for column b on the FROM QUERY path.
-- Method: CREATE v3 table with b INT DEFAULT 5, INSERT (a) SELECT 1, SELECT to confirm row materialized as (1, 5).
-- Scope: write-default on FROM QUERY path (Task 17).

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_default_iselect ;
CREATE TABLE ${case_db}.t_v3_default_iselect (
  a INT,
  b INT DEFAULT 5
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_v3_default_iselect (a) SELECT 1;

-- query 2
SELECT a, b FROM ${case_db}.t_v3_default_iselect;

-- query 3
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_default_iselect ;
