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
-- Test Point: ALTER TABLE ADD COLUMN with DEFAULT on a v3 table with existing rows: old rows read default via initial-default; new INSERTs materialize write-default.
-- Method: CREATE v3 table, INSERT two rows, ALTER ADD COLUMN b INT DEFAULT 9, SELECT (expect default backfill on existing rows), then INSERT one more row with column list, SELECT again.
-- Scope: read-side initial-default backfill (A) + write-side write-default materialization (B).

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_default_addcol ;
CREATE TABLE ${case_db}.t_v3_default_addcol (
  a INT
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_v3_default_addcol VALUES ROW(1), ROW(2);
ALTER TABLE ${case_db}.t_v3_default_addcol ADD COLUMN b INT DEFAULT 9;

-- query 2
-- Pre-existing rows must read b = 9 via initial-default backfill, not NULL.
SELECT a, b FROM ${case_db}.t_v3_default_addcol ORDER BY a;

-- query 3
-- New INSERT (col list omits b) must materialize write-default = 9.
INSERT INTO ${case_db}.t_v3_default_addcol (a) VALUES ROW(3);
SELECT a, b FROM ${case_db}.t_v3_default_addcol ORDER BY a;

-- query 4
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_default_addcol ;
