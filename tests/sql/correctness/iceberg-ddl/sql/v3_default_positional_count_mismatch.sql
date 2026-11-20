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
-- Test Point: Positional INSERT INTO t VALUES (...) with fewer values than columns continues to error (write-default does NOT auto-fill positional INSERT).
-- Method: CREATE v3 table with 2 columns where the second has DEFAULT 5, INSERT positional with only one value, expect a count-mismatch error.
-- Scope: D4 — positional INSERT semantics are unchanged.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_default_positional ;
CREATE TABLE ${case_db}.t_v3_default_positional (
  a INT,
  b INT DEFAULT 5
) WITH (format_version = 3);

-- query 2
-- @expect_error=mismatch
INSERT INTO ${case_db}.t_v3_default_positional VALUES ROW(1);

-- query 3
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_default_positional ;
