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

-- Migrated from dev/test/sql/test_agg/R/test_tinyint_agg
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained SQL test case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_tinyint_agg @mac
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE "t1" (
  "tinyint_col_1" tinyint NOT NULL,
  "tinyint_col_2" tinyint
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into t1 values ROW(1, 1), ROW(1, 2), ROW(1,3), ROW(1,4), ROW(2, null), ROW(3, null), ROW(4, null);

-- query 4
USE ${case_db};
select count(distinct tinyint_col_1) from t1;

-- query 5
USE ${case_db};
select count(distinct tinyint_col_2) from t1;
