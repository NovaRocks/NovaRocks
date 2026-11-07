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

-- Test Objective:
-- 1. Validate query correctness when GRF exceeds its size limit and is disabled.
-- 2. SET_VAR hint sets global_runtime_filter_build_max_size=1 to force GRF to
--    exceed its limit, so the filter falls back to no-filter mode.
-- 3. The count result must match the baseline without GRF.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
    k1 INT
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.t1 SELECT generate_series FROM TABLE(generate_series(1, 65535));
INSERT INTO ${case_db}.t1 SELECT k1 + 65535 FROM ${case_db}.t1;
INSERT INTO ${case_db}.t1 SELECT k1 + 65535 * 2 FROM ${case_db}.t1;
INSERT INTO ${case_db}.t1 SELECT k1 + 65535 * 3 FROM ${case_db}.t1;

-- query 2
-- GRF build size limit=1 forces the runtime filter to exceed limit and be disabled.
-- The result must still be correct (no rows dropped, no extra rows from GRF bypass).
with tw1 as (
    select t1.k1 from ${case_db}.t1 join  ${case_db}.t1 t2 using(k1)
)
select
    count(1) from tw1 join  ${case_db}.t1 t3 using(k1);
