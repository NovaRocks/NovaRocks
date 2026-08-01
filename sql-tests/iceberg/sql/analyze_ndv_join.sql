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

-- @tags=iceberg,statistics,ndv
-- Verify ANALYZE publishes the native Puffin statistics artifact and refreshes
-- the same-session statistics view. Theta NDV is deliberately not promoted to
-- an exact optimizer denominator, so this join must retain the bounded
-- many-to-many fallback rather than treating a sketch as authoritative NDV.

-- @skip_result_check=true
CREATE DATABASE iceberg_cat_${suite_uuid0}.ndv_db_${uuid0};

-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.l_${uuid0} (k INT, payload INT);

-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.r_${uuid0} (k INT, flag INT);

-- l: k in [0,99] over 1000 rows -> NDV(k)=100 ; r: k in [0,79] over 800 -> NDV(k)=80
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.l_${uuid0}
  SELECT generate_series % 100, generate_series FROM TABLE(generate_series(1, 1000));

-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.r_${uuid0}
  SELECT generate_series % 80, generate_series % 2 FROM TABLE(generate_series(1, 800));

-- @skip_result_check=true
ANALYZE TABLE iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.l_${uuid0};

-- @skip_result_check=true
ANALYZE TABLE iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.r_${uuid0};

-- The exact Theta mapping is intentionally unavailable. The fallback is
-- |l|*|r|*0.25 = 162000 and must stay explicit and bounded.
-- @explain_contains=HASH JOIN
-- @explain_contains=stats={rows=162000}
EXPLAIN VERBOSE SELECT l.k
FROM iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.l_${uuid0} l
JOIN iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.r_${uuid0} r ON l.k = r.k;

-- @skip_result_check=true
DROP DATABASE iceberg_cat_${suite_uuid0}.ndv_db_${uuid0};
