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
-- the same-session statistics view. The measured Theta NDV then reaches the
-- optimizer as an Estimated statistic -- approximate, but far better than the
-- no-statistics equijoin default -- so this join must size itself from the
-- collected distinct counts rather than from that default.

-- @skip_result_check=true
CREATE SCHEMA iceberg_cat_${suite_uuid0}.ndv_db_${uuid0};

-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.l_${uuid0} (k INT, payload INT);

-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.r_${uuid0} (k INT, flag INT);

-- l: k in [0,99] over 1000 rows -> approx_distinct(k)=100 ; r: k in [0,79] over 800 -> approx_distinct(k)=80
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.l_${uuid0}
  SELECT generate_series % 100, generate_series FROM TABLE(sequence(1, 1000)) AS generate_series(generate_series);

-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.r_${uuid0}
  SELECT generate_series % 80, generate_series % 2 FROM TABLE(sequence(1, 800)) AS generate_series(generate_series);

-- @skip_result_check=true
ANALYZE iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.l_${uuid0};

-- @skip_result_check=true
ANALYZE iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.r_${uuid0};

-- ANALYZE publishes the measured approximate NDV as an Estimated statistic,
-- so the join uses |l|*|r|/max(ndv_l, ndv_r) = 6480.
-- @explain_contains=HASH JOIN
-- @explain_contains=stats={rows=6480}
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'  SELECT l.k
FROM iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.l_${uuid0} l
JOIN iceberg_cat_${suite_uuid0}.ndv_db_${uuid0}.r_${uuid0} r ON l.k = r.k;

-- @skip_result_check=true
DROP SCHEMA iceberg_cat_${suite_uuid0}.ndv_db_${uuid0};
