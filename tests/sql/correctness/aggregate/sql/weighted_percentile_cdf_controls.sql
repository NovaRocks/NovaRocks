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
-- Weighted CDF reference: centers .5,2,4.5 for (2,w1),(3,w2),(4,w3).
-- q=.5 interpolates 3.4; q=.25 interpolates 8/3. NULL/zero rows add no mass.
-- query 1
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE weighted_cdf_control (v DOUBLE, w BIGINT) TBLPROPERTIES ("format-version" = "3");
INSERT INTO weighted_cdf_control VALUES (2,1),(3,2),(4,3),(NULL,100),(100,NULL),(200,0),(999,0);

-- query 2
SELECT COUNT(*) AS rows_seen, COUNT(v) AS values_seen, COUNT(w) AS weights_seen FROM weighted_cdf_control;

-- query 3
SELECT percentile_approx_weighted(v,w,0.5,10000) AS p FROM weighted_cdf_control;

-- query 4
SELECT percentile_approx_weighted(v,w,[0.0,0.25,0.5,1.0],2048) AS p FROM weighted_cdf_control;

-- query 5
SELECT percentile_approx_weighted(v,0,0.5) IS NULL AS empty FROM weighted_cdf_control;

-- query 6
SELECT percentile_approx_weighted(v,w,0.5) IS NULL AS empty FROM weighted_cdf_control WHERE v IS NULL;

-- query 7
SELECT percentile_approx_weighted(v,w,0.5) IS NULL AS empty FROM weighted_cdf_control WHERE w IS NULL;

-- query 8
-- @expect_error=percentile weight must be non-negative
SELECT percentile_approx_weighted(v,-1,0.5) FROM weighted_cdf_control;

-- query 9
SELECT percentile_approx_weighted(1,1,0.5) AS p FROM weighted_cdf_control;

-- query 10
-- @expect_error=compression parameter must be positive
SELECT percentile_approx_weighted(v,w,0.5,0) FROM weighted_cdf_control;

-- query 11
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE weighted_cdf_control;
