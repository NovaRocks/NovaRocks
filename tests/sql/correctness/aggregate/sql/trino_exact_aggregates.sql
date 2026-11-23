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

-- query 1
SELECT stddev(x) AS sample_sd, stddev_samp(x) AS sample_sd_alias,
       stddev_pop(x) AS population_sd, variance(x) AS sample_variance,
       var_samp(x) AS sample_variance_alias, var_pop(x) AS population_variance
FROM (VALUES CAST(NULL AS DOUBLE)) t(x)
WHERE false;

-- query 2
SELECT stddev(x) AS sample_sd, stddev_samp(x) AS sample_sd_alias,
       stddev_pop(x) AS population_sd, variance(x) AS sample_variance,
       var_samp(x) AS sample_variance_alias, var_pop(x) AS population_variance
FROM (VALUES DOUBLE '2', CAST(NULL AS DOUBLE)) t(x);

-- query 3
SELECT stddev(x) AS sample_sd, stddev_samp(x) AS sample_sd_alias,
       stddev_pop(x) AS population_sd, variance(x) AS sample_variance,
       var_samp(x) AS sample_variance_alias, var_pop(x) AS population_variance
FROM (VALUES DOUBLE '1', DOUBLE '3') t(x);

-- query 4
SELECT min(x, n) AS smallest, max(x, n) AS largest
FROM (VALUES (1, CAST(NULL AS BIGINT)), (CAST(NULL AS INTEGER), BIGINT '10001')) t(x,n);

-- query 5
SELECT min(x, 10000) AS smallest, max(x, 10000) AS largest
FROM (VALUES 2, 1, 2) t(x);

-- query 6
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT max(x, 10001) FROM (VALUES 1) t(x);

-- query 7
SELECT array_agg(x) FILTER (WHERE false) AS empty_array,
       array_agg(x) FILTER (WHERE x IS NULL) AS selected_null,
       map_agg(x, v) FILTER (WHERE false) AS empty_map,
       map_agg(x, v) AS valid_key_with_null_value
FROM (VALUES (1, CAST(NULL AS INTEGER)), (CAST(NULL AS INTEGER), 2)) t(x,v);

-- query 8
SELECT bool_or(x) AS some_true, bool_and(x) AS all_true, every(x) AS all_true_alias
FROM (VALUES true, false, CAST(NULL AS BOOLEAN)) t(x);
