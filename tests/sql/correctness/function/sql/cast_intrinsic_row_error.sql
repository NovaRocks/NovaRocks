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

-- Independent legacy mode and catching controls; no recorded outputs.
-- query 1
SET disable_function_fold_constants=on;
SET sql_mode='0';
SELECT CAST(CAST(2147483648 AS DOUBLE) AS INT) IS NULL AS caught_range;

-- query 2
-- @expect_error=conflict with range of INT
SELECT /*+ SET_VAR(sql_mode='ALLOW_THROW_EXCEPTION') */ CAST(CAST(2147483648 AS DOUBLE) AS INT) AS narrowed;

-- query 3
SELECT /*+ SET_VAR(sql_mode='ALLOW_THROW_EXCEPTION') */ CAST(CAST(127 AS DOUBLE) AS TINYINT) AS narrowed;

-- query 4
SELECT CAST(unhex('FF') AS VARCHAR) IS NULL AS caught_utf8;

-- query 5
SELECT CAST(CAST('2020-01-01' AS DATE) AS VARCHAR) AS rendered;

-- query 6
-- @skip_result_check=true
SET disable_function_fold_constants=off;
