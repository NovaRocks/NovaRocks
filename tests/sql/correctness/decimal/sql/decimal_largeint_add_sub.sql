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

-- Test Objective: freeze exact mixed Decimal/LARGEINT Add/Sub with VARCHAR output.
-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.decimal_largeint_pair (id INT, d DECIMAL(38,15), n LARGEINT)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.decimal_largeint_pair VALUES
(1,1.250000000000000,170141183460469231731687303715884105727),
(2,-1.250000000000000,-170141183460469231731687303715884105728),
(3,NULL,1),(4,1.250000000000000,NULL);

-- query 2
SELECT id,CAST(d+n AS VARCHAR) AS add_forward,CAST(n+d AS VARCHAR) AS add_reverse,
CAST(d-n AS VARCHAR) AS sub_forward,CAST(n-d AS VARCHAR) AS sub_reverse
FROM ${case_db}.decimal_largeint_pair ORDER BY id;

-- query 3
SELECT CAST(CAST(0.000000000000000000000000000000000001 AS DECIMAL(38,36)) +
CAST('170141183460469231731687303715884105727' AS LARGEINT) AS VARCHAR) AS precision76,
CAST(CAST(-0.000000000000000000000000000000000001 AS DECIMAL(38,36)) +
CAST('-170141183460469231731687303715884105728' AS LARGEINT) AS VARCHAR) AS negative76;

-- query 4
-- @expect_error=no frozen result rule
SELECT CAST(0 AS DECIMAL(38,37)) + CAST(1 AS LARGEINT);

-- query 5
-- @expect_error=no frozen result rule
SELECT CAST(1 AS DECIMAL(38,15)) * CAST(1 AS LARGEINT);

-- query 6
-- @expect_error=no frozen result rule
SELECT CAST(1 AS DECIMAL(38,15)) / CAST(1 AS LARGEINT);

-- query 7
-- @expect_error=no frozen result rule
SELECT CAST(1 AS DECIMAL(38,15)) % CAST(1 AS LARGEINT);

-- query 8
-- @skip_result_check=true
DROP TABLE ${case_db}.decimal_largeint_pair;
