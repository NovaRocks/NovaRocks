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


-- Typed temporal result domains; independent calendar/epoch oracles.

-- query 1
SET disable_function_fold_constants=off;
SELECT time_slice('2023-12-31 03:12:04',INTERVAL 2147483647 YEAR) AS sliced;

-- query 2
SELECT date_slice('2023-12-31 03:12:04',INTERVAL 2147483647 YEAR) AS sliced;

-- query 3
SELECT time_slice(CAST('2020-01-01' AS DATE),INTERVAL 1 DAY,CEIL) AS sliced;

-- query 4
SELECT date_slice(CAST('2020-01-01 17:00:00' AS DATETIME),INTERVAL 1 DAY,CEIL) AS sliced;

-- query 5
SELECT time_slice('2023-10-31 23:59:59.123456',INTERVAL 17 MICROSECOND,CEIL) AS sliced;

-- query 6
SELECT time_slice('2023-12-31 03:12:04',INTERVAL 2147483647 QUARTER,CEIL) AS sliced;

-- query 7
-- @expect_error=can't use time_slice for date with time(hour/minute/second)
SELECT date_slice(NULL,INTERVAL 1 HOUR) AS sliced;

-- query 8
-- @expect_error=time_slice requires second parameter must be greater than 0
SELECT time_slice(NULL,INTERVAL 0 DAY) AS sliced;

-- query 9
-- @expect_error=date_slice requires second parameter must be a constant interval
SELECT date_slice('2020-01-01',INTERVAL 3.2 DAY) AS sliced;

-- query 10
-- @expect_error=time_slice interval must fit INT32
SELECT time_slice('2020-01-01',INTERVAL 2147483648 DAY) AS sliced;

-- query 11
-- @expect_error=time used with time_slice can't before 0001-01-01 00:00:00
SELECT time_slice('0000-01-01',INTERVAL 1 DAY) AS sliced;

-- query 12
SET disable_function_fold_constants=on;
SELECT time_slice('2023-12-31 03:12:04',INTERVAL 2147483647 YEAR) AS sliced;

-- query 13
SELECT date_slice('2023-12-31 03:12:04',INTERVAL 2147483647 YEAR) AS sliced;

-- query 14
SELECT time_slice('2023-10-31 23:59:59.123456',INTERVAL 17 MICROSECOND,CEIL) AS sliced;

-- query 15
SELECT time_slice('2023-12-31 03:12:04',INTERVAL 2147483647 QUARTER,CEIL) AS sliced;

-- query 16
SELECT time_slice('not a date',INTERVAL 1 DAY) AS sliced;

-- query 17
-- @expect_error=time_slice unsupported unit: century
SELECT time_slice(NULL,1,'century','floor') AS sliced;

-- query 18
-- @expect_error=time_slice expects boundary floor/ceil, got round
SELECT time_slice(NULL,1,'day','round') AS sliced;

-- query 19
-- @expect_error=time_slice first argument cannot be cast to its temporal domain
SELECT time_slice([1],1,'day','floor') AS sliced;

-- query 20
SELECT date_slice(NULL,INTERVAL 1 DAY) AS sliced;
SET disable_function_fold_constants=off;
