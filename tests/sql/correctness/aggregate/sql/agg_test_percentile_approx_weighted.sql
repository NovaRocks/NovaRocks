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
-- Values and types captured from Trino 484.

-- query 1
SELECT approx_percentile(x, w, 0.5) AS median, approx_percentile(x,w,ARRAY[DOUBLE '0',DOUBLE '0.5',DOUBLE '1']) AS bounds FROM (VALUES (BIGINT '0', DOUBLE '1.1'), (BIGINT '10', DOUBLE '1.9'), (BIGINT '20', DOUBLE '1.1'), (BIGINT '30', DOUBLE '1.9')) t(x,w);

-- query 2
SELECT approx_percentile(x, w, 0.5) AS median, approx_percentile(x,w,ARRAY[DOUBLE '0',DOUBLE '1']) AS bounds FROM (VALUES (REAL '0', DOUBLE '1.1'), (REAL '10', DOUBLE '1.9'), (REAL '20', DOUBLE '1.1'), (REAL '30', DOUBLE '1.9')) t(x,w);

-- query 3
SELECT approx_percentile(x, w, 0.5) AS median, approx_percentile(x,w,ARRAY[DOUBLE '0',DOUBLE '1']) AS bounds FROM (VALUES (DOUBLE '0', DOUBLE '1.1'), (DOUBLE '10', DOUBLE '1.9'), (DOUBLE '20', DOUBLE '1.1'), (DOUBLE '30', DOUBLE '1.9')) t(x,w);

-- query 4
SELECT approx_percentile(x, w, p) AS median FROM (VALUES (BIGINT '2', DOUBLE '0.5', CAST(NULL AS DOUBLE)), (BIGINT '4', DOUBLE '2', DOUBLE '0.5'), (CAST(NULL AS BIGINT), DOUBLE '0', DOUBLE '1.5')) t(x,w,p);

-- query 5
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT approx_percentile(x, DOUBLE '0.5', DOUBLE '0.5') AS invalid FROM (VALUES DOUBLE '2') t(x);

-- query 6
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT approx_percentile(x, DOUBLE '1.0', DOUBLE '1.5') AS invalid FROM (VALUES BIGINT '2') t(x);

-- query 7
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT approx_percentile(x, DOUBLE '0.5') AS invalid FROM (VALUES BIGINT '9007199254740993') t(x);

-- query 8
SELECT approx_percentile(x, DOUBLE '0.5') AS median FROM (VALUES BIGINT '9007199254740994') t(x);

-- query 9
SELECT approx_percentile(x, ARRAY[DOUBLE '0', DOUBLE '1']) AS bounds FROM (VALUES BIGINT '-9223372036854775808', BIGINT '9223372036854775807') t(x);
