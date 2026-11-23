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
SELECT approx_percentile(x,w,DOUBLE '0.5') AS fractional, approx_percentile(x, floor(w), DOUBLE '0.5') AS integral FROM (VALUES (BIGINT '0', DOUBLE '1.1'), (BIGINT '10', DOUBLE '1.9'), (BIGINT '20', DOUBLE '1.1'), (BIGINT '30', DOUBLE '1.9')) t(x,w);

-- query 2
SELECT approx_percentile(x,w,ARRAY[DOUBLE '0', DOUBLE '0.25', DOUBLE '0.5', DOUBLE '1']) AS estimates FROM (VALUES (DOUBLE '2', DOUBLE '1'), (DOUBLE '3', DOUBLE '2'), (DOUBLE '4', DOUBLE '3'), (NULL, DOUBLE '100'), (DOUBLE '100', NULL)) t(x,w);

-- query 3
SELECT approx_percentile(x,w,DOUBLE '0.5') IS NULL AS empty_state FROM (VALUES (CAST(NULL AS DOUBLE), DOUBLE '0'), (DOUBLE '10', CAST(NULL AS DOUBLE))) t(x,w);

-- query 4
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT approx_percentile(x, DOUBLE '0', DOUBLE '0.5') AS invalid FROM (VALUES DOUBLE '2') t(x);
