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
SELECT approx_percentile(x, DOUBLE '0.5') AS median, approx_percentile(x, ARRAY[DOUBLE '0', DOUBLE '1', DOUBLE '0.5']) AS bounds FROM (VALUES BIGINT '2', BIGINT '4', BIGINT '8') t(x);

-- query 2
SELECT approx_percentile(x, DOUBLE '0.5') AS median, approx_percentile(x, ARRAY[DOUBLE '0', DOUBLE '1']) AS bounds FROM (VALUES REAL '2', REAL '4', REAL '8') t(x);

-- query 3
SELECT approx_percentile(x, DOUBLE '0.5') AS median, approx_percentile(x, ARRAY[DOUBLE '0', DOUBLE '1']) AS bounds FROM (VALUES DOUBLE '2', DOUBLE '4', DOUBLE '8') t(x);

-- query 4
WITH requests AS (
    SELECT array_agg(DOUBLE '0.5') AS p FROM TABLE(sequence(1, 4097))
)
SELECT cardinality(approx_percentile(x, p)) AS targets
FROM (VALUES DOUBLE '2', DOUBLE '4', DOUBLE '8') AS samples(x)
CROSS JOIN requests;

-- query 5
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT approx_percentile(x, ARRAY[DOUBLE '0', CAST(NULL AS DOUBLE)]) AS invalid FROM (VALUES DOUBLE '2') t(x);

-- query 6
SELECT approx_percentile(x, p) AS median FROM (VALUES (5, CAST(NULL AS DOUBLE)), (CAST(NULL AS INTEGER), DOUBLE '1.7'), (3, DOUBLE '0.5')) t(x,p);

-- query 7
SELECT approx_percentile(x, DOUBLE '0.5') AS median FROM (VALUES 1) t(x) WHERE false;
