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

-- query 1
WITH samples(g, x, w) AS (
    VALUES (1, DOUBLE '0', DOUBLE '1.1'), (1, DOUBLE '10', DOUBLE '1.9'),
           (1, DOUBLE '20', DOUBLE '1.1'), (1, DOUBLE '30', DOUBLE '1.9'),
           (2, DOUBLE '2', DOUBLE '1'), (2, DOUBLE '3', DOUBLE '2'),
           (2, DOUBLE '4', DOUBLE '3'), (3, CAST(NULL AS DOUBLE), DOUBLE '1')
)
SELECT g,
       CAST(approx_percentile(x, w, DOUBLE '0.5') AS DECIMAL(9, 3)) AS median,
       approx_percentile(x, w, ARRAY[DOUBLE '0', DOUBLE '1']) AS endpoints
FROM samples GROUP BY g ORDER BY g;

-- query 2
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT approx_percentile(x, DOUBLE '1', DOUBLE '0.5', DOUBLE '0.01')
FROM (VALUES BIGINT '2') t(x);
