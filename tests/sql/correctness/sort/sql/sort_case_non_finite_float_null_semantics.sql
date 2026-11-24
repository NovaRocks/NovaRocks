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
-- @tags=sort,float,null_semantics,case
-- Test Objective:
-- Verify ascending order with NULLS FIRST when CASE returns NULL, NaN, infinity, or a finite value.
-- query 1
WITH t AS (SELECT 1 AS id, CAST(NULL AS DOUBLE) AS d UNION ALL SELECT 2, (- (DECIMAL '3.5')) UNION ALL SELECT 3, DECIMAL '0.0' UNION ALL SELECT 4, DECIMAL '7.25' UNION ALL SELECT 5, (- (DECIMAL '7.25')) UNION ALL SELECT 6, DECIMAL '1.0' UNION ALL SELECT 7, DECIMAL '3.5' UNION ALL SELECT 8, NULL UNION ALL SELECT 9, DECIMAL '2.2' UNION ALL SELECT 10, (- (DECIMAL '0.0'))) SELECT id, CASE WHEN (id = 1) THEN CAST('NaN' AS DOUBLE) WHEN (id = 2) THEN CAST('Infinity' AS DOUBLE) WHEN (id = 3) THEN CAST('-Infinity' AS DOUBLE) ELSE d END AS k FROM t ORDER BY k ASC NULLS FIRST, id ASC;

-- query 2
-- Common zero sort key, with the original positive and negative zeros retained.
SELECT id, k
FROM (VALUES
    (1, CAST(NULL AS DOUBLE)), (2, CAST('NaN' AS DOUBLE)), (3, CAST('-Infinity' AS DOUBLE)),
    (4, CAST('-0.0' AS DOUBLE)), (5, 0e0), (6, CAST('Infinity' AS DOUBLE))
) AS t(id, k)
ORDER BY k ASC NULLS LAST, id DESC;

-- query 3
SELECT id, k
FROM (VALUES
    (1, CAST(NULL AS DOUBLE)), (2, CAST('NaN' AS DOUBLE)), (3, CAST('-Infinity' AS DOUBLE)),
    (4, CAST('-0.0' AS DOUBLE)), (5, 0e0), (6, CAST('Infinity' AS DOUBLE))
) AS t(id, k)
ORDER BY k DESC NULLS FIRST, id DESC;

-- query 4
-- Apply the same key policy when a bounded sort retains REAL rows.
SELECT id, k
FROM (VALUES
    (1, CAST(NULL AS REAL)), (2, CAST('NaN' AS REAL)), (3, CAST('-Infinity' AS REAL)),
    (4, CAST('-0.0' AS REAL)), (5, REAL '0.0'), (6, CAST('Infinity' AS REAL))
) AS t(id, k)
ORDER BY k ASC NULLS FIRST, id DESC
LIMIT 4;
