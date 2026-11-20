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
-- @tags=aggregate,repeat,multicast,pipeline
-- Test Objective:
-- 1. Validate CUBE semantics under multi-cast data stream sinks with pipeline parallelism.
-- 2. Prevent regressions where early EOS truncates exchange data when pipeline_dop > 1.
-- Test Flow:
-- 1. Force pipeline parallel execution with pipeline_dop = 5.
-- 2. Run a minimal CUBE query that triggers MULTI_CAST_DATA_STREAM_SINK and REPEAT paths.
-- 3. Assert deterministic full result set with detail, subtotal, and grand-total rows.
SET SESSION pipeline_dop = 5;

WITH t AS (
    SELECT 1 AS a, 'x' AS b
    UNION ALL
    SELECT 1, 'y'
    UNION ALL
    SELECT 2, 'z'
)
SELECT a, b
FROM t
GROUP BY CUBE(a, b)
ORDER BY a, b;

SET SESSION pipeline_dop = 0;
