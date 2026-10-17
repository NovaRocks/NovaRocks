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

-- Null-valued MAP lookups retain valid NullArray storage for both hits and misses.
-- @order_sensitive=true

-- query 1
SELECT map{1:NULL}[1] AS found_value, map{1:NULL}[2] AS missing_value,
       element_at(map{1:NULL}, 1) AS explicit_call;

-- query 2
WITH input_values AS (
    SELECT 1 AS id, map{1:NULL} AS m
    UNION ALL SELECT 2, map{2:NULL}
    UNION ALL SELECT 3, NULL
)
SELECT id, m[1] AS found_value, m[99] AS missing_value
FROM input_values ORDER BY id;
