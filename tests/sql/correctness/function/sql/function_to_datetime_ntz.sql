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

-- Migrated from dev/test/sql/test_datetime/T/test_to_datetime_ntz
-- Test Objective:
-- 1. Validate to_datetime_ntz(ts, scale) converts Unix timestamps to UTC DATETIME (timezone-free).
-- 2. Validate scale=0 (seconds), scale=3 (milliseconds), scale=6 (microseconds).
-- 3. Validate default call to_datetime_ntz(ts) without scale uses scale=0.
-- 4. Validate invalid scale (>6) returns NULL.
-- 5. Validate NULL input handling.
-- 6. Validate dynamic scale via inline VALUES table.
-- Results are always UTC regardless of session timezone.

-- query 1
-- @skip_result_check=true

-- query 2
-- scale=0: seconds; 1598306400 → 2020-08-24 22:00:00 UTC
SELECT to_datetime_ntz(1598306400, 0);

-- query 3
-- Default (no scale): same as scale=0
SELECT to_datetime_ntz(1598306400);

-- query 4
-- scale=3: milliseconds; 1598306400123 → 2020-08-24 22:00:00.123000 UTC
SELECT to_datetime_ntz(1598306400123, 3);

-- query 5
-- scale=6: microseconds; 1598306400123456 → 2020-08-24 22:00:00.123456 UTC
SELECT to_datetime_ntz(1598306400123456, 6);

-- query 6
-- Unix epoch 0 → 1970-01-01 00:00:00 UTC
SELECT to_datetime_ntz(0, 0);

-- query 7
-- Max valid timestamp: 253402243199 → 9999-12-31 07:59:59 UTC
SELECT to_datetime_ntz(253402243199, 0);

-- query 8
-- Invalid scale=10 (>6) → NULL
SELECT to_datetime_ntz(1598306400, 10);

-- query 9
-- NULL scale → NULL
SELECT to_datetime_ntz(1598306400, null);

-- query 10
-- NULL timestamp → NULL
SELECT to_datetime_ntz(null, null);

-- query 11
-- @order_sensitive=true
-- Dynamic scale via inline VALUES: mix of scales, negative ts, NULL inputs
SELECT to_datetime_ntz(t.ts_val, t.sc_val) AS dyn_scale
FROM (VALUES
        (1598306400123456, 6),
        (1598306400123,    3),
        (1598306400,       0),
        (-1001,            3),
        (NULL,             0),
        (1598306400,       NULL)
     ) AS t(ts_val, sc_val);
