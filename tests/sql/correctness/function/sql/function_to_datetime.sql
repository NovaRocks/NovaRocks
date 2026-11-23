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

-- Migrated from dev/test/sql/test_datetime/T/test_to_datetime
-- Epoch conversions at a fixed +08:00 offset, including exact microseconds
-- and the explicit scale selection required by the migrated caller.

-- query 1
-- @skip_result_check=true

-- query 2
-- @skip_result_check=true

-- query 3

-- query 4
SELECT CAST(from_unixtime(DOUBLE '1598306400', '+08:00') AS TIMESTAMP(6));

-- query 5
SELECT to_unixtime(from_unixtime(DOUBLE '1598306400', '+08:00'));

-- query 6
SELECT CAST(from_unixtime(DOUBLE '1598306400.123', '+08:00') AS TIMESTAMP(6));

-- query 7
SELECT TIMESTAMP '1970-01-01 08:00:00.000000'
       + BIGINT '1598306400123456' * INTERVAL '0.000001' SECOND(13, 6);

-- query 8
SELECT CAST(from_unixtime(DOUBLE '0', '+08:00') AS TIMESTAMP(6));

-- query 9
SELECT CAST(from_unixtime(DOUBLE '253402243199', '+08:00') AS TIMESTAMP(6));

-- query 10
SELECT CASE WHEN 10 IN (0, 3, 6) THEN CAST(from_unixtime(DOUBLE '1598306400', '+08:00') AS TIMESTAMP(6)) END;

-- query 11
SELECT CASE WHEN CAST(NULL AS INTEGER) IN (0, 3, 6) THEN CAST(from_unixtime(DOUBLE '1598306400', '+08:00') AS TIMESTAMP(6)) END;

-- query 12
SELECT CAST(from_unixtime(CAST(NULL AS DOUBLE), '+08:00') AS TIMESTAMP(6));

-- query 13
-- @order_sensitive=true
SELECT CASE sc_val
       WHEN 0 THEN CAST(from_unixtime(CAST(ts_val AS DOUBLE), '+08:00') AS TIMESTAMP(6))
       WHEN 3 THEN CAST(from_unixtime(CAST(ts_val AS DOUBLE) / DOUBLE '1000', '+08:00') AS TIMESTAMP(6))
       WHEN 6 THEN TIMESTAMP '1970-01-01 08:00:00.000000'
                   + ts_val * INTERVAL '0.000001' SECOND(13, 6)
       END AS dyn_scale_timezone_aware
FROM (VALUES
        ROW(1, BIGINT '1598306400123456', 6),
        ROW(2, BIGINT '1598306400123',    3),
        ROW(3, BIGINT '1598306400',       0),
        ROW(4, BIGINT '-1001',            3),
        ROW(5, CAST(NULL AS BIGINT),      0),
        ROW(6, BIGINT '1598306400',       CAST(NULL AS INTEGER))
     ) AS t(ord, ts_val, sc_val)
ORDER BY ord;
