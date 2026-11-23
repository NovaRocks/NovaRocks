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

-- Migrated from dev/test/sql/test_function/T/test_greatest_least_coalesce_datetime
-- Mixed DATE and TIMESTAMP arguments use their full common declaration.
-- All-DATE arguments keep DATE and numeric inputs preserve their original type.

-- query 1
SELECT CAST(GREATEST(DATE '2026-01-14', TIMESTAMP '2026-01-11 00:00:00.000000') AS DATE);

-- query 2
SELECT GREATEST(DATE '2026-01-11', TIMESTAMP '2026-01-14 00:00:00.000000');

-- query 3
SELECT typeof(GREATEST(DATE '2026-01-11', TIMESTAMP '2026-01-14 00:00:00.000000'));

-- query 4
SELECT LEAST(DATE '2026-01-14', TIMESTAMP '2026-01-11 00:00:00.000000');

-- query 5
SELECT typeof(LEAST(DATE '2026-01-14', TIMESTAMP '2026-01-11 00:00:00.000000'));

-- query 6
SELECT COALESCE(NULL, DATE '2026-01-11', TIMESTAMP '2026-01-14 00:00:00.000000');

-- query 7
SELECT typeof(COALESCE(NULL, DATE '2026-01-11', TIMESTAMP '2026-01-14 00:00:00.000000'));

-- query 8
SELECT GREATEST(DATE '2026-01-11', DATE '2026-01-14', DATE '2026-01-09');

-- query 9
SELECT typeof(GREATEST(DATE '2026-01-11', DATE '2026-01-14', DATE '2026-01-09'));

-- query 10
-- Verify no regression: numeric types should still work correctly
select typeof(greatest(cast(100 as int), cast(200 as bigint)));

-- query 11
select typeof(greatest(20260114, 20260115));

-- query 12
SELECT
    GREATEST(DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00.000000000001') AS after_epoch,
    LEAST(TIMESTAMP '1970-01-01 00:00:00.000000000001', TIMESTAMP '1970-01-01 00:00:00.000000') AS epoch,
    GREATEST(TIMESTAMP '1970-01-01 00:00:00.000000000001', CAST(NULL AS TIMESTAMP(12))) AS null_wide;

-- query 13
SELECT
    GREATEST(TIMESTAMP '2000-01-01 01:00:00.000000000001 +01:00', TIMESTAMP '2000-01-01 00:00:00.000000000001 UTC') AS first_instant,
    LEAST(TIME '00:00:00.000000000001 -01:00', TIME '01:00:00.000000000001 +00:00') AS first_clock;

-- query 14
SELECT
    GREATEST(INTERVAL '1.000000000001' SECOND, INTERVAL '1.000000000002' SECOND) AS elapsed;
