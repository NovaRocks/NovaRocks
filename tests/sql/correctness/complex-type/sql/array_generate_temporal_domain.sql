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

-- Test Objective: exercise Trino scalar sequence on typed date and timestamp endpoints.
-- query 1
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE temporal_range (id INT, d DATE, t TIMESTAMP(6), v VARCHAR(40)) WITH (format_version = 3);
INSERT INTO temporal_range VALUES
ROW(1,DATE '2025-10-01',TIMESTAMP '2025-10-01 01:00:00','2025-10-02'),
ROW(2,DATE '2025-10-02',TIMESTAMP '2025-10-02 01:00:00','abc'),
ROW(3,NULL,NULL,NULL);

-- query 2
USE ${case_db};
SELECT id, sequence(CAST(d AS TIMESTAMP(6)), t, INTERVAL '1' DAY) AS forward, sequence(t, CAST(d AS TIMESTAMP(6)), INTERVAL '-1' DAY) AS reverse, sequence(d, TRY_CAST(v AS DATE), INTERVAL '1' DAY) AS anchored_text FROM temporal_range ORDER BY id;

-- query 3
SELECT sequence(DATE '2025-10-01',DATE '2025-10-03') AS dates, sequence(DATE '2025-01-31',DATE '2025-04-30', INTERVAL '1' MONTH) AS month_end, sequence(DATE '2025-10-03',DATE '2025-10-01') AS descending;

-- query 4
SELECT sequence(CAST(TIMESTAMP '2025-10-01 14:28:31' AS TIMESTAMP(6)),CAST(TIMESTAMP '2025-10-01 14:28:32.800' AS TIMESTAMP(6)), INTERVAL '0.5' SECOND) AS forward, sequence(CAST(TIMESTAMP '2025-10-01 14:28:32.800' AS TIMESTAMP(6)),CAST(TIMESTAMP '2025-10-01 14:28:31' AS TIMESTAMP(6)), INTERVAL '-0.5' SECOND) AS reverse;

-- query 5
SELECT sequence(CAST(TIMESTAMP '2025-10-01 00:00:00' AS TIMESTAMP(6)),CAST(TIMESTAMP '2025-10-02 00:00:00' AS TIMESTAMP(6)), INTERVAL '12' HOUR) AS subday, sequence(CAST(DATE '2025-10-01' AS TIMESTAMP(6)),CAST(TIMESTAMP '2025-10-02 01:00:00' AS TIMESTAMP(6)), INTERVAL '12' HOUR) AS mixed;

-- query 6
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT sequence('2025-10-01','2025-10-02');

-- query 7
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT sequence(DATE '2025-10-01',DATE '2025-10-02', INTERVAL '0' DAY);

-- query 8
SELECT sequence(CAST(NULL AS DATE),DATE '2025-10-01',INTERVAL '1' DAY) AS null_bound, sequence(1,1,NULL) AS numeric_null_step;

-- query 9
WITH temporal(id,d) AS (VALUES (2,DATE '2025-10-01')) SELECT sequence(d,d + INTERVAL '3' DAY,id * INTERVAL '1' DAY) AS value FROM temporal;

-- query 10
SELECT sequence(DATE '2025-10-01',DATE '2025-10-02',CAST(NULL AS INTERVAL DAY TO SECOND)) AS value;

-- query 11
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT sequence(DATE '2025-10-01',DATE '2025-10-02',INTERVAL '-1' DAY);

-- query 12
-- @expect_trino_error=INVALID_LITERAL,USER_ERROR
SELECT sequence(DATE '2025-02-30',DATE '2025-10-02');

-- query 13
SELECT sequence(3,1) AS default_desc, sequence(1,5,2) AS numeric_step, sequence(1,NULL,1) AS numeric_null;

-- query 14
SELECT sequence(DATE '2025-10-01',DATE '2025-10-02',INTERVAL '2147483647' MONTH) AS value;

-- query 15
-- @skip_result_check=true
USE ${case_db};
DROP TABLE temporal_range;

-- query 16
WITH endpoints(early, late) AS (
    VALUES (TIMESTAMP '2024-01-01 00:00:00.0000001', TIMESTAMP '2024-01-01 00:00:00.0000002')
)
SELECT sequence(early, late, INTERVAL '1' MONTH) AS forward,
       sequence(late, early, INTERVAL '-1' MONTH) AS reverse,
       sequence(late, late, INTERVAL '-1' MONTH) AS same_endpoint
FROM endpoints;

-- query 17
-- The endpoints share a microsecond. Direction uses their full precision.
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT sequence(TIMESTAMP '2024-01-01 00:00:00.0000001',
                TIMESTAMP '2024-01-01 00:00:00.0000002', INTERVAL '-1' MONTH);

-- query 18
SELECT TRY(cardinality(sequence(TIMESTAMP '2024-01-01 00:00:00.0000002',
                                TIMESTAMP '2024-01-01 00:00:00.0000001', INTERVAL '1' MONTH)))
           AS wrong_reverse;

-- query 19
SELECT sequence(CAST(TIMESTAMP '1969-12-31 23:59:59' AS TIMESTAMP(0)),
                CAST(TIMESTAMP '1970-01-01 00:00:01' AS TIMESTAMP(0)),
                INTERVAL '0.999999999999' SECOND(2,12)) AS picosecond_step;

-- query 20
SELECT cardinality(sequence(1, 10000)) AS sequence_limit,
       TRY(cardinality(sequence(1, 10001))) AS sequence_over_limit,
       cardinality(repeat(1, 100000)) AS repeat_limit,
       TRY(cardinality(repeat(1, 100001))) AS repeat_over_limit;

-- query 21
WITH input(low_precision, high_precision) AS (
    VALUES (TIMESTAMP '1969-12-31 23:59:59.999999', TIMESTAMP '1969-12-31 23:59:59.9999990'),
           (TIMESTAMP '1970-01-01 00:00:00.000001', TIMESTAMP '1970-01-01 00:00:00.0000011')
)
SELECT contains(ARRAY[low_precision], high_precision) AS hit,
       contains_sequence(ARRAY[ARRAY[low_precision]], ARRAY[ARRAY[high_precision]]) AS nested_hit,
       array_intersect(ARRAY[low_precision], ARRAY[high_precision]) AS intersection
FROM input
ORDER BY low_precision;
