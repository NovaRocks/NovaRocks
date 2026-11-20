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


-- Test IN and NOT IN over typed scan values and all three Boolean outcomes.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.membership_values (
    id INTEGER, truth BOOLEAN, f REAL, d DOUBLE, n DECIMAL(12, 2),
    day DATE, stamp TIMESTAMP, bytes VARBINARY, big LARGEINT
) WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.membership_values VALUES
    ROW(1, TRUE, 1.25, 2.5, 3.75, DATE '2024-11-20', TIMESTAMP '2024-11-20 01:02:03', X'CAFE',
        CAST('123456789012345678901234567890' AS LARGEINT)),
    ROW(2, FALSE, 2.25, 3.5, 4.75, DATE '2024-11-21', TIMESTAMP '2024-11-21 01:02:03', X'FACE',
        CAST('123456789012345678901234567891' AS LARGEINT)),
    ROW(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

-- query 3
SELECT id,
    truth IN (TRUE, NULL) AS b,
    f IN (CAST(1.25 AS REAL), NULL) AS f,
    d IN (CAST(2.5 AS DOUBLE), NULL) AS d,
    n IN (DECIMAL '3.75', NULL) AS n,
    day IN (DATE '2024-11-20', NULL) AS day,
    stamp IN (TIMESTAMP '2024-11-20 01:02:03', NULL) AS stamp,
    bytes IN (X'CAFE', NULL) AS bytes,
    big IN (CAST('123456789012345678901234567890' AS LARGEINT), NULL) AS big
FROM ${case_db}.membership_values ORDER BY id;

-- query 4
SELECT id,
    truth NOT IN (TRUE, TRUE) AS b,
    f NOT IN (CAST(1.25 AS REAL), CAST(8.25 AS REAL)) AS f,
    d NOT IN (CAST(2.5 AS DOUBLE), CAST(9.5 AS DOUBLE)) AS d,
    n NOT IN (DECIMAL '3.75', DECIMAL '9.75') AS n,
    day NOT IN (DATE '2024-11-20', DATE '2024-11-22') AS day,
    stamp NOT IN (TIMESTAMP '2024-11-20 01:02:03', TIMESTAMP '2024-11-22 01:02:03') AS stamp,
    bytes NOT IN (X'CAFE', X'FACA') AS bytes,
    big NOT IN (CAST('123456789012345678901234567890' AS LARGEINT),
        CAST('123456789012345678901234567892' AS LARGEINT)) AS big
FROM ${case_db}.membership_values ORDER BY id;

-- query 5
SELECT CAST(1.25 AS DOUBLE) IN (CAST(1.25 AS DOUBLE), NULL) AS folded_in,
    CAST(2.25 AS DOUBLE) NOT IN (CAST(1.25 AS DOUBLE), NULL) AS folded_not_in;
