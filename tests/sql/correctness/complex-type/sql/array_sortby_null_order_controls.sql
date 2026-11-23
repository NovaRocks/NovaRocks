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

-- Independent, small controls for key NULL policy, value NULL identity, and ties.
-- @order_sensitive=true

-- query 1
SELECT array_agg(payload_value ORDER BY k1 NULLS FIRST, k2 NULLS FIRST, ordinal) AS value
FROM (VALUES (1, 10, 2, 0), (2, NULL, NULL, 2), (3, 30, NULL, 1), (4, 40, 1, 0)) AS input_rows(ordinal, payload_value, k1, k2);

-- query 2
SELECT array_agg(payload_value ORDER BY k1 NULLS FIRST, ordinal) AS value
FROM (VALUES (1, 30, NULL), (2, 10, NULL), (3, 20, NULL)) AS input_rows(ordinal, payload_value, k1);

-- query 3
SELECT array_agg(payload_value ORDER BY k1 NULLS FIRST, ordinal) AS value
FROM (VALUES (1, 9, 3), (2, 8, 1), (3, 7, 2)) AS input_rows(ordinal, payload_value, k1);

-- query 4
SELECT array_agg(payload_value ORDER BY k1 NULLS FIRST, k2 NULLS FIRST, ordinal) AS value
FROM (VALUES (1, 30, 1, 1), (2, 10, 1, 1), (3, 40, 1, 1), (4, 20, 1, 1)) AS input_rows(ordinal, payload_value, k1, k2);

-- query 5
SELECT CAST(NULL AS ARRAY(INTEGER)) AS value;

-- query 6
SELECT ARRAY[] AS value;

-- query 7
SELECT array_agg(payload_value ORDER BY k1 NULLS FIRST, ordinal) AS value
FROM (VALUES (1, NULL, 3), (2, 8, 1), (3, 7, 2)) AS input_rows(ordinal, payload_value, k1);

-- query 8
WITH inputs AS (
    SELECT generate_series AS id
    FROM TABLE(sequence(1,26)) AS generate_series(generate_series)
), positions (ordinal) AS (
    VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)
), sorted AS (
    SELECT id, array_agg(ordinal ORDER BY
        CASE WHEN id % 13 = 0 THEN NULL ELSE ordinal % 3 END NULLS FIRST,
        ordinal) AS value
    FROM inputs CROSS JOIN positions
    GROUP BY id
)
SELECT count(*) AS row_count,
    sum(CASE WHEN id % 13 = 0 THEN 1 ELSE 0 END) AS null_key_rows,
    sum(CASE WHEN value = CASE WHEN id % 13 = 0 THEN ARRAY[1,2,3,4,5,6,7,8,9,10]
                               ELSE ARRAY[3,6,9,1,4,7,10,2,5,8] END
             THEN 1 ELSE 0 END) AS correct_order_rows
FROM sorted;
