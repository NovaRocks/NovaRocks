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

-- Test Objective: untyped NULL elements obey ordered-array null policies.
-- @order_sensitive=true

-- query 1
SELECT array_agg(payload_value ORDER BY k1 NULLS FIRST, ordinal) AS ties
FROM (VALUES (1, 30, NULL), (2, 10, NULL), (3, 20, NULL)) AS input_rows(ordinal, payload_value, k1);

-- query 2
SELECT array_agg(payload_value ORDER BY k1 NULLS FIRST, k2 NULLS FIRST, ordinal) AS refined
FROM (VALUES (1, 30, NULL, 3), (2, 10, NULL, 1), (3, 20, NULL, 2)) AS input_rows(ordinal, payload_value, k1, k2);

-- query 3
SELECT array_agg(payload_value ORDER BY k1 NULLS FIRST, k2 NULLS FIRST, ordinal) AS trailing_null_key
FROM (VALUES (1, 30, 3, NULL), (2, 10, 1, NULL), (3, 20, 2, NULL)) AS input_rows(ordinal, payload_value, k1, k2);

-- query 4
SELECT array_sort(ARRAY[NULL,NULL,NULL]) AS sorted, array_min(ARRAY[NULL,NULL,NULL]) AS smallest, array_max(ARRAY[NULL,NULL,NULL]) AS largest;

-- query 5
SELECT array_agg(payload_value ORDER BY k1 NULLS FIRST, k2 NULLS FIRST, ordinal) AS null_value_moves
FROM (VALUES (1, NULL, NULL, 3), (2, 8, NULL, 1), (3, 7, NULL, 2)) AS input_rows(ordinal, payload_value, k1, k2);

-- query 6
SELECT array_min(ARRAY[TINYINT '2', TINYINT '1']) AS narrow_min,
       array_max(ARRAY[SMALLINT '2', SMALLINT '1']) AS narrow_max;
