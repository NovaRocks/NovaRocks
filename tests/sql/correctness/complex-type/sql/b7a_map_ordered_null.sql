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
-- Force ordered argument demand to observe a cast collision only on active rows.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.b7a_map_order (id INTEGER, skip_map BOOLEAN)
WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.b7a_map_order VALUES ROW(1, true), ROW(2, false);

-- query 3
WITH inputs AS (
    SELECT id, CASE WHEN skip_map THEN CAST(NULL AS MAP(DOUBLE, INTEGER))
                    ELSE CAST(map() AS MAP(DOUBLE, INTEGER)) END AS first_map,
           MAP(ARRAY[BIGINT '9007199254740991' + id, BIGINT '9007199254740992' + id],
               ARRAY[7, 8]) AS second_map
    FROM ${case_db}.b7a_map_order
)
SELECT id, cardinality(map_concat(first_map, second_map)) AS entry_count
FROM inputs ORDER BY id;

-- query 4
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT map_concat(
    MAP(ARRAY[BIGINT '9007199254740991' + id, BIGINT '9007199254740992' + id], ARRAY[7, 8]),
    CASE WHEN skip_map THEN CAST(NULL AS MAP(DOUBLE, INTEGER))
         ELSE CAST(map() AS MAP(DOUBLE, INTEGER)) END)
FROM ${case_db}.b7a_map_order WHERE id = 1;

-- query 5
SELECT element_at(MAP(ARRAY[ARRAY[3, 4]], ARRAY[1]), ARRAY[1, NULL]) AS absent_value;
