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
SELECT array_sortby([10,NULL,30,40], [2,NULL,NULL,1], [0,2,1,0]) AS value;

-- query 2
SELECT array_sortby([30,10,20], [NULL,NULL,NULL]) AS value;

-- query 3
SELECT array_sortby([9,8,7], CAST(NULL AS ARRAY<INT>), [3,1,2]) AS value;

-- query 4
SELECT array_sortby([30,10,40,20], [1,1,1,1], [1,1,1,1]) AS value;

-- query 5
SELECT array_sortby(CAST(NULL AS ARRAY<INT>), [1], [1,2]) AS value;

-- query 6
SELECT array_sortby([], [], []) AS value;

-- query 7
SELECT array_sortby([NULL,8,7], [3,1,2]) AS value;

-- query 8
-- @expect_error=Input arrays' size are not equal in array_sortby
SELECT array_sortby([1,2,3], [1,2]) AS value;

-- query 9
WITH inputs AS (
    SELECT generate_series AS id, array_generate(10) AS source
    FROM TABLE(generate_series(1,26))
), sorted AS (
    SELECT id, array_sortby(source,
        array_map(source, x -> CASE WHEN id % 13 = 0 THEN NULL ELSE x % 3 END)) AS value
    FROM inputs
)
SELECT count(*) AS row_count,
    sum(CASE WHEN id % 13 = 0 THEN 1 ELSE 0 END) AS null_key_rows,
    sum(murmur_hash3_32(array_join(value,'-'))) AS fingerprint
FROM sorted;
