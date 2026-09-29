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

-- Test Objective:
-- Validate logical NULL elements carried by NullArray across array kernels.
-- No DDL: ARRAY<NULL> is an execution value, not an external storage schema.
-- @order_sensitive=true

-- query 1
SELECT array_contains([], NULL) AS empty_contains,
       array_position([], NULL) AS empty_position,
       array_contains([NULL], NULL) AS singleton_contains,
       array_position([NULL], NULL) AS singleton_position,
       array_contains([NULL, NULL], NULL) AS repeated_contains,
       array_position([NULL, NULL], NULL) AS repeated_position;

-- query 2
SELECT array_remove([NULL, NULL], NULL) AS removed,
       array_distinct([NULL, NULL]) AS deduplicated,
       array_intersect([NULL, NULL], [NULL]) AS intersected,
       array_intersect([NULL], []) AS empty_intersection,
       arrays_overlap([NULL], [NULL]) AS shared_null,
       arrays_overlap([], [NULL]) AS empty_overlap,
       array_contains_all([NULL, NULL], [NULL]) AS contains_all_null,
       array_contains_seq([NULL, NULL], [NULL]) AS contains_null_seq;

-- query 3
SELECT array_contains(CAST(NULL AS ARRAY<INT>), NULL) AS null_contains,
       array_position(CAST(NULL AS ARRAY<INT>), NULL) AS null_position,
       array_remove(CAST(NULL AS ARRAY<INT>), NULL) AS null_removed,
       array_distinct(CAST(NULL AS ARRAY<INT>)) AS null_distinct,
       array_intersect(CAST(NULL AS ARRAY<INT>), [NULL]) AS null_intersection,
       arrays_overlap(CAST(NULL AS ARRAY<INT>), [NULL]) AS null_overlap;

-- query 4
SELECT id, array_contains(a, NULL) AS contains_null,
       array_position(a, NULL) AS null_position,
       array_remove(a, NULL) AS removed,
       array_distinct(a) AS deduplicated
FROM (SELECT 1 AS id, [] AS a
      UNION ALL SELECT 2, [NULL]
      UNION ALL SELECT 3, [NULL, NULL]) AS source
ORDER BY id;

-- query 5
SELECT array_contains([1, NULL, 2], NULL) AS typed_contains,
       array_position([1, NULL, 2], NULL) AS typed_position,
       array_remove([1, NULL, 2], NULL) AS typed_removed,
       array_distinct([[NULL], [NULL]]) AS nested_distinct,
       array_intersect([[NULL], [NULL]], [[NULL]]) AS nested_intersection;
