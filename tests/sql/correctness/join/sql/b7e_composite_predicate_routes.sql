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

-- query 1
-- The right side returns one ROW-valued column, not two scalar columns.
-- Duplicate right rows must not multiply the outer rows.
-- @explain_contains=NULL AWARE LEFT MARK
-- @explain_not_contains=HASH JOIN
WITH probes AS (
    SELECT id,
           ROW(IF(id = 4, BIGINT '1', id),
               CASE id
                   WHEN 1 THEN CAST(NULL AS BIGINT)
                   WHEN 4 THEN BIGINT '2'
                   ELSE BIGINT '9'
               END) AS probe_value
    FROM TABLE(sequence(1, 4)) AS s(id)
)
SELECT id,
       probe_value IN (
           SELECT ROW(BIGINT '1', BIGINT '2') FROM TABLE(sequence(1, 2))
       ) AS member,
       probe_value NOT IN (
           SELECT ROW(BIGINT '1', BIGINT '2') FROM TABLE(sequence(1, 2))
       ) AS nonmember
FROM probes
ORDER BY id;

-- query 2
-- Equality drops the pair with a NULL member of an otherwise valid ROW.
-- @explain_contains=NEST LOOP JOIN
-- @explain_not_contains=HASH JOIN
WITH probes AS (
    SELECT id, ROW(id, IF(id = 1, CAST(NULL AS BIGINT), id)) AS left_value
    FROM TABLE(sequence(1, 3)) AS s(id)
), candidates AS (
    SELECT rid, ROW(rid, rid) AS right_value
    FROM TABLE(sequence(1, 3)) AS s(rid)
)
SELECT p.id AS probe_id, c.rid AS candidate_id
FROM probes p JOIN candidates c ON p.left_value = c.right_value
ORDER BY probe_id, candidate_id;

-- query 3
-- The scalar key remains the hash key. Only the non-NULL row identity
-- relation lets the first even candidate match its NULL member.
-- @explain_contains=HASH JOIN
-- @explain_contains=other:
WITH probes AS (
    SELECT id, CAST(ROW(id, IF(id = 1, CAST(NULL AS BIGINT), BIGINT '7')) AS ROW(key_value BIGINT, label_value BIGINT)) AS left_value
    FROM TABLE(sequence(1, 256)) AS s(id)
), right_rows AS (
    SELECT rid,
           CASE
               WHEN rid <= 2 THEN BIGINT '1'
               WHEN rid <= 4 THEN BIGINT '2'
               WHEN rid <= 6 THEN BIGINT '3'
               ELSE rid
           END AS group_id
    FROM TABLE(sequence(1, 512)) AS s(rid)
), candidates AS (
    SELECT rid, group_id,
           CAST(ROW(group_id, IF(rid <= 6 AND rid % 2 = 0, IF(group_id = 1, CAST(NULL AS BIGINT), BIGINT '7'), BIGINT '99')) AS ROW(key_value BIGINT, label_value BIGINT)) AS right_value
    FROM right_rows
)
SELECT p.id AS probe_id, c.rid AS candidate_id
FROM probes p JOIN candidates c
  ON p.id = c.group_id AND p.left_value IS NOT DISTINCT FROM c.right_value
ORDER BY probe_id, candidate_id;
