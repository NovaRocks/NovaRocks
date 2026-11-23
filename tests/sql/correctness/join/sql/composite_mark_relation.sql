-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements. See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership. The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License. You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied. See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- Expected marks are the three-valued OR of ordinary equality on the
-- actual pairs. In Trino 484 query 1's first mark is false.

-- query 1
SELECT ARRAY[1,NULL] IN (SELECT ARRAY[1,2]) AS unknown_mark,
       ARRAY[9,NULL] IN (SELECT ARRAY[1,2]) AS false_mark;

-- query 2
SELECT ARRAY[1,NULL] NOT IN (SELECT ARRAY[1,2]) AS unknown_mark,
       ARRAY[9,NULL] NOT IN (SELECT ARRAY[1,2]) AS true_mark;

-- query 3
SELECT ARRAY[1,NULL] IN (SELECT ARRAY[1,2] WHERE FALSE) AS empty_mark,
       ARRAY[1,2] IN (SELECT CAST(NULL AS ARRAY(INTEGER))) AS null_mark;

-- query 4
SELECT id, a IN (VALUES ARRAY[1,NULL], ARRAY[1,2], ARRAY[1,2]) AS mark
FROM (VALUES (1, ARRAY[1,2]), (2, ARRAY[1,2]),
             (3, ARRAY[9,NULL]), (4, ARRAY[1,NULL])) AS t(id, a)
ORDER BY id;

-- query 5
SELECT guard OR (ARRAY[CAST(v AS INTEGER)] IN (SELECT ARRAY[1])) AS mark
FROM (VALUES (TRUE, 'not an integer')) AS t(guard, v);

-- query 6
SELECT CASE WHEN guard THEN ARRAY[CAST(v AS INTEGER)] IN (SELECT ARRAY[1])
            ELSE TRUE END AS mark
FROM (VALUES (FALSE, 'not an integer')) AS t(guard, v);

-- query 7
SELECT id
FROM (VALUES (1, ARRAY[1,2]), (2, ARRAY[1,NULL]), (3, ARRAY[9,NULL])) AS t(id, a)
WHERE a IN (SELECT ARRAY[1,2])
ORDER BY id;

-- query 8
SELECT sum(CASE WHEN ARRAY[x] IN (SELECT ARRAY[1]) THEN 1 ELSE 0 END) AS matched
FROM (VALUES (1), (2), (1)) AS t(x);

-- query 9
SELECT ARRAY[count(*)] IN (SELECT ARRAY[CAST(2 AS BIGINT)]) AS mark
FROM (VALUES 1, 2) AS t(x);

-- query 10
SELECT x, count(*) AS rows
FROM (VALUES (1), (1), (2)) AS t(x)
GROUP BY x
HAVING ARRAY[count(*)] IN (SELECT ARRAY[CAST(2 AS BIGINT)])
ORDER BY x;

-- query 11
SELECT ARRAY[CAST(100001 AS BIGINT)] IN (
           SELECT ARRAY[i] FROM TABLE(sequence(1,100001)) AS s(i)
       ) AS matched,
       ARRAY[CAST(100002 AS BIGINT)] IN (
           SELECT ARRAY[i] FROM TABLE(sequence(1,100001)) AS s(i)
       ) AS unmatched;
