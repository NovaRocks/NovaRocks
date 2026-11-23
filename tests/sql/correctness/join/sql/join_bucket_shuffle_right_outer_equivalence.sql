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

-- A right outer join must retain each right-side row, including rows whose key is NULL.
-- Repeat with each left-side filter to distinguish the one matching key from the other rounds.
-- A separate pair of equal negative fingerprints tests DISTINCT independently of join placement.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t0(c0 BIGINT NOT NULL, c1 BIGINT NOT NULL, c2 BIGINT NOT NULL) WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1(c0 BIGINT NOT NULL, c1 BIGINT NOT NULL, c2 BIGINT NOT NULL) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
CREATE TABLE ${case_db}.t2(c0 BIGINT NOT NULL, c1 BIGINT NOT NULL, c2 BIGINT NOT NULL) WITH (format_version = 3);

-- query 4
-- @skip_result_check=true
CREATE TABLE ${case_db}.r(fp BIGINT ) WITH (format_version = 3);

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.t0 (c0,c1,c2) VALUES
  ROW(0,0,0),ROW(1,1,1),ROW(2,2,2),ROW(3,3,3),ROW(4,4,4),
  ROW(5,5,5),ROW(6,6,6),ROW(7,7,7),ROW(8,8,8),ROW(9,9,9);

-- query 6
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 SELECT * FROM ${case_db}.t0;

-- query 7
-- @skip_result_check=true
INSERT INTO ${case_db}.t2 SELECT * FROM ${case_db}.t0;

-- query 8
SELECT count(*) AS retained_rows,
       count(l.c0) AS matched_rows,
       count_if(r.join_key IS NULL) AS null_keys,
       array_agg(r.c1 ORDER BY r.c1) AS retained_values
FROM (SELECT c0 FROM ${case_db}.t1 WHERE c0 = 0) AS l
RIGHT OUTER JOIN (
    SELECT CASE WHEN c0 = 0 THEN c0 END AS join_key, c1
    FROM ${case_db}.t0
) AS r ON r.join_key = l.c0
LEFT JOIN ${case_db}.t2 AS t ON t.c0 = l.c0;

-- query 9
SELECT count(*) AS retained_rows,
       count(l.c0) AS matched_rows,
       count_if(r.join_key IS NULL) AS null_keys,
       array_agg(r.c1 ORDER BY r.c1) AS retained_values
FROM (SELECT c0 FROM ${case_db}.t1 WHERE c0 = 1) AS l
RIGHT OUTER JOIN (
    SELECT CASE WHEN c0 = 0 THEN c0 END AS join_key, c1
    FROM ${case_db}.t0
) AS r ON r.join_key = l.c0
LEFT JOIN ${case_db}.t2 AS t ON t.c0 = l.c0;

-- query 10
SELECT count(*) AS retained_rows,
       count(l.c0) AS matched_rows,
       count_if(r.join_key IS NULL) AS null_keys,
       array_agg(r.c1 ORDER BY r.c1) AS retained_values
FROM (SELECT c0 FROM ${case_db}.t1 WHERE c0 = 2) AS l
RIGHT OUTER JOIN (
    SELECT CASE WHEN c0 = 0 THEN c0 END AS join_key, c1
    FROM ${case_db}.t0
) AS r ON r.join_key = l.c0
LEFT JOIN ${case_db}.t2 AS t ON t.c0 = l.c0;

-- query 11
SELECT count(*) AS retained_rows,
       count(l.c0) AS matched_rows,
       count_if(r.join_key IS NULL) AS null_keys,
       array_agg(r.c1 ORDER BY r.c1) AS retained_values
FROM (SELECT c0 FROM ${case_db}.t1 WHERE c0 = 3) AS l
RIGHT OUTER JOIN (
    SELECT CASE WHEN c0 = 0 THEN c0 END AS join_key, c1
    FROM ${case_db}.t0
) AS r ON r.join_key = l.c0
LEFT JOIN ${case_db}.t2 AS t ON t.c0 = l.c0;

-- query 12
SELECT count(*) AS retained_rows,
       count(l.c0) AS matched_rows,
       count_if(r.join_key IS NULL) AS null_keys,
       array_agg(r.c1 ORDER BY r.c1) AS retained_values
FROM (SELECT c0 FROM ${case_db}.t1 WHERE c0 = 4) AS l
RIGHT OUTER JOIN (
    SELECT CASE WHEN c0 = 0 THEN c0 END AS join_key, c1
    FROM ${case_db}.t0
) AS r ON r.join_key = l.c0
LEFT JOIN ${case_db}.t2 AS t ON t.c0 = l.c0;

-- query 13
SELECT count(*) AS retained_rows,
       count(l.c0) AS matched_rows,
       count_if(r.join_key IS NULL) AS null_keys,
       array_agg(r.c1 ORDER BY r.c1) AS retained_values
FROM (SELECT c0 FROM ${case_db}.t1 WHERE c0 = 5) AS l
RIGHT OUTER JOIN (
    SELECT CASE WHEN c0 = 0 THEN c0 END AS join_key, c1
    FROM ${case_db}.t0
) AS r ON r.join_key = l.c0
LEFT JOIN ${case_db}.t2 AS t ON t.c0 = l.c0;

-- query 14
SELECT count(*) AS retained_rows,
       count(l.c0) AS matched_rows,
       count_if(r.join_key IS NULL) AS null_keys,
       array_agg(r.c1 ORDER BY r.c1) AS retained_values
FROM (SELECT c0 FROM ${case_db}.t1 WHERE c0 = 6) AS l
RIGHT OUTER JOIN (
    SELECT CASE WHEN c0 = 0 THEN c0 END AS join_key, c1
    FROM ${case_db}.t0
) AS r ON r.join_key = l.c0
LEFT JOIN ${case_db}.t2 AS t ON t.c0 = l.c0;

-- query 15
SELECT count(*) AS retained_rows,
       count(l.c0) AS matched_rows,
       count_if(r.join_key IS NULL) AS null_keys,
       array_agg(r.c1 ORDER BY r.c1) AS retained_values
FROM (SELECT c0 FROM ${case_db}.t1 WHERE c0 = 7) AS l
RIGHT OUTER JOIN (
    SELECT CASE WHEN c0 = 0 THEN c0 END AS join_key, c1
    FROM ${case_db}.t0
) AS r ON r.join_key = l.c0
LEFT JOIN ${case_db}.t2 AS t ON t.c0 = l.c0;

-- query 16
SELECT count(*) AS retained_rows,
       count(l.c0) AS matched_rows,
       count_if(r.join_key IS NULL) AS null_keys,
       array_agg(r.c1 ORDER BY r.c1) AS retained_values
FROM (SELECT c0 FROM ${case_db}.t1 WHERE c0 = 8) AS l
RIGHT OUTER JOIN (
    SELECT CASE WHEN c0 = 0 THEN c0 END AS join_key, c1
    FROM ${case_db}.t0
) AS r ON r.join_key = l.c0
LEFT JOIN ${case_db}.t2 AS t ON t.c0 = l.c0;

-- query 17
SELECT count(*) AS retained_rows,
       count(l.c0) AS matched_rows,
       count_if(r.join_key IS NULL) AS null_keys,
       array_agg(r.c1 ORDER BY r.c1) AS retained_values
FROM (SELECT c0 FROM ${case_db}.t1 WHERE c0 = 9) AS l
RIGHT OUTER JOIN (
    SELECT CASE WHEN c0 = 0 THEN c0 END AS join_key, c1
    FROM ${case_db}.t0
) AS r ON r.join_key = l.c0
LEFT JOIN ${case_db}.t2 AS t ON t.c0 = l.c0;

-- query 18
WITH fingerprints AS (
    SELECT c0, -crc32(to_utf8(CAST(c0 AS VARCHAR))) AS fp
    FROM ${case_db}.t0
)
SELECT count(fp) AS fingerprints,
       count_if(fp < 0) AS negative_fingerprints,
       count(DISTINCT fp) AS distinct_fingerprints,
       array_agg(fp ORDER BY c0) AS values_by_key
FROM fingerprints;

-- query 19
-- @skip_result_check=true
INSERT INTO ${case_db}.r (fp)
SELECT -crc32(to_utf8(CAST(c0 AS VARCHAR)))
FROM ${case_db}.t0 WHERE c0 = 4;
INSERT INTO ${case_db}.r (fp)
SELECT -crc32(to_utf8(CAST(c0 AS VARCHAR)))
FROM ${case_db}.t0 WHERE c0 = 4;

-- query 20
SELECT count(fp) AS fingerprints,
       count(DISTINCT fp) AS distinct_fingerprints,
       min(fp) AS smallest_fingerprint,
       max(fp) AS largest_fingerprint
FROM ${case_db}.r;
