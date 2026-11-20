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

-- Migrated from: dev/test/sql/test_window_function/T/test_leadlag_window_function
-- Test Objective:
-- 1. Validate lead/lag window functions over partitioned decimal64 windows with functional-scale datasets.
-- 2. Validate default offset (1), custom offset, and custom default value for lead/lag.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}."t1" (
  "v1" int(11) ,
  "v2" int(11) ,
  "v3" int(11) NOT NULL,
  "v4" int(11)
) WITH (format_version = 3);

INSERT INTO ${case_db}."t1" (v1, v2, v3, v4) values
    ROW(1, 1, 1, NULL),
    ROW(1, 1, 2, NULL),
    ROW(1, NULL, 3, NULL),
    ROW(1, NULL, 4, NULL),
    ROW(1, 2, 5, NULL),
    ROW(1, 2, 6, NULL),
    ROW(1, NULL, 7, NULL),
    ROW(1, NULL, 8, NULL),
    ROW(2, 3, 9, NULL),
    ROW(2, 3, 10, NULL),
    ROW(2, NULL, 11, NULL),
    ROW(2, NULL, 12, NULL),
    ROW(2, 4, 13, NULL),
    ROW(2, 4, 14, NULL),
    ROW(2, NULL, 15, NULL),
    ROW(2, NULL, 16, NULL),
    ROW(NULL, 3, 17, NULL),
    ROW(NULL, 3, 18, NULL),
    ROW(NULL, NULL, 19, NULL),
    ROW(NULL, NULL, 20, NULL),
    ROW(NULL, 4, 21, NULL),
    ROW(NULL, 4, 22, NULL),
    ROW(NULL, NULL, 23, NULL),
    ROW(NULL, NULL, 24, NULL);

INSERT INTO ${case_db}."t1" SELECT * FROM ${case_db}."t1";
INSERT INTO ${case_db}."t1" SELECT * FROM ${case_db}."t1";
INSERT INTO ${case_db}."t1" SELECT * FROM ${case_db}."t1";
INSERT INTO ${case_db}."t1" SELECT * FROM ${case_db}."t1";
INSERT INTO ${case_db}."t1" SELECT * FROM ${case_db}."t1";
INSERT INTO ${case_db}."t1" SELECT * FROM ${case_db}."t1";

INSERT INTO ${case_db}."t1" (v1, v2, v3, v4) values
    ROW(101, 101, 101, NULL),
    ROW(101, 101, 102, NULL),
    ROW(101, NULL, 103, NULL),
    ROW(101, NULL, 104, NULL),
    ROW(101, 102, 105, NULL),
    ROW(101, 102, 106, NULL),
    ROW(101, NULL, 107, NULL),
    ROW(101, NULL, 108, NULL),
    ROW(102, 103, 109, NULL),
    ROW(102, 103, 110, NULL),
    ROW(102, NULL, 111, NULL),
    ROW(102, NULL, 112, NULL),
    ROW(102, 104, 113, NULL),
    ROW(102, 104, 114, NULL),
    ROW(102, NULL, 115, NULL),
    ROW(102, NULL, 116, NULL),
    ROW(NULL, 103, 117, NULL),
    ROW(NULL, 103, 118, NULL),
    ROW(NULL, NULL, 119, NULL),
    ROW(NULL, NULL, 120, NULL),
    ROW(NULL, 104, 121, NULL),
    ROW(NULL, 104, 122, NULL),
    ROW(NULL, NULL, 123, NULL),
    ROW(NULL, NULL, 124, NULL);

INSERT INTO ${case_db}."t1" SELECT * FROM ${case_db}."t1";
INSERT INTO ${case_db}."t1" SELECT * FROM ${case_db}."t1";

CREATE TABLE ${case_db}."t5" (
  "v1" decimal64(18,5) ,
  "v2" decimal64(18,5) ,
  "v3" decimal64(18,5) NOT NULL,
  "v4" decimal64(18,5) NOT NULL,
  "v5" decimal64(18,5) NOT NULL,
  "v6" decimal64(18,5) NOT NULL
) WITH (format_version = 3);

INSERT INTO ${case_db}."t5" (v1, v2, v3, v4, v5, v6)
SELECT v1, v2, v3,
row_number() OVER (ORDER BY v1, v2, v3, v4) as v4,
row_number() OVER (PARTITION BY v1 ORDER BY v2, v3, v4) as v5,
1 as v6
FROM ${case_db}."t1";

-- query 2
SELECT SUM(wv), AVG(wv), MIN(wv), MAX(wv) FROM (SELECT lead(v3, 1) OVER(PARTITION BY v2 ORDER BY v4) AS wv FROM ${case_db}.t5) a;

-- query 3
SELECT SUM(wv), AVG(wv), MIN(wv), MAX(wv) FROM (SELECT lead(v3) OVER(PARTITION BY v2 ORDER BY v4) AS wv FROM ${case_db}.t5) a;

-- query 4
SELECT SUM(wv), AVG(wv), MIN(wv), MAX(wv) FROM (SELECT lead(v3, 10, -1) OVER(PARTITION BY v2 ORDER BY v4) AS wv FROM ${case_db}.t5) a;

-- query 5
SELECT SUM(wv), AVG(wv), MIN(wv), MAX(wv) FROM (SELECT lag(v3, 1) OVER(PARTITION BY v2 ORDER BY v4) AS wv FROM ${case_db}.t5) a;

-- query 6
SELECT SUM(wv), AVG(wv), MIN(wv), MAX(wv) FROM (SELECT lag(v3) OVER(PARTITION BY v2 ORDER BY v4) AS wv FROM ${case_db}.t5) a;

-- query 7
SELECT SUM(wv), AVG(wv), MIN(wv), MAX(wv) FROM (SELECT lag(v3, 10, -1) OVER(PARTITION BY v2 ORDER BY v4) AS wv FROM ${case_db}.t5) a;
