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

-- Migrated from: dev/test/sql/test_window_function/T/test_minmax_by_window_function
-- Test Objective:
-- 1. Validate min_by over window functions with nullable and non-nullable columns of various types (int, bigint, largeint, string, json, map, array).
-- 2. Cover both nullable and non-nullable table variants.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}."t1_nulls" (
  "v1" int ,
  "v2" bigint ,
  "v3" largeint ,
  "v4" VARCHAR ,
  "v5" json ,
  "v6" map<int, VARCHAR> ,
  "v7" array<int>
) WITH (format_version = 3);
INSERT INTO ${case_db}."t1_nulls" SELECT generate_series, generate_series, generate_series, generate_series, generate_series, map(generate_series, generate_series), [generate_series] FROM TABLE(generate_series(1, 40960));

-- query 2
select count(mb), sum(mb) from (select min_by(v1, v1) over (partition by v1) mb from ${case_db}.t1_nulls) t;

-- query 3
select count(mb), sum(mb) from (select min_by(v2, v2) over (partition by v1) mb from ${case_db}.t1_nulls) t;

-- query 4
select count(mb), sum(mb) from (select min_by(v3, v3) over (partition by v1) mb from ${case_db}.t1_nulls) t;

-- query 5
select count(mb), max(mb) from (select min_by(v4, v4) over (partition by v1) mb from ${case_db}.t1_nulls) t;

-- query 6
select count(mb), max(get_json_int(mb, '$')) from (select min_by(v5, v1) over (partition by v1) mb from ${case_db}.t1_nulls) t;

-- query 7
-- @skip_result_check=true
CREATE TABLE ${case_db}."t1_not_nulls" (
  "v1" int,
  "v2" bigint,
  "v3" largeint,
  "v4" VARCHAR,
  "v5" json,
  "v6" map<int, VARCHAR>,
  "v7" array<int>
) WITH (format_version = 3);
INSERT INTO ${case_db}."t1_not_nulls" SELECT generate_series, generate_series, generate_series, generate_series, generate_series, map(generate_series, generate_series), [generate_series] FROM TABLE(generate_series(1, 40960));

-- query 8
select count(mb), sum(mb) from (select min_by(v1, v1) over (partition by v1) mb from ${case_db}.t1_not_nulls) t;

-- query 9
select count(mb), sum(mb) from (select min_by(v2, v2) over (partition by v1) mb from ${case_db}.t1_not_nulls) t;

-- query 10
select count(mb), sum(mb) from (select min_by(v3, v3) over (partition by v1) mb from ${case_db}.t1_not_nulls) t;

-- query 11
select count(mb), max(mb) from (select min_by(v4, v4) over (partition by v1) mb from ${case_db}.t1_not_nulls) t;

-- query 12
select count(mb), max(get_json_int(mb, '$')) from (select min_by(v5, v1) over (partition by v1) mb from ${case_db}.t1_not_nulls) t;

-- query 13
select count(mb) from (select min_by(named_struct('k',v1,'v',v1), v1) over (partition by v1) mb from ${case_db}.t1_not_nulls) t;

-- query 14
select count(mb) from (select min_by(named_struct('k',v1,'v',v1), v1) over (partition by v1) mb from ${case_db}.t1_not_nulls) t;

-- query 15
select count(mb) from (select min_by(['k',v1,'v',v1], v1) over (partition by v1) mb from ${case_db}.t1_not_nulls) t;

-- query 16
select count(mb) from (select min_by(map('k',v1), v1) over (partition by v1) mb from ${case_db}.t1_not_nulls) t;

-- query 17
select count(mb) from (select min_by(map('k',[v1]), v1) over (partition by v1) mb from ${case_db}.t1_not_nulls) t;
