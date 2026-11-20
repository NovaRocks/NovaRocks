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

-- Migrated from dev/test/sql/test_agg_function/R/test_dict_merge
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained SQL test case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: testDictMerge
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE "test_dict_merge" (
  "id" int,
  "city" string NOT NULL,
  "city_null" string,
  "city_array" array<string> NOT NULL,
  "city_array_null" array<string>
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into test_dict_merge values
ROW(1, 'beijing', 'beijing', ['beijing', 'shanghai'], NULL),
ROW(1, 'beijing', NULL, ['shenzhen', 'shanghai'], ['shenzhen', 'shanghai']),
ROW(1, 'shanghai', 'shanghai', ['shenzhen', NULL], ['shenzhen', NULL]),
ROW(1, 'shanghai', NULL, ['beijing', NULL, 'shanghai'], NULL);

-- query 4
USE ${case_db};
select dict_merge(city, 255) from test_dict_merge;

-- query 5
USE ${case_db};
select dict_merge(city_null, 255) from test_dict_merge;

-- query 6
USE ${case_db};
select dict_merge(city_array, 255) from test_dict_merge;

-- query 7
USE ${case_db};
select dict_merge(city_array_null, 255) from test_dict_merge;

-- query 8
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1 (
    c1 int,
    c2 string
    ) WITH (format_version = 3);

-- query 9
-- @skip_result_check=true
USE ${case_db};
insert into t1 select generate_series, cast(generate_series as int) from table(generate_series(1, 1000));

-- query 10
USE ${case_db};
select dict_merge(c2, 256) from t1;

-- query 11
USE ${case_db};
select dict_merge(c2, 512) from t1;

-- query 12
USE ${case_db};
select dict_merge(c2, 1024) from t1;
