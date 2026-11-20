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

-- Migrated from dev/test/sql/test_array_fn/R/test_array_distinct
-- Test Objective:
-- Preserve array test coverage migrated from dev/test.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_array_distinct @slow @mac
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1 (
    c1 INT,
    c2 ARRAY<BIGINT>
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t2 (
    c1 INT,
    c2 ARRAY<ARRAY<BIGINT>>
) WITH (format_version = 3);

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into t1 select generate_series, array_append([], generate_series) from TABLE(generate_series(1, 100000));

-- query 5
-- @skip_result_check=true
USE ${case_db};
insert into t2 select 1, array_agg(c2) from t1;

-- query 6
USE ${case_db};
select array_length(array_distinct(c2)) from t2;
