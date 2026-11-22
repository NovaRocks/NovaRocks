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

-- Migrated from dev/test/sql/test_function/T/test_str_to_map
-- Test Objective:
-- 1. Validate str_to_map() correctly parses string values into maps.
-- 2. Validate cardinality of resulting maps over a large dataset (10000 rows).
-- 3. Ensure sum of cardinalities matches expected total.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1(c1 INT, c2 VARCHAR) WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
USE ${case_db};
insert into t1 SELECT generate_series, CAST(generate_series AS VARCHAR) FROM TABLE(generate_series(1, 10000));

-- query 3
USE ${case_db};
select sum(cardinality(str_to_map(c2, ',', ':'))) from t1;
