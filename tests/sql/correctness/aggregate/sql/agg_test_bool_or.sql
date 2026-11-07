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

-- Migrated from dev/test/sql/test_agg_function/R/test_bool_or
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained SQL test case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_bool_or
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `t1` (
  `c0` bigint NOT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL,
  `c3` bigint DEFAULT NULL
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
USE ${case_db};
select bool_or(c0), boolor_agg(c1), bool_or(c2), bool_or(c3), bool_or(null) from t1;

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into t1 SELECT generate_series, generate_series, generate_series, null FROM TABLE(generate_series(1,  40960));

-- query 5
USE ${case_db};
select bool_or(c0), boolor_agg(c1), bool_or(c2), bool_or(c3), bool_or(null) from t1;

-- query 6
-- @skip_result_check=true
USE ${case_db};

-- query 7
USE ${case_db};
select sum (a), sum(b), sum(c),sum(d), sum(e) from (select bool_or(c0) a, boolor_agg(c1) b, bool_or(c2) c, bool_or(c3) d, bool_or(null) e from t1 group by c0) t;

-- query 8
USE ${case_db};
select sum(a) from ( select bool_or(c0) over (partition by c2 order by c3) a from t1) t;
