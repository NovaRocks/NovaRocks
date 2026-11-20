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

-- Migrated from dev/test/sql/test_agg/R/test_orderby_agg
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained SQL test case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_orderby_agg @mac
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE "t0" (
  "v1" bigint(20),
  "v2" bigint(20),
  "v3" bigint(20),
  "v4" varchar
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into t0 values ROW(1, 2, 3, 'a'), ROW(1, 3, 4, 'b'), ROW(2, 3, 4, 'a'), ROW(null, 1, null, 'c'), ROW(4, null, 1 , null),
ROW(5, 1 , 3, 'c'), ROW(2, 2, null, 'a'), ROW(4, null, 4, 'c'), ROW(null, null, 2, null);

-- query 4
USE ${case_db};
select min(v1) v1 from t0 group by v3 order by round(count(v2) / min(v1)), min(v1);

-- query 5
USE ${case_db};
select min(v1) v1, round(count(v2) / min(v1)) round_col from t0 group by v3 order by abs(min(v1)) + abs(v1) asc;

-- query 6
USE ${case_db};
select min(v1) v1 from t0 group by v3 order by round(count(v2) / min(v1)), abs(v1);

-- query 7
USE ${case_db};
select min(v1) v11 from t0 group by v3 order by round(count(v2) / min(v1)), abs(v11);

-- query 8
USE ${case_db};
select min(v1) v11, min(v1) v1 from t0 group by v3 order by round(count(v2) / min(v1)), abs(v11), abs(v1);

-- query 9
USE ${case_db};
select round(count(v1) * 100.0 / min(v2), 4) as potential_customer_rate, min(v2) v2 from t0 group by v4 order by round(count(v1) * 100.0 / min(v2), 4), min(v2);

-- query 10
USE ${case_db};
select round(count(v1) * 100.0 / min(v2), 4) as potential_customer_rate, min(v2) v2 from t0 group by v4 order by round(count(v1) * 100.0 / min(v2), 4), abs(v2);
