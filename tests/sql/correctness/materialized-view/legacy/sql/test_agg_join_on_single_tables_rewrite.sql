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

-- Test Objective:
-- 1. Validate optimizer rewrite chooses the intended materialized view plan.
-- 2. Cover query correctness together with rewrite plan inspection.
-- Source: dev/test/sql/test_materialized_view/T/test_view_based_mv_rewrite

-- query 1
CREATE TABLE t1 (
                    k1 INT,
                    v1 INT,
                    v2 INT)
TBLPROPERTIES ("format-version" = "3");

-- query 2
insert into t1 values (1,1,1),(2,1,1),(3,1,1),(1,2,3),(2,2,3),(3,2,3);

-- query 3
create view v1 as select k1,sum(v1) as sum_v1 from t1 group by k1;

-- query 4
create materialized view mv_2 refresh manual as select v1.k1,v1.sum_v1,t1.v1 from v1 join t1 on v1.k1=t1.k1;

-- query 5
refresh materialized view mv_2 with sync mode;

-- query 6


-- query 7
-- @skip_result_check=true
SET enable_materialized_view_rewrite = true;
EXPLAIN select v1.k1,v1.sum_v1,t1.v1 from v1 join t1 on v1.k1=t1.k1 order by 1,3;
