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


-- query 2
CREATE TABLE `t1` (
`v4` bigint NULL COMMENT "",
`v5` bigint NULL COMMENT "",
`v6` bigint NULL
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
INSERT INTO `t1` (`v4`, `v5`, `v6`) VALUES (1, 10, 100);

-- query 4
INSERT INTO `t1` (`v4`, `v5`, `v6`) VALUES (2, 20, 200);

-- query 5
INSERT INTO `t1` (`v4`, `v5`, `v6`) VALUES (3, 30, 300);

-- query 6
INSERT INTO `t1` (`v4`, `v5`, `v6`) VALUES (4, 40, 400);

-- query 7
INSERT INTO `t1` (`v4`, `v5`, `v6`) VALUES (5, 50, 500);

-- query 8
CREATE TABLE `t2` (
`c4` bigint NULL COMMENT "",
`c5` bigint NULL COMMENT "",
`c6` bigint NULL
)
TBLPROPERTIES ("format-version" = "3");

-- query 9
INSERT INTO `t2` (`c4`, `c5`, `c6`) VALUES (1, 1009, 10009);

-- query 10
INSERT INTO `t2` (`c4`, `c5`, `c6`) VALUES (2, NULL, 10010);

-- query 11
INSERT INTO `t2` (`c4`, `c5`, `c6`) VALUES (3, 1011, NULL);

-- query 12
INSERT INTO `t2` (`c4`, `c5`, `c6`) VALUES (4, 1009, 10009);

-- query 13
INSERT INTO `t2` (`c4`, `c5`, `c6`) VALUES (5, NULL, 10010);

-- query 14
create view agg_view_1
as
select v4, sum(v5) as total from t1 group by v4;

-- query 15
create materialized view mv_1
DISTRIBUTED by hash(`v4`)
refresh manual
as
select v4, total, c5, c6 from t2 join agg_view_1 on c4 = v4;

-- query 16
refresh materialized view mv_1 with sync mode;

-- query 17
-- @skip_result_check=true
SET enable_materialized_view_rewrite = true;
EXPLAIN select v4, total, c5, c6 from t2 join agg_view_1 on c4 = v4;
