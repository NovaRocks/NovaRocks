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

-- Migrated from dev/test/sql/test_agg/R/test_serialize_key_agg
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained SQL test case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_serialize_key_agg @mac
-- query 2
-- @skip_result_check=true
USE ${case_db};
create table t0 (
    c0 VARCHAR,
    c1 VARCHAR,
    c2 VARCHAR
) WITH (format_version = 3);

-- query 3
USE ${case_db};
select distinct c0, c1 from t0 order by c0, c1 desc limit 10;

-- query 4
USE ${case_db};
select length(c0), length(c1) from (select distinct c0, c1 from (select * from t0 union all select repeat(' ', 1000000) as c0, repeat(' ', 1000000) as c1, repeat(' ', 1000000) as c2) tb) tb order by 1, 2 desc limit 10;

-- query 5
USE ${case_db};
select length(c0), max(length(c1)), max(length(c2)) from (select * from t0 union all select repeat(' ', 1000000) as c0, repeat(' ', 1000000) as c1, repeat(' ', 1000000) as c2) tb group by c0 order by 1, 2 desc limit 10;

-- query 6
-- @skip_result_check=true
USE ${case_db};
insert into t0 SELECT CAST(generate_series AS VARCHAR), CAST(4096 - generate_series AS VARCHAR), CAST(generate_series AS VARCHAR) FROM TABLE(generate_series(1,  4096));

-- query 7
USE ${case_db};
select max(length(c0)), max(length(c1)) from (select distinct c0, c1 from t0) tb;

-- query 8
USE ${case_db};
select max(length(c0)), max(length(c1)) from (select distinct c0, c1 from (select * from t0 union all select repeat(' ', 1000000) as c0, repeat(' ', 1000000) as c1, repeat(' ', 1000000) as c2) tb) tb order by 1, 2 desc limit 10;

-- query 9
USE ${case_db};
select length(c0), max(length(c1)), max(length(c2)) from (select * from t0 union all select repeat(' ', 1000000) as c0, repeat(' ', 1000000) as c1, repeat(' ', 1000000) as c2) tb group by c0, c1, c2 order by 1, 2 desc limit 10;
