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

-- @order_sensitive=true
-- @tags=aggregate,legacy-migration
-- Migrated from dev/test/sql/test_agg/R/test_meta_scan_agg
-- query 1
USE ${case_db};
create table t2 (
    c0 INT
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
USE ${case_db};
insert into t2 values (1), (2), (3);

-- query 3
USE ${case_db};
select count(*) from t2;

-- query 4
USE ${case_db};
select count(*) from t2;

-- query 5
USE ${case_db};
alter table t2 rename column c0 to c1;

-- query 6
USE ${case_db};
select count(*) from t2;

-- query 7
USE ${case_db};
select count(*) from t2;

-- query 8
USE ${case_db};
alter table t2 rename column c1 to c2;

-- query 9
USE ${case_db};
select count(*) from t2;

-- query 10
USE ${case_db};
select count(*) from t2;
