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

-- Migrated from dev/test/sql/test_function/T/test_substr
-- Test Objective:
-- 1. Validate SUBSTR/SUBSTRING with extreme BIGINT offsets on empty and nonempty strings.
-- 2. Assert an empty string for out-of-range offsets supplied as literals or columns.
-- 3. Cover calls with an offset alone and with an explicit length.

-- query 1
select SUBSTR('', 9223372036854775807) ;

-- query 2
select SUBSTR('', 9223372036854775807, 465254298) ;

-- query 3
select SUBSTR('', -9223372036854775807, 465254298) ;

-- query 4
select SUBSTR('', 9223372036854775806, 465254298) ;

-- query 5
select SUBSTRING('', 9223372036854775807) ;

-- query 6
select SUBSTRING('', 9223372036854775807, 465254298) ;

-- query 7
select SUBSTRING('', -9223372036854775807, 465254298) ;

-- query 8
select SUBSTRING('', 9223372036854775806, 465254298) ;

-- query 9
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (id int, v bigint) WITH (format_version = 3);

-- query 10
-- @skip_result_check=true
USE ${case_db};
insert into t1 values ROW(1, 9223372036854775807), ROW(2, -9223372036854775807), ROW(3, 9223372036854775806);

-- query 11
USE ${case_db};
select SUBSTR('', v) from t1;

-- query 12
USE ${case_db};
select SUBSTR('', v, id) from t1;

-- query 13
USE ${case_db};
select SUBSTR('STARROCKS', v, id) from t1;

-- query 14
USE ${case_db};
select SUBSTRING('', v) from t1;

-- query 15
USE ${case_db};
select SUBSTRING('', v, id) from t1;

-- query 16
USE ${case_db};
select SUBSTRING('STARROCKS', v, id) from t1;

-- query 17
-- @result_contains=TAR
select SUBSTRING('STARROCKS' FROM 2 FOR 3) AS substring_result;

-- query 18
-- @result_contains=ROCKS
select SUBSTRING('STARROCKS' FROM 5) AS substring_result;

-- query 19
select SUBSTRING('' FROM 9223372036854775807);

-- query 20
select SUBSTRING('' FROM -9223372036854775807 FOR 1);
