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
-- The input domain, overflow-to-NULL, and all coefficients are independent oracles.
-- query 1
CREATE TABLE ${case_db}.scalar_integer_domain(id INT,t TINYINT,age SMALLINT,i INT);
INSERT INTO ${case_db}.scalar_integer_domain SELECT x,x,x,x FROM generate_series(127,128) g(x);
INSERT INTO ${case_db}.scalar_integer_domain SELECT x,x,x,x FROM generate_series(32767,32768) g(x);
INSERT INTO ${case_db}.scalar_integer_domain SELECT x,x,x,x FROM generate_series(-129,-128) g(x);
SELECT id,t,age,i FROM ${case_db}.scalar_integer_domain ORDER BY id;

-- query 2
SELECT typeof(t) AS tiny_type,typeof(age) AS age_type,typeof(i) AS int_type,
count(t) AS tiny_count,count(age) AS age_count,count(i) AS int_count,
sum(t) AS tiny_sum,sum(age) AS age_sum,sum(i) AS int_sum
FROM ${case_db}.scalar_integer_domain GROUP BY 1,2,3;

-- query 3
SELECT CAST(128 AS TINYINT) AS tiny_overflow,CAST(32768 AS SMALLINT) AS small_overflow,
CAST(-128 AS TINYINT) AS tiny_min,CAST(-32768 AS SMALLINT) AS small_min;

-- query 4
SELECT id FROM ${case_db}.scalar_integer_domain WHERE t>=127 ORDER BY id;

-- query 5
SELECT id FROM ${case_db}.scalar_integer_domain WHERE age>=32767 ORDER BY id;

-- query 6
ALTER TABLE ${case_db}.scalar_integer_domain RENAME COLUMN t TO renamed;
ALTER TABLE ${case_db}.scalar_integer_domain ADD COLUMN t INT;
SELECT typeof(renamed) AS renamed_type,typeof(t) AS reused_name_type,count(renamed) AS tiny_count,count(t) AS new_count
FROM ${case_db}.scalar_integer_domain GROUP BY 1,2;

-- query 7
-- @expect_error=Iceberg MODIFY cannot change a scalar integer domain without changing its physical schema
ALTER TABLE ${case_db}.scalar_integer_domain MODIFY COLUMN age INT;

-- query 8
ALTER TABLE ${case_db}.scalar_integer_domain MODIFY COLUMN renamed TINYINT;
SELECT typeof(renamed) AS tiny_type,sum(CAST(renamed AS INT)) AS widened_sum,count(renamed) AS tiny_count
FROM ${case_db}.scalar_integer_domain GROUP BY 1;

-- query 9
ALTER TABLE ${case_db}.scalar_integer_domain MODIFY COLUMN renamed BIGINT;
INSERT INTO ${case_db}.scalar_integer_domain (id,renamed) VALUES(999,128);
SELECT typeof(renamed) AS wide_type,sum(renamed) AS wide_sum,count(renamed) AS wide_count
FROM ${case_db}.scalar_integer_domain GROUP BY 1;

-- query 10
-- @skip_result_check=true
DROP TABLE ${case_db}.scalar_integer_domain;
