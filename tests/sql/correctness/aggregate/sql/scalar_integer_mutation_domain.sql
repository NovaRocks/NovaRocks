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
-- Exercise the signed narrow domain through row mutation and delete recipes.
-- query 1
CREATE TABLE ${case_db}.scalar_integer_mutation(id INT,t TINYINT,age SMALLINT)
TBLPROPERTIES ("format-version"="3","write.row-lineage"="true");
INSERT INTO ${case_db}.scalar_integer_mutation VALUES (1,127,32767),(2,-128,-32768),(3,NULL,7);
SELECT id,t,age FROM ${case_db}.scalar_integer_mutation ORDER BY id;

-- query 2
UPDATE ${case_db}.scalar_integer_mutation AS m SET age=32768 WHERE m.id=1;
SELECT id,t,age FROM ${case_db}.scalar_integer_mutation ORDER BY id;

-- query 3
DELETE FROM ${case_db}.scalar_integer_mutation WHERE t=-128;
SELECT id,t,age FROM ${case_db}.scalar_integer_mutation ORDER BY id;

-- query 4
CREATE TABLE ${case_db}.scalar_integer_partition(id INT,t TINYINT,age SMALLINT)
PARTITION BY t TBLPROPERTIES ("format-version"="2");
INSERT INTO ${case_db}.scalar_integer_partition VALUES (1,127,1),(2,-128,2),(3,NULL,3);
DELETE FROM ${case_db}.scalar_integer_partition WHERE t=-128;
SELECT id,t,age FROM ${case_db}.scalar_integer_partition ORDER BY id;

-- query 5
ALTER TABLE ${case_db}.scalar_integer_mutation ADD EQUALITY DELETE (t) VALUES (127);
SELECT id,t,age FROM ${case_db}.scalar_integer_mutation ORDER BY id;

-- query 6
-- @skip_result_check=true
DROP TABLE ${case_db}.scalar_integer_mutation FORCE;
DROP TABLE ${case_db}.scalar_integer_partition FORCE;
