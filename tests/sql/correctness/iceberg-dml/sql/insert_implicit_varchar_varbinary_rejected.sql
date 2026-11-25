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

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_insert_exact_bytes (id INT, b VARBINARY);

-- query 2
-- @expect_sql_code=sql.analyze.type_mismatch
INSERT INTO ${case_db}.t_insert_exact_bytes VALUES ROW(1,'ÿ'),ROW(2,X'FF00');

-- query 3
-- @expect_sql_code=sql.analyze.type_mismatch
INSERT INTO ${case_db}.t_insert_exact_bytes SELECT 3,'ÿ';

-- query 4
SELECT count(*) AS persisted_rows FROM ${case_db}.t_insert_exact_bytes;

-- query 5
-- @skip_result_check=true
DROP TABLE ${case_db}.t_insert_exact_bytes;
