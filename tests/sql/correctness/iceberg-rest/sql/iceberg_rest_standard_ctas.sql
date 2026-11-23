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
-- Standard Iceberg REST CTAS has one staged-create publication frontier. The
-- target becomes readable only after that single catalog publication commits.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA iceberg_rest_${suite_uuid0}.ctas_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.ctas_db_${uuid0}.src_${uuid0} (
  id INT,
  name VARCHAR
);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.ctas_db_${uuid0}.src_${uuid0}
VALUES ROW(1, 'alice'), ROW(2, 'bob');

-- query 4
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.ctas_db_${uuid0}.dst_${uuid0} AS SELECT id, UPPER(name) AS uname
FROM iceberg_rest_${suite_uuid0}.ctas_db_${uuid0}.src_${uuid0};

-- query 5
SELECT id, uname
FROM iceberg_rest_${suite_uuid0}.ctas_db_${uuid0}.dst_${uuid0}
ORDER BY id;

-- query 6
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.ctas_db_${uuid0}.dst_${uuid0};

-- query 7
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.ctas_db_${uuid0}.src_${uuid0};

-- query 8
-- @skip_result_check=true
DROP SCHEMA iceberg_rest_${suite_uuid0}.ctas_db_${uuid0};
