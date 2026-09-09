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
CREATE DATABASE iceberg_paimon_${suite_uuid0}.cross_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_paimon_${suite_uuid0}.cross_${uuid0}.dim_${uuid0} (
    id BIGINT,
    label STRING
);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_paimon_${suite_uuid0}.cross_${uuid0}.dim_${uuid0}
VALUES (1, 'ice-a'), (4, 'ice-d'), (9, 'ice-x');

-- query 4
-- @order_sensitive=true
SELECT p.id, p.note, i.label
FROM paimon_${suite_uuid0}.fixture.pk_default p
JOIN iceberg_paimon_${suite_uuid0}.cross_${uuid0}.dim_${uuid0} i
  ON p.id = i.id
ORDER BY p.id;

-- query 5
-- @skip_result_check=true
DROP TABLE iceberg_paimon_${suite_uuid0}.cross_${uuid0}.dim_${uuid0} FORCE;

-- query 6
-- @skip_result_check=true
DROP DATABASE iceberg_paimon_${suite_uuid0}.cross_${uuid0};
